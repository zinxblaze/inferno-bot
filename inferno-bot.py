import asyncio
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
import os
from dotenv import load_dotenv
import aiosqlite
import logging
from logging.handlers import RotatingFileHandler

import discord
from discord.ext import commands, tasks
from discord import app_commands, Embed, Interaction, ui

# Load environment variables
load_dotenv()

# Constants
MIN_INVITES = None  # CHANGED: Removed default value of 1
MAX_INVITES = 100
DATABASE_PATH = 'bot_data.db'
LOG_FILE = 'bot.log'
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=MAX_LOG_SIZE,
            backupCount=LOG_BACKUP_COUNT
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize bot with required intents
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.reactions = True
intents.invites = True

class Bot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix="s!",
            intents=intents,
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="the server"
            )
        )
        self.db_pool = None
        self.invite_cache: Dict[int, Dict[str, discord.Invite]] = {}
        self.giveaway_participants: Dict[int, Dict[int, Dict]] = {}
        self.active_giveaways: Dict[int, Dict[int, asyncio.Task]] = {}
        self.processed_users: Dict[int, Dict[int, List[int]]] = {}
        self.current_member_count = 0
        self.active_polls: Dict[int, Dict[int, Dict]] = {}
        self.countdown_tasks: Dict[int, Dict[int, asyncio.Task]] = {}
        self.presence_lock = asyncio.Lock()
        self.channel_cooldowns: Dict[int, Dict[int, datetime]] = {}  # guild_id -> channel_id -> cooldown_end
        self.message_tracker: Dict[int, Dict[int, List[datetime]]] = {}  # guild_id -> channel_id -> [timestamps]
        self.verification_configs: Dict[int, Dict] = {}  # guild_id -> config
        self.active_users: Dict[int, Dict[int, Dict[int, datetime]]] = {}  # guild_id -> channel_id -> {user_id: last_activity}
        self.ticket_categories: Dict[int, int] = {}  # guild_id -> category_id
        self.ticket_logs: Dict[int, Dict[str, int]] = {}  # guild_id -> {"crew_apply": channel_id}

bot = Bot()

# Database initialization
async def init_db():
    """Initialize the SQLite database with required tables."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS guild_configs
                         (guild_id INTEGER PRIMARY KEY, 
                         giveaway_log INTEGER, 
                         bot_log INTEGER, 
                         invite_log INTEGER)''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS user_invites
                         (guild_id INTEGER, 
                         user_id INTEGER, 
                         invite_count INTEGER,
                         PRIMARY KEY (guild_id, user_id))''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS giveaways
                         (guild_id INTEGER, 
                         message_id INTEGER, 
                         data TEXT,
                         PRIMARY KEY (guild_id, message_id))''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS polls
                         (guild_id INTEGER, 
                         message_id INTEGER, 
                         data TEXT,
                         PRIMARY KEY (guild_id, message_id))''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS invite_history
                         (guild_id INTEGER, 
                         inviter_id INTEGER, 
                         invited_id INTEGER,
                         PRIMARY KEY (guild_id, inviter_id, invited_id))''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS verification_configs
                         (guild_id INTEGER PRIMARY KEY,
                         channel_id INTEGER,
                         unverified_role_id INTEGER,
                         verified_role_id INTEGER,
                         log_channel_id INTEGER,
                         title TEXT,
                         description TEXT)''')
        
        await db.execute('''CREATE TABLE IF NOT EXISTS user_messages
                         (guild_id INTEGER,
                         user_id INTEGER,
                         message_count INTEGER,
                         PRIMARY KEY (guild_id, user_id))''')
        
        # Check if ticket_config table exists and get its columns
        try:
            async with db.execute("PRAGMA table_info(ticket_config)") as cursor:
                columns = await cursor.fetchall()
                if columns:
                    # If table exists with wrong number of columns, recreate it
                    if len(columns) != 2:
                        await db.execute("DROP TABLE ticket_config")
                        await db.execute('''CREATE TABLE ticket_config
                                         (guild_id INTEGER PRIMARY KEY,
                                         ticket_category INTEGER)''')
                else:
                    # Table doesn't exist, create it
                    await db.execute('''CREATE TABLE ticket_config
                                     (guild_id INTEGER PRIMARY KEY,
                                     ticket_category INTEGER)''')
        except Exception:
            # Table doesn't exist, create it
            await db.execute('''CREATE TABLE ticket_config
                             (guild_id INTEGER PRIMARY KEY,
                             ticket_category INTEGER)''')
        
        await db.commit()

# Database helper functions
async def get_guild_config(guild_id: int) -> Dict[str, Union[int, bool]]:
    """Get guild configuration from database."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT giveaway_log, bot_log, invite_log FROM guild_configs WHERE guild_id=?", 
                            (guild_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {'giveaway_log': row[0], 'bot_log': row[1], 'invite_log': row[2]}
            else:
                return {
                    'giveaway_log': None, 'bot_log': None, 'invite_log': None
                }

async def set_guild_config(guild_id: int, giveaway_log: Optional[int] = None, 
                         bot_log: Optional[int] = None, invite_log: Optional[int] = None):
    """Update guild configuration in database."""
    current = await get_guild_config(guild_id)
    giveaway_log = giveaway_log if giveaway_log is not None else current.get('giveaway_log')
    bot_log = bot_log if bot_log is not None else current.get('bot_log')
    invite_log = invite_log if invite_log is not None else current.get('invite_log')
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO guild_configs VALUES (?, ?, ?, ?)",
                      (guild_id, giveaway_log, bot_log, invite_log))
        await db.commit()

async def get_ticket_config(guild_id: int) -> Dict:
    """Get ticket configuration from database."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT ticket_category FROM ticket_config WHERE guild_id=?", 
                            (guild_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    'ticket_category': row[0]
                }
            else:
                return {
                    'ticket_category': None
                }

async def set_ticket_config(guild_id: int, ticket_category: Optional[int] = None):
    """Update ticket configuration in database."""
    current = await get_ticket_config(guild_id)
    ticket_category = ticket_category if ticket_category is not None else current.get('ticket_category')
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO ticket_config VALUES (?, ?)",
                      (guild_id, ticket_category))
        await db.commit()

async def get_invite_count(guild_id: int, user_id: int) -> int:
    """Get a user's invite count."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT invite_count FROM user_invites WHERE guild_id=? AND user_id=?", 
                            (guild_id, user_id)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def set_invite_count(guild_id: int, user_id: int, count: int):
    """Set a user's invite count."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO user_invites VALUES (?, ?, ?)",
                      (guild_id, user_id, count))
        await db.commit()

# Message count functions
async def get_message_count(guild_id: int, user_id: int) -> int:
    """Get a user's message count in the guild."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT message_count FROM user_messages WHERE guild_id=? AND user_id=?", 
                            (guild_id, user_id)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def increment_message_count(guild_id: int, user_id: int):
    """Increment a user's message count."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        current_count = await get_message_count(guild_id, user_id)
        await db.execute("INSERT OR REPLACE INTO user_messages VALUES (?, ?, ?)",
                      (guild_id, user_id, current_count + 1))
        await db.commit()

async def set_message_count(guild_id: int, user_id: int, count: int):
    """Set a user's message count."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO user_messages VALUES (?, ?, ?)",
                      (guild_id, user_id, count))
        await db.commit()

# Invite history functions
async def add_invite_history(guild_id: int, inviter_id: int, invited_id: int):
    """Add an invite relationship to the history."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO invite_history VALUES (?, ?, ?)",
                      (guild_id, inviter_id, invited_id))
        await db.commit()

async def remove_invite_history(guild_id: int, inviter_id: int, invited_id: int):
    """Remove an invite relationship from the history."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM invite_history WHERE guild_id=? AND inviter_id=? AND invited_id=?",
                      (guild_id, inviter_id, invited_id))
        await db.commit()

async def get_invite_history(guild_id: int, invited_id: int) -> Optional[int]:
    """Get the inviter for a specific invited user."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT inviter_id FROM invite_history WHERE guild_id=? AND invited_id=?", 
                            (guild_id, invited_id)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def count_invites_by_inviter(guild_id: int, inviter_id: int) -> int:
    """Count how many users a specific inviter has invited."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM invite_history WHERE guild_id=? AND inviter_id=?", 
                            (guild_id, inviter_id)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

# Verification system functions
async def get_verification_config(guild_id: int) -> Optional[Dict]:
    """Get verification configuration for a guild."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT channel_id, unverified_role_id, verified_role_id, log_channel_id, title, description FROM verification_configs WHERE guild_id=?",
            (guild_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    'channel_id': row[0],
                    'unverified_role_id': row[1],
                    'verified_role_id': row[2],
                    'log_channel_id': row[3],
                    'title': row[4],
                    'description': row[5]
                }
    return None

async def set_verification_config(
    guild_id: int,
    channel_id: int,
    unverified_role_id: int,
    verified_role_id: int,
    log_channel_id: int,
    title: str,
    description: str
):
    """Set verification configuration for a guild."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO verification_configs VALUES (?, ?, ?, ?, ?, ?, ?)",
            (guild_id, channel_id, unverified_role_id, verified_role_id, log_channel_id, title, description)
        )
        await db.commit()
    
    # Update in-memory cache
    bot.verification_configs[guild_id] = {
        'channel_id': channel_id,
        'unverified_role_id': unverified_role_id,
        'verified_role_id': verified_role_id,
        'log_channel_id': log_channel_id,
        'title': title,
        'description': description
    }

class CountdownUpdater:
    """Class to handle real-time countdown updates for giveaways and polls."""
    
    @staticmethod
    async def update_giveaway_countdown(guild_id: int, message_id: int, end_time: datetime):
        """Update the giveaway countdown in real-time."""
        try:
            giveaway = bot.giveaway_participants.get(guild_id, {}).get(message_id)
            if not giveaway:
                return
                
            channel = bot.get_channel(giveaway['channel_id'])
            if not channel:
                return
                
            message = await channel.fetch_message(message_id)
            if not message.embeds:
                return
                
            embed = message.embeds[0]
            
            while datetime.utcnow() < end_time:
                remaining = end_time - datetime.utcnow()
                hours, remainder = divmod(int(remaining.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                
                # Update the duration field in the embed
                for i, field in enumerate(embed.fields):
                    if field.name.lower() == "duration":
                        embed.set_field_at(
                            i,
                            name=field.name,
                            value=f"{hours}h {minutes}m {seconds}s remaining",
                            inline=field.inline
                        )
                        break
                
                try:
                    await message.edit(embed=embed)
                except (discord.NotFound, discord.HTTPException):
                    break
                    
                # Update every 30 seconds for better performance
                await asyncio.sleep(30)
                
        except Exception as e:
            logger.error(f"Countdown error: {e}")
        finally:
            # Clean up task reference
            if guild_id in bot.countdown_tasks and message_id in bot.countdown_tasks[guild_id]:
                del bot.countdown_tasks[guild_id][message_id]

    @staticmethod
    async def update_poll_countdown(guild_id: int, message_id: int, end_time: datetime):
        """Update the poll countdown in real-time."""
        try:
            poll = bot.active_polls.get(guild_id, {}).get(message_id)
            if not poll:
                return
                
            channel = bot.get_channel(poll['channel_id'])
            if not channel:
                return
                
            message = await channel.fetch_message(message_id)
            if not message.embeds:
                return
                
            embed = message.embeds[0]
            
            while datetime.utcnow() < end_time:
                remaining = end_time - datetime.utcnow()
                hours, remainder = divmod(int(remaining.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                
                # Update the footer with remaining time
                original_footer = embed.footer.text
                if "|" in original_footer:
                    base_footer = original_footer.split("|")[0].strip()
                else:
                    base_footer = original_footer
                    
                embed.set_footer(text=f"{base_footer} | Time remaining: {hours}h {minutes}m {seconds}s")
                
                try:
                    await message.edit(embed=embed)
                except (discord.NotFound, discord.HTTPException):
                    break
                    
                # Update every 30 seconds for better performance
                await asyncio.sleep(30)
                
        except Exception as e:
            logger.error(f"Poll countdown error: {e}")
        finally:
            # Clean up task reference
            if guild_id in bot.countdown_tasks and message_id in bot.countdown_tasks[guild_id]:
                del bot.countdown_tasks[guild_id][message_id]

# ===== TICKET SYSTEM CLASSES =====

class TicketCloseView(ui.View):
    """View for closing tickets"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="close_ticket", emoji="🔒")
    async def close_ticket_button(self, interaction: Interaction, button: ui.Button):
        """Handle close ticket button click - now anyone can close."""
        try:
            # Confirm closure
            embed = Embed(
                title="🔒 Close Ticket",
                description="Are you sure you want to close this ticket?",
                color=0xED4245
            )
            
            view = ConfirmCloseView()
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error in close ticket button: {e}")
            await interaction.response.send_message("❌ An error occurred.", ephemeral=True)

class ConfirmCloseView(ui.View):
    """View for confirming ticket closure."""
    def __init__(self):
        super().__init__(timeout=60)
    
    @ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: Interaction, button: ui.Button):
        """Confirm and close the ticket."""
        try:
            channel = interaction.channel
            guild = interaction.guild
            
            # Get the ticket creator from the channel name or topic
            creator_name = channel.name.split("-")[-1]
            
            # Send closure message
            embed = Embed(
                title="🎫 Ticket Closed",
                description=f"This ticket has been closed by {interaction.user.mention}.",
                color=0xED4245,
                timestamp=datetime.utcnow()
            )
            await channel.send(embed=embed)
            
            # Delete the channel after a short delay
            await asyncio.sleep(3)
            await channel.delete()
            
            await log_activity(
                "ticket closed",
                f"Ticket {channel.name} closed by {interaction.user} in {guild.name}",
                guild.id,
                interaction.user
            )
            
        except Exception as e:
            logger.error(f"Error closing ticket: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Failed to close ticket.", ephemeral=True)
    
    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, interaction: Interaction, button: ui.Button):
        """Cancel ticket closure."""
        await interaction.response.edit_message(content="Ticket closure cancelled.", view=None, embed=None)

class TicketDropdown(ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Report", description="Report a rule breaker", emoji="🚨"),
            discord.SelectOption(label="Support", description="Get help from staff", emoji="🛠️"),
            discord.SelectOption(label="War Request", description="Request a crew war", emoji="⚔️"),
            discord.SelectOption(label="Try Out", description="Try out for PvP", emoji="🎮"),
        ]
        super().__init__(placeholder="Select a ticket type...", options=options, custom_id="ticket_dropdown")

    async def callback(self, interaction: Interaction):
        """Handle the dropdown selection."""
        await self.view.handle_ticket_creation(interaction, self.values[0])

class TicketDropdownView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketDropdown())

    async def handle_ticket_creation(self, interaction: Interaction, ticket_type: str):
        """Handle ticket creation based on type."""
        try:
            # Check if bot has necessary permissions
            guild = interaction.guild
            user = interaction.user
            
            # Check if the bot has permission to create channels
            if not guild.me.guild_permissions.manage_channels:
                await interaction.response.send_message(
                    "❌ I don't have permission to create channels. Please grant me 'Manage Channels' permission.",
                    ephemeral=True
                )
                return
            
            ticket_config = await get_ticket_config(guild.id)
            
            # Get or create ticket category
            category_id = ticket_config.get('ticket_category')
            if not category_id:
                # Create a category for tickets
                try:
                    category = await guild.create_category("🎫 Tickets")
                    category_id = category.id
                    await set_ticket_config(guild.id, ticket_category=category_id)
                except discord.Forbidden:
                    await interaction.response.send_message(
                        "❌ I don't have permission to create categories. Please create a '🎫 Tickets' category manually and set it up using `/setup_tickets`.",
                        ephemeral=True
                    )
                    return
                except Exception as e:
                    logger.error(f"Error creating category: {e}")
                    await interaction.response.send_message(
                        "❌ Failed to create ticket category. Please try again.",
                        ephemeral=True
                    )
                    return
            
            category = guild.get_channel(category_id)
            if not category:
                try:
                    category = await guild.create_category("🎫 Tickets")
                    category_id = category.id
                    await set_ticket_config(guild.id, ticket_category=category_id)
                except Exception as e:
                    logger.error(f"Error recreating category: {e}")
                    await interaction.response.send_message(
                        "❌ Ticket category not found and couldn't be recreated. Please set up the ticket system again.",
                        ephemeral=True
                    )
                    return
            
            # Create ticket channel with specific name
            ticket_name = f"{ticket_type.lower().replace(' ', '-')}-{user.name}"
            # Limit channel name length
            if len(ticket_name) > 100:
                ticket_name = ticket_name[:97] + "..."
            
            # Define specific roles that should have access to tickets
            # Hardcoded as requested
            SPECIFIC_SERVER_ID = 1462489924434530389
            SPECIFIC_CATEGORY_ID = 1464336798531719338
            SPECIFIC_ROLE_IDS = [
                1463218617968627854,  # Role 1
                1463218725103861762,  # Role 2
                1465969789305618647,  # Role 3
                1465969640193921044,  # Role 4
                1463187576667115531,  # Role 5
                1463073755239682119   # Role 6
            ]
            
            # Set up basic overwrites
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
            }
            
            # Add admin/mod roles if they exist (always)
            for role in guild.roles:
                if role.permissions.administrator or role.permissions.manage_guild:
                    overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
            
            # Check if this is the specific server and category
            if guild.id == SPECIFIC_SERVER_ID and category_id == SPECIFIC_CATEGORY_ID:
                # Add the specific roles to have view and communicate permissions
                for role_id in SPECIFIC_ROLE_IDS:
                    role = guild.get_role(role_id)
                    if role:
                        overwrites[role] = discord.PermissionOverwrite(
                            read_messages=True,
                            send_messages=True,
                            read_message_history=True,
                            attach_files=True,
                            embed_links=True
                        )
                        logger.info(f"Added role {role.name} ({role.id}) to ticket permissions")
                    else:
                        logger.warning(f"Role with ID {role_id} not found in guild {guild.name}")
            
            try:
                channel = await category.create_text_channel(
                    name=ticket_name,
                    overwrites=overwrites,
                    topic=f"{ticket_type} ticket for {user.name}"
                )
            except discord.Forbidden:
                await interaction.response.send_message(
                    "❌ I don't have permission to create channels in this category. Please check my permissions.",
                    ephemeral=True
                )
                return
            except discord.HTTPException as e:
                if "Maximum number of channels in category reached" in str(e):
                    await interaction.response.send_message(
                        "❌ Cannot create ticket: Maximum number of channels in this category reached.",
                        ephemeral=True
                    )
                    return
                else:
                    raise
            
            # Add close ticket button to the channel
            close_view = TicketCloseView()
            await channel.send(f"**🎫 {ticket_type} Ticket**\n\nCreated by {user.mention}\n\nUse the button below to close this ticket.", view=close_view)
            
            # Send initial message based on ticket type
            if ticket_type == "Report":
                await self.handle_report(interaction, channel, user)
            elif ticket_type == "Support":
                await self.handle_support(interaction, channel, user)
            elif ticket_type == "War Request":
                await self.handle_war_request(interaction, channel, user)
            elif ticket_type == "Try Out":
                await self.handle_try_out(interaction, channel, user)
            
            # Only send response if not already responded
            if not interaction.response.is_done():
                await interaction.response.send_message(f"✅ Created {ticket_type} ticket: {channel.mention}", ephemeral=True)
            
        except Exception as e:
            logger.error(f"Error creating ticket: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Failed to create ticket. Please try again.", ephemeral=True)

    async def handle_report(self, interaction: Interaction, channel: discord.TextChannel, user: discord.Member):
        """Handle report ticket."""
        try:
            if not interaction.response.is_done():
                modal = ReportModal()
                await interaction.response.send_modal(modal)
                
                try:
                    await modal.wait()
                    if hasattr(modal, 'values') and modal.values:
                        embed = Embed(
                            title="🚨 Report Ticket",
                            description=f"Report by {user.mention}",
                            color=0xED4245
                        )
                        for key, value in modal.values.items():
                            embed.add_field(name=key, value=value, inline=False)
                        await channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error in report modal: {e}")
            else:
                await channel.send(f"🚨 **Report Ticket for {user.mention}**\n\nPlease describe your report in this channel.")
        except Exception as e:
            logger.error(f"Error in report: {e}")

    async def handle_support(self, interaction: Interaction, channel: discord.TextChannel, user: discord.Member):
        """Handle support ticket."""
        await channel.send(f"🛠️ **Support Ticket for {user.mention}**\n\nPlease describe your issue and staff will assist you shortly.")

    async def handle_war_request(self, interaction: Interaction, channel: discord.TextChannel, user: discord.Member):
        """Handle war request ticket."""
        try:
            if not interaction.response.is_done():
                modal = WarRequestModal()
                await interaction.response.send_modal(modal)
                
                try:
                    await modal.wait()
                    if hasattr(modal, 'values') and modal.values:
                        embed = Embed(
                            title="⚔️ War Request",
                            description=f"Request by {user.mention}",
                            color=0xFF0000
                        )
                        for key, value in modal.values.items():
                            embed.add_field(name=key, value=value, inline=False)
                        await channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error in war request modal: {e}")
            else:
                await channel.send(f"⚔️ **War Request Ticket for {user.mention}**\n\nPlease provide details about your war request in this channel.")
        except Exception as e:
            logger.error(f"Error in war request: {e}")

    async def handle_try_out(self, interaction: Interaction, channel: discord.TextChannel, user: discord.Member):
        """Handle try out ticket."""
        try:
            if not interaction.response.is_done():
                modal = TryOutModal()
                await interaction.response.send_modal(modal)
                
                try:
                    await modal.wait()
                    if hasattr(modal, 'values') and modal.values:
                        embed = Embed(
                            title="🎮 Try Out Request",
                            description=f"Try out by {user.mention}",
                            color=0x00FF00
                        )
                        for key, value in modal.values.items():
                            embed.add_field(name=key, value=value, inline=False)
                        await channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error in try out modal: {e}")
            else:
                await channel.send(f"🎮 **Try Out Ticket for {user.mention}**\n\nPlease provide your try out details in this channel.")
        except Exception as e:
            logger.error(f"Error in try out: {e}")

# ===== MODAL CLASSES =====

class ReportModal(ui.Modal, title="Report User"):
    def __init__(self):
        super().__init__(timeout=None)
        self.values = {}
        
        self.report_type = ui.TextInput(
            label="Report Type",
            placeholder="Breaking server rules / Breaking private server rules / General report",
            required=True,
            max_length=50
        )
        self.add_item(self.report_type)
        
        self.user_reported = ui.TextInput(
            label="User Being Reported",
            placeholder="Username or Discord ID",
            required=True,
            max_length=100
        )
        self.add_item(self.user_reported)
        
        self.description = ui.TextInput(
            label="Description",
            placeholder="What happened? Provide details and evidence if possible",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1000
        )
        self.add_item(self.description)
    
    async def on_submit(self, interaction: Interaction):
        self.values = {
            "Report Type": self.report_type.value,
            "User Reported": self.user_reported.value,
            "Description": self.description.value
        }
        await interaction.response.send_message("✅ Report submitted! Staff will investigate.", ephemeral=True)

class WarRequestModal(ui.Modal, title="War Request"):
    def __init__(self):
        super().__init__(timeout=None)
        self.values = {}
        
        self.crew_name = ui.TextInput(
            label="Opposing Crew Name",
            placeholder="Name of your crew",
            required=True,
            max_length=100
        )
        self.add_item(self.crew_name)
        
        self.region = ui.TextInput(
            label="Region",
            placeholder="EU / NA / Asia",
            required=True,
            max_length=20
        )
        self.add_item(self.region)
        
        self.fighters = ui.TextInput(
            label="Fighter Names",
            placeholder="Names of your fighters (comma separated)",
            required=True,
            max_length=200
        )
        self.add_item(self.fighters)
    
    async def on_submit(self, interaction: Interaction):
        self.values = {
            "Opposing Crew": self.crew_name.value,
            "Region": self.region.value,
            "Fighters": self.fighters.value
        }
        await interaction.response.send_message("✅ War request submitted! Staff will review it.", ephemeral=True)

class TryOutModal(ui.Modal, title="Try Out Request"):
    def __init__(self):
        super().__init__(timeout=None)
        self.values = {}
        
        self.region = ui.TextInput(
            label="Region",
            placeholder="EU / NA / Asia",
            required=True,
            max_length=20
        )
        self.add_item(self.region)
        
        self.bounty = ui.TextInput(
            label="Bounty/Honor",
            placeholder="Your current bounty or honor",
            required=True,
            max_length=50
        )
        self.add_item(self.bounty)
        
        self.former_crew = ui.TextInput(
            label="Were you in a crew before?",
            placeholder="Yes/No and if yes, which crew?",
            required=True,
            max_length=100
        )
        self.add_item(self.former_crew)
    
    async def on_submit(self, interaction: Interaction):
        self.values = {
            "Region": self.region.value,
            "Bounty/Honor": self.bounty.value,
            "Former Crew": self.former_crew.value
        }
        await interaction.response.send_message("✅ Try out request submitted! Staff will contact you.", ephemeral=True)

# ===== SETUP_TICKETS COMMAND =====

@bot.tree.command(name="setup_tickets", description="Set up the ticket system")
@app_commands.describe(
    category="Category where tickets will be created",
    channel="Channel where the ticket panel will be sent",
    title="Title for the ticket panel embed",
    description="Description for the ticket panel embed"
)
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_tickets(
    interaction: Interaction,
    category: discord.CategoryChannel,
    channel: discord.TextChannel,
    title: str = "🎫 Create a Ticket",
    description: str = "Select a ticket type from the dropdown below to create a ticket."
):
    """Set up the ticket system."""
    try:
        # Save ticket configuration
        await set_ticket_config(
            interaction.guild.id,
            ticket_category=category.id
        )
        
        # Create ticket panel embed
        embed = Embed(
            title=title,
            description=description,
            color=0x5865F2
        )
        
        # Send ticket panel with dropdown
        view = TicketDropdownView()
        await channel.send(embed=embed, view=view)
        
        # Send confirmation
        confirm_embed = Embed(
            title="✅ Ticket System Setup Complete",
            description=(
                f"**Ticket Category:** {category.mention}\n"
                f"**Ticket Panel Channel:** {channel.mention}\n"
                f"**Panel Title:** {title}\n"
                f"**Panel Description:** {description}\n\n"
                f"The ticket creation panel has been sent to {channel.mention}"
            ),
            color=0x57F287
        )
        
        await interaction.response.send_message(embed=confirm_embed)
        
        await log_activity(
            "ticket system setup",
            f"Ticket system setup by {interaction.user} in {interaction.guild.name}\n"
            f"Category: {category.name}\nChannel: {channel.name}\nTitle: {title}",
            interaction.guild.id,
            interaction.user
        )
        
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to setup ticket system: {str(e)}", ephemeral=True)
        await log_activity("ticket setup error", f"error: {str(e)}", interaction.guild.id, interaction.user)

# ===== PVP COMMAND =====

@bot.tree.command(name="pvp", description="Log a PvP match")
@app_commands.describe(
    player1="First player",
    player2="Second player",
    winner="Who won the match",
    score="Match score (e.g., 3-2)",
    region="Region where match took place",
    clip="Link to match clip/video (optional)"
)
async def pvp(
    interaction: Interaction,
    player1: discord.Member,
    player2: discord.Member,
    winner: discord.Member,
    score: str,
    region: str,
    clip: Optional[str] = None
):
    """Log a PvP match."""
    try:
        embed = Embed(
            title="⚔️ PvP Match Logged",
            color=0x5865F2,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="🎮 Player 1", value=player1.mention, inline=True)
        embed.add_field(name="🎮 Player 2", value=player2.mention, inline=True)
        embed.add_field(name="🏆 Winner", value=winner.mention, inline=True)
        embed.add_field(name="📊 Score", value=score, inline=True)
        embed.add_field(name="🌍 Region", value=region, inline=True)
        
        if clip:
            embed.add_field(name="🎬 Clip", value=f"[Watch Here]({clip})", inline=False)
        
        embed.set_footer(text=f"Logged by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed)
        await log_activity(
            "pvp logged",
            f"{interaction.user} logged a PvP match:\n"
            f"Players: {player1} vs {player2}\n"
            f"Winner: {winner}\nScore: {score}\nRegion: {region}",
            interaction.guild.id,
            interaction.user
        )
    except Exception as e:
        await interaction.response.send_message("❌ Failed to log PvP match", ephemeral=True)
        await log_activity("pvp log error", f"error: {str(e)}", interaction.guild.id, interaction.user)

# ===== POLL SYSTEM =====

class PollView(ui.View):
    """View for polls."""
    def __init__(self, guild_id: int, message_id: int, duration: int, question: str):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.message_id = message_id
        self.duration = duration
        self.question = question
        self.task = bot.loop.create_task(self.end_poll_after_duration())

    async def end_poll_after_duration(self):
        """Automatically end the poll after the specified duration."""
        try:
            end_time = datetime.utcnow() + timedelta(minutes=self.duration)
            
            # Wait for the duration
            await asyncio.sleep(self.duration * 60)
            await self.end_poll()
            
        except asyncio.CancelledError:
            return
        finally:
            # Clean up task
            self.task = None

    async def end_poll(self):
        """End the poll and display results without changing the title."""
        try:
            if self.guild_id not in bot.active_polls or self.message_id not in bot.active_polls[self.guild_id]:
                return

            poll = bot.active_polls[self.guild_id][self.message_id]
            channel = bot.get_channel(poll['channel_id'])
            if not channel:
                return

            message = await channel.fetch_message(self.message_id)
            if not message.embeds:
                return

            embed = message.embeds[0]
            results = {}
            total_votes = 0

            # Count votes for each option
            for reaction in message.reactions:
                if reaction.emoji in poll['options']:
                    count = reaction.count - 1  # Subtract 1 for the bot's reaction
                    results[reaction.emoji] = count
                    total_votes += count

            # Format results with improved layout
            result_text = []
            for emoji, option_text in poll['options'].items():
                votes = results.get(emoji, 0)
                percentage = (votes / total_votes * 100) if total_votes > 0 else 0
                bar_length = int(percentage / 5)  # Scale for visual bar
                bar = "█" * bar_length + "░" * (20 - bar_length)
                result_text.append(f"{emoji} **{option_text}**\n`{bar}` {votes} votes ({percentage:.1f}%)")

            # Create results embed with better design - KEEP ORIGINAL TITLE
            embed = Embed(
                title=f"📊 Poll: {poll['question']}",  # Keep original title
                description="**Poll Results:**\n\n" + "\n\n".join(result_text),
                color=0x5865F2
            )
            embed.add_field(name="📈 Total Votes", value=str(total_votes), inline=True)
            embed.set_footer(text=f"Poll created by {poll['creator'].display_name} • Ended at")
            embed.timestamp = datetime.utcnow()

            await message.edit(embed=embed, view=None)
            
            # Clean up
            if self.guild_id in bot.active_polls and self.message_id in bot.active_polls[self.guild_id]:
                del bot.active_polls[self.guild_id][self.message_id]
            if self.guild_id in bot.countdown_tasks and self.message_id in bot.countdown_tasks[self.guild_id]:
                if not bot.countdown_tasks[self.guild_id][self.message_id].done():
                    bot.countdown_tasks[self.guild_id][self.message_id].cancel()
                del bot.countdown_tasks[self.guild_id][self.message_id]
        except Exception as e:
            logger.error(f"Error ending poll: {e}")

@bot.tree.command(name="create_poll", description="create a poll with multiple options")
@app_commands.describe(
    question="the poll question",
    duration="duration in minutes (1-1440)",
    option1="first option",
    option2="second option",
    option3="third option (optional)",
    option4="fourth option (optional)",
    option5="fifth option (optional)",
    ping_role="role to ping for the poll (optional)"
)
async def create_poll(
    interaction: Interaction,
    question: str,
    duration: app_commands.Range[int, 1, 1440],
    option1: str,
    option2: str,
    option3: Optional[str] = None,
    option4: Optional[str] = None,
    option5: Optional[str] = None,
    ping_role: Optional[discord.Role] = None
):
    """Create a new poll."""
    try:
        options = {
            "1️⃣": option1,
            "2️⃣": option2
        }
        
        if option3:
            options["3️⃣"] = option3
        if option4:
            options["4️⃣"] = option4
        if option5:
            options["5️⃣"] = option5
        
        # Create poll embed like giveaway (no real-time updates)
        end_time = datetime.utcnow() + timedelta(minutes=duration)
        
        embed = Embed(
            title=f"📊 Poll: {question}",
            description="**Vote by reacting below!**\n\n" + "\n".join([f"{emoji} {text}" for emoji, text in options.items()]),
            color=0x5865F2
        )
        embed.set_footer(text=f"Poll ends in {duration} minutes • React to vote")
        embed.timestamp = end_time
        
        content = ping_role.mention if ping_role else None
        allowed_mentions = discord.AllowedMentions(roles=True) if ping_role else None
        
        await interaction.response.send_message(
            content=content, 
            embed=embed,
            allowed_mentions=allowed_mentions
        )
        message = await interaction.original_response()
        
        # Add reactions
        for emoji in options.keys():
            await message.add_reaction(emoji)
        
        # Store poll data
        if interaction.guild.id not in bot.active_polls:
            bot.active_polls[interaction.guild.id] = {}
            
        bot.active_polls[interaction.guild.id][message.id] = {
            'question': question,
            'options': options,
            'channel_id': message.channel.id,
            'creator': interaction.user,
            'duration': duration
        }
        
        # Create and attach view (no countdown updates)
        view = PollView(interaction.guild.id, message.id, duration, question)
        await message.edit(view=view)
        
        await log_activity(
            "poll created",
            f"Poll created by {interaction.user.mention}\n"
            f"Question: {question}\n"
            f"Options: {len(options)}\n"
            f"Duration: {duration} minutes",
            interaction.guild.id,
            interaction.user
        )
        
    except Exception as e:
        await interaction.response.send_message("❌ Failed to create poll", ephemeral=True)
        await log_activity(
            "poll creation error",
            f"error in {interaction.guild.name}: {str(e)}",
            interaction.guild.id,
            interaction.user
        )

class GiveawayView(ui.View):
    """View for handling giveaway interactions."""
    def __init__(self, guild_id: int, min_invites: int, required_role_id: Optional[int], 
                 min_messages: Optional[int], message_id: int, bypass_role_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.min_invites = min_invites
        self.required_role_id = required_role_id
        self.min_messages = min_messages
        self.message_id = message_id
        self.bypass_role_id = bypass_role_id
    
    async def update_embed(self):
        """Update the giveaway embed with current participant counts."""
        try:
            giveaway = bot.giveaway_participants.get(self.guild_id, {}).get(self.message_id)
            if not giveaway:
                return
                
            channel = bot.get_channel(giveaway['channel_id'])
            if not channel:
                return
                
            message = await channel.fetch_message(self.message_id)
            if not message.embeds:
                return
                
            embed = message.embeds[0]
            
            # Count eligible participants
            eligible_count = 0
            guild = bot.get_guild(self.guild_id)
            for user_id in giveaway['users']:
                try:
                    member = await guild.fetch_member(user_id)
                    if await self.is_eligible(member, giveaway):
                        eligible_count += 1
                except Exception:
                    continue
            
            # Update embed description with improved formatting
            description = (
                f"**🎁 Prize:** {giveaway['prize']}\n"
                f"**⏰ Duration:** {giveaway['duration']}\n"
                f"**🏆 Winners:** {giveaway['winners']}\n"
                f"**👥 Total Participants:** {len(giveaway['users'])}\n"
                f"**✅ Eligible Participants:** {eligible_count}\n"
                f"**📋 Requirements:**\n"
                f"- Minimum {giveaway['min_invites']} invites\n"
            )
            
            if giveaway.get('required_role_id'):
                description += f"- Required role: <@&{giveaway['required_role_id']}>\n"
            if giveaway.get('min_messages'):
                description += f"- Minimum {giveaway['min_messages']} messages\n"
            if giveaway.get('bypass_role_id'):
                description += f"- Bypass role: <@&{giveaway['bypass_role_id']}> (bypasses all requirements)\n"
            
            description += (
                f"**🚫 Blocked Roles:** {', '.join([f'<@&{rid}>' for rid in giveaway.get('blocked_roles', [])]) if giveaway.get('blocked_roles') else 'None'}\n"
                f"**🎯 Hosted By:** {giveaway['hoster'].mention}\n\n"
                f"**📝 How to Enter:**\nReact with 🎉 below to participate!"
            )
            
            embed.description = description
            
            await message.edit(embed=embed)
        except Exception as e:
            logger.error(f"Error updating embed: {e}")

    async def is_eligible(self, member: discord.Member, giveaway: Dict) -> bool:
        """Check if a member is eligible for the giveaway."""
        # Check if user has bypass role
        if giveaway.get('bypass_role_id'):
            bypass_role = member.guild.get_role(giveaway['bypass_role_id'])
            if bypass_role and bypass_role in member.roles:
                return True
        
        # Check invite requirement
        if await get_invite_count(self.guild_id, member.id) < giveaway['min_invites']:
            return False
        
        # Check required role
        if giveaway.get('required_role_id'):
            required_role = member.guild.get_role(giveaway['required_role_id'])
            if not required_role or required_role not in member.roles:
                return False
        
        # Check message requirement
        if giveaway.get('min_messages'):
            message_count = await get_message_count(self.guild_id, member.id)
            if message_count < giveaway['min_messages']:
                return False
        
        return True

class VerificationView(ui.View):
    """View for verification button."""
    def __init__(self):
        super().__init__(timeout=None)
    
    @ui.button(label="Verify", style=discord.ButtonStyle.green, custom_id="verify_button", emoji="✅")
    async def verify_button(self, interaction: Interaction, button: ui.Button):
        """Handle verification button click."""
        try:
            guild_id = interaction.guild.id
            config = await get_verification_config(guild_id)
            
            if not config:
                await interaction.response.send_message("❌ Verification system not configured for this server.", ephemeral=True)
                return
            
            member = interaction.user
            
            # Get roles
            unverified_role = interaction.guild.get_role(config['unverified_role_id'])
            verified_role = interaction.guild.get_role(config['verified_role_id'])
            
            if not unverified_role or not verified_role:
                await interaction.response.send_message("❌ Verification roles not found.", ephemeral=True)
                return
            
            # Check if user has unverified role
            if unverified_role not in member.roles:
                await interaction.response.send_message("✅ You are already verified!", ephemeral=True)
                return
            
            # Remove unverified role and add verified role
            try:
                await member.remove_roles(unverified_role)
                await member.add_roles(verified_role)
                
                # Send success message
                embed = Embed(
                    title="✅ Verification Successful",
                    description="You have been successfully verified! Welcome to the server!",
                    color=0x57F287  # Discord green
                )
                embed.set_thumbnail(url=member.display_avatar.url)
                embed.set_footer(text=f"Verified at")
                embed.timestamp = datetime.utcnow()
                
                await interaction.response.send_message(embed=embed, ephemeral=True)
                
                # Log verification
                log_channel = interaction.guild.get_channel(config['log_channel_id'])
                if log_channel:
                    log_embed = Embed(
                        title="📝 Member Verified",
                        description=f"**User:** {member.mention} (`{member.id}`)\n**Name:** {member.display_name}",
                        color=0x57F287
                    )
                    log_embed.set_thumbnail(url=member.display_avatar.url)
                    log_embed.set_footer(text=f"Verified at")
                    log_embed.timestamp = datetime.utcnow()
                    await log_channel.send(embed=log_embed)
                
                await log_activity(
                    "member verified",
                    f"{member} ({member.id}) verified in {interaction.guild.name}",
                    guild_id
                )
                
            except discord.Forbidden:
                await interaction.response.send_message("❌ I don't have permission to manage roles.", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message("❌ An error occurred during verification.", ephemeral=True)
                logger.error(f"Verification error: {e}")
                
        except Exception as e:
            logger.error(f"Verification button error: {e}")
            await interaction.response.send_message("❌ An error occurred.", ephemeral=True)

# ===== START GIVEAWAY COMMAND =====

@bot.tree.command(name="start_giveaway", description="start a giveaway with invite requirements")
@app_commands.describe(
    duration="duration in minutes",
    prize="what's being given away",
    min_invites="minimum invites required to enter",
    winners="number of winners (default: 1)",
    hoster="who is hosting the giveaway (default: you)",
    blocked_roles="role that can't participate (optional)",
    pin_role="role to ping for the giveaway (optional)",
    image_url="image URL for the giveaway (optional)",
    required_role="role required to enter (optional)",
    min_messages="minimum messages required (1-150, optional)",
    bypass_role="role that bypasses all requirements (optional)"
)
async def start_giveaway(
    interaction: Interaction, 
    duration: int, 
    prize: str, 
    min_invites: int = None,
    winners: int = 1,
    hoster: Optional[discord.Member] = None,
    blocked_roles: Optional[discord.Role] = None,
    pin_role: Optional[discord.Role] = None,
    image_url: Optional[str] = None,
    required_role: Optional[discord.Role] = None,
    min_messages: Optional[app_commands.Range[int, 1, 150]] = None,
    bypass_role: Optional[discord.Role] = None
):
    """Start a new giveaway."""
    try:
        # Handle min_invites - if None, set to 0 (no requirement)
        if min_invites is None:
            min_invites = 0
        
        duration_minutes = duration
        duration_hours = 0
        if duration_minutes > 60:
            duration_hours = duration_minutes // 60
            duration_minutes = duration_minutes % 60
        
        duration_text = ""
        if duration_hours > 0:
            duration_text += f"{duration_hours} hour{'s' if duration_hours != 1 else ''}"
        if duration_minutes > 0:
            if duration_text:
                duration_text += " and "
            duration_text += f"{duration_minutes} minute{'s' if duration_minutes != 1 else ''}"
        
        # Ensure min_invites is between 0 and MAX_INVITES
        min_invites = max(0, min(min_invites, MAX_INVITES))
        winners = max(1, winners)
        
        blocked_roles_list = []
        if blocked_roles:
            blocked_roles_list.append(blocked_roles.id)
        
        hoster_member = hoster if hoster else interaction.user
        
        # Build requirements description
        requirements = ""
        if min_invites > 0:
            requirements += f"• Minimum {min_invites} invites\n"
        if required_role:
            requirements += f"• Required role: {required_role.mention}\n"
        if min_messages:
            requirements += f"• Minimum {min_messages} messages\n"
        if bypass_role:
            requirements += f"• Bypass role: {bypass_role.mention} (bypasses all requirements)\n"
        
        # Create giveaway embed
        embed = Embed(
            title="🎉 Giveaway Started!",
            description=(
                f"**🎁 Prize:** {prize}\n"
                f"**⏰ Duration:** {duration_text}\n"
                f"**🏆 Winners:** {winners}\n"
                f"**👥 Total Participants:** 0\n"
                f"**✅ Eligible Participants:** 0\n\n"
                f"**📋 Requirements:**\n{requirements if requirements else 'None'}\n"
                f"**🚫 Blocked Roles:** {', '.join([f'<@&{rid}>' for rid in blocked_roles_list]) if blocked_roles_list else 'None'}\n"
                f"**🎯 Hosted By:** {hoster_member.mention}\n\n"
                f"**📝 How to Enter:**\nReact with 🎉 below to participate!"
            ),
            color=0xF47FFF,
            timestamp=datetime.utcnow()
        )

        if image_url:
            embed.set_image(url=image_url)
        
        embed.set_footer(text="Giveaway ends at")
        embed.timestamp = datetime.utcnow() + timedelta(minutes=duration)
        
        # Send the initial response with optional role ping
        content = pin_role.mention if pin_role else None
        allowed_mentions = discord.AllowedMentions(roles=True) if pin_role else None
        await interaction.response.send_message(
            content=content,
            embed=embed,
            allowed_mentions=allowed_mentions
        )
        
        # Get the message object
        message = await interaction.original_response()
        await message.add_reaction("🎉")
        
        # Store giveaway data
        if interaction.guild.id not in bot.giveaway_participants:
            bot.giveaway_participants[interaction.guild.id] = {}
            
        bot.giveaway_participants[interaction.guild.id][message.id] = {
            'users': [],
            'min_invites': min_invites,
            'prize': prize,
            'start_time': datetime.utcnow(),
            'duration': duration_text,
            'duration_minutes': duration,
            'winners': winners,
            'channel_id': interaction.channel_id,
            'message_id': message.id,
            'hoster': hoster_member,
            'blocked_roles': blocked_roles_list,
            'image_url': image_url,
            'guild_id': interaction.guild.id,
            'required_role_id': required_role.id if required_role else None,
            'min_messages': min_messages,
            'bypass_role_id': bypass_role.id if bypass_role else None
        }
        
        await log_giveaway(
            "giveaway started",
            f"Guild: {interaction.guild.name}\n"
            f"Prize: {prize}\nDuration: {duration_text}\nMin invites: {min_invites}\n"
            f"Winners: {winners}\nRequired role: {required_role.name if required_role else 'None'}\n"
            f"Min messages: {min_messages or 'None'}\n"
            f"Bypass role: {bypass_role.name if bypass_role else 'None'}\n"
            f"Started by: {interaction.user}",
            interaction.guild.id,
            interaction.user
        )
        
        # Schedule the giveaway end
        if interaction.guild.id not in bot.active_giveaways:
            bot.active_giveaways[interaction.guild.id] = {}
            
        end_time = datetime.utcnow() + timedelta(minutes=duration)
        bot.active_giveaways[interaction.guild.id][message.id] = bot.loop.create_task(
            end_giveaway(interaction.guild.id, message.id, duration * 60)
        )
        
        # Start countdown task
        if interaction.guild.id not in bot.countdown_tasks:
            bot.countdown_tasks[interaction.guild.id] = {}
        bot.countdown_tasks[interaction.guild.id][message.id] = bot.loop.create_task(
            CountdownUpdater.update_giveaway_countdown(interaction.guild.id, message.id, end_time)
        )
        
    except Exception as e:
        await log_activity("giveaway error", f"error starting giveaway in {interaction.guild.name}: {e}", interaction.guild.id, interaction.user)
        await interaction.followup.send("❌ Giveaway failed to start", ephemeral=True)

# Logging functions
async def log_activity(action: str, details: str, guild_id: Optional[int] = None, user: Optional[discord.User] = None):
    """Log bot activity to configured channel or console."""
    try:
        mention = user.mention if user else "system"
        config = await get_guild_config(guild_id) if guild_id else {}
        if guild_id and config.get("bot_log"):
            channel = bot.get_channel(config["bot_log"])
            if channel:
                embed = Embed(
                    title="📋 Bot Activity Log",
                    description=f"**{action}**\n**By:** {mention}\n{details}",
                    color=0x5865F2,
                    timestamp=datetime.utcnow()
                )
                await channel.send(embed=embed)
                return
        
        logger.info(f"{action} - {details}")
    except Exception as e:
        logger.error(f"Logging error: {e}")

async def log_giveaway(action: str, details: str, guild_id: int, user: Optional[discord.User] = None):
    """Log giveaway activity to configured channel."""
    try:
        mention = user.mention if user else "system"
        config = await get_guild_config(guild_id)
        if config.get("giveaway_log"):
            channel = bot.get_channel(config["giveaway_log"])
            if channel:
                embed = Embed(
                    title="🎉 Giveaway Log",
                    description=f"**{action}**\n**By:** {mention}\n{details}",
                    color=0xF47FFF,
                    timestamp=datetime.utcnow()
                )
                await channel.send(embed=embed)
    except Exception as e:
        await log_activity("giveaway log error", f"failed to log giveaway activity: {e}", guild_id)

async def log_invite(guild_id: int, inviter: discord.Member, invited: discord.Member):
    """Log invite activity to configured channel."""
    try:
        config = await get_guild_config(guild_id)
        if config.get("invite_log"):
            channel = bot.get_channel(config["invite_log"])
            if channel:
                embed = Embed(
                    title="📥 New Member Joined",
                    description=(
                        f"{invited.mention} joined the server!\n"
                        f"**Invited By:** {inviter.mention}\n"
                        f"**Inviter's Total Invites:** {await get_invite_count(guild_id, inviter.id)}"
                    ),
                    color=0x57F287,
                    timestamp=datetime.utcnow()
                )
                embed.set_thumbnail(url=invited.display_avatar.url)
                
                # Add server icon if available
                guild = bot.get_guild(guild_id)
                if guild and guild.icon:
                    embed.set_author(name=guild.name, icon_url=guild.icon.url)
                
                await channel.send(embed=embed)
    except Exception as e:
        await log_activity("invite log error", f"failed to log invite: {e}", guild_id)

# Utility functions
async def cache_invites(guild: discord.Guild):
    """Cache all invites for a guild."""
    try:
        invites = await guild.invites()
        bot.invite_cache[guild.id] = {inv.code: inv for inv in invites}
        logger.info(f"Cached {len(invites)} invites for {guild.name}")
    except Exception as e:
        await log_activity("invite caching error", f"failed to cache invites in {guild.name}: {e}", guild.id)

async def check_giveaway_eligibility(guild_id: int, member: discord.Member, message_id: int) -> bool:
    """Check if a member is eligible for a giveaway."""
    try:
        giveaway = bot.giveaway_participants.get(guild_id, {}).get(message_id)
        if not giveaway:
            return False
            
        guild_processed = bot.processed_users.get(guild_id, {})
        if message_id in guild_processed and member.id in guild_processed[message_id]:
            return False
            
        if guild_id not in bot.processed_users:
            bot.processed_users[guild_id] = {}
        if message_id not in bot.processed_users[guild_id]:
            bot.processed_users[guild_id][message_id] = []
            
        bot.processed_users[guild_id][message_id].append(member.id)
        
        # Check if user has bypass role
        if giveaway.get('bypass_role_id'):
            bypass_role = member.guild.get_role(giveaway['bypass_role_id'])
            if bypass_role and bypass_role in member.roles:
                await set_invite_count(guild_id, member.id, await get_invite_count(guild_id, member.id))
                await log_activity("giveaway entry with bypass",
                                 f"{member} ({member.id}) entered giveaway with bypass role for {giveaway['prize']} in {member.guild.name}",
                                 guild_id)
                
                view = GiveawayView(guild_id, giveaway['min_invites'], 
                                  giveaway.get('required_role_id'), giveaway.get('min_messages'), 
                                  message_id, giveaway.get('bypass_role_id'))
                await view.update_embed()
                return True
        
        # Check all requirements
        error_message = None
        
        # Check invite requirement (only if min_invites > 0)
        if giveaway['min_invites'] > 0:
            invite_count = await get_invite_count(guild_id, member.id)
            if invite_count < giveaway['min_invites']:
                error_message = f"You only have **{invite_count}** invites, but **{giveaway['min_invites']}** are needed."
        
        # Check required role
        if not error_message and giveaway.get('required_role_id'):
            required_role = member.guild.get_role(giveaway['required_role_id'])
            if not required_role or required_role not in member.roles:
                error_message = f"You don't have the required role {required_role.mention if required_role else ''}."
        
        # Check message requirement
        if not error_message and giveaway.get('min_messages'):
            message_count = await get_message_count(guild_id, member.id)
            if message_count < giveaway['min_messages']:
                error_message = f"You only have **{message_count}** messages, but **{giveaway['min_messages']}** are required."
        
        if error_message:
            try:
                await member.send(embed=Embed(
                    title="❌ Not Eligible for Giveaway",
                    description=(
                        f"{error_message}\n\n"
                        f"**Giveaway Prize:** {giveaway['prize']}\n"
                        f"**Giveaway Link:** {giveaway.get('message_link', 'N/A')}\n\n"
                        "**Requirements:**\n"
                        + (f"- Minimum {giveaway['min_invites']} invites\n" if giveaway['min_invites'] > 0 else "")
                        + (f"- Required role\n" if giveaway.get('required_role_id') else "")
                        + (f"- Minimum {giveaway['min_messages']} messages\n" if giveaway.get('min_messages') else "")
                        + (f"- Bypass role: <@&{giveaway['bypass_role_id']}> (bypasses all requirements)\n" if giveaway.get('bypass_role_id') else "")
                    ),
                    color=0xED4245
                ))
            except Exception:
                pass
            return False

        await set_invite_count(guild_id, member.id, await get_invite_count(guild_id, member.id))
        await log_activity("giveaway entry",
                          f"{member} ({member.id}) entered giveaway for {giveaway['prize']} in {member.guild.name}\n"
                          f"Invites: {await get_invite_count(guild_id, member.id)}, Messages: {await get_message_count(guild_id, member.id)}",
                          guild_id)

        view = GiveawayView(guild_id, giveaway['min_invites'], 
                          giveaway.get('required_role_id'), giveaway.get('min_messages'), 
                          message_id, giveaway.get('bypass_role_id'))
        await view.update_embed()
        return True
    except Exception as e:
        logger.error(f"Eligibility error for {member}: {e}")
        await log_activity("eligibility error", f"error checking eligibility for {member} in {member.guild.name}: {e}", guild_id)
        return False

# Context menu command
@bot.tree.context_menu(name="user info")
async def user_info_context(interaction: Interaction, member: discord.Member):
    """Context menu command to show user info."""
    await show_user_info(interaction, member)

async def show_user_info(interaction: Interaction, member: discord.Member):
    """Display information about a user."""
    try:
        embed = Embed(
            title=f"👤 User Info: {member.display_name}",
            color=member.color or 0x5865F2,
            timestamp=datetime.utcnow()
        )
        
        embed.set_thumbnail(url=member.display_avatar.url)
        
        embed.add_field(name="📛 Username", value=f"{member.name}#{member.discriminator}", inline=True)
        embed.add_field(name="🏷️ Nickname", value=member.nick or "None", inline=True)
        embed.add_field(name="🆔 ID", value=f"`{member.id}`", inline=False)
        
        embed.add_field(
            name="📅 Account Created", 
            value=f"<t:{int(member.created_at.timestamp())}:F>\n(<t:{int(member.created_at.timestamp())}:R>)", 
            inline=True
        )
        
        embed.add_field(
            name="📥 Joined Server", 
            value=f"<t:{int(member.joined_at.timestamp())}:F>\n(<t:{int(member.joined_at.timestamp())}:R>)", 
            inline=True
        )
        
        # Add invite count
        invite_count = await get_invite_count(interaction.guild.id, member.id)
        embed.add_field(name="📨 Invites", value=f"`{invite_count}`", inline=True)
        
        # Add message count
        message_count = await get_message_count(interaction.guild.id, member.id)
        embed.add_field(name="💬 Messages", value=f"`{message_count}`", inline=True)
        
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        await log_activity("user info viewed", 
                         f"{interaction.user} viewed info for {member.display_name}",
                         interaction.guild.id, interaction.user)
    except Exception as e:
        await interaction.response.send_message("❌ Failed to fetch user info", ephemeral=True)
        await log_activity("user info error", f"error: {str(e)}", interaction.guild.id, interaction.user)

# ===== TRYOUTLOG COMMAND =====

@bot.tree.command(name="tryoutlog", description="log a tryout match")
@app_commands.describe(
    region="region where the tryout took place",
    tester="the tester (who conducted the tryout)",
    opponent="the opponent (who was being tested)",
    winner="who won the tryout",
    score="final score (e.g., 3-2)",
    note="additional notes or observations",
    clip="link to match clip/video (optional)"
)
async def tryoutlog(
    interaction: Interaction,
    region: str,
    tester: discord.Member,
    opponent: discord.Member,
    winner: discord.Member,
    score: str,
    note: str,
    clip: Optional[str] = None
):
    """Log a tryout match."""
    try:
        embed = Embed(
            title="🎮 Tryout Match Logged",
            color=0x5865F2,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="🌍 Region", value=region, inline=True)
        embed.add_field(name="🧪 Tester", value=tester.mention, inline=True)
        embed.add_field(name="⚔️ Opponent", value=opponent.mention, inline=True)
        embed.add_field(name="🏆 Winner", value=winner.mention, inline=True)
        embed.add_field(name="📊 Score", value=score, inline=True)
        embed.add_field(name="📝 Notes", value=note, inline=False)
        
        if clip:
            embed.add_field(name="🎬 Clip", value=f"[Watch Here]({clip})", inline=False)
        
        embed.set_footer(text=f"Logged by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed)
        await log_activity(
            "tryout logged",
            f"{interaction.user} logged a tryout:\n"
            f"Region: {region}\nTester: {tester}\nOpponent: {opponent}\n"
            f"Winner: {winner}\nScore: {score}",
            interaction.guild.id,
            interaction.user
        )
    except Exception as e:
        await interaction.response.send_message("❌ Failed to log tryout")
        await log_activity("tryout log error", f"error: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="warlog", description="log a crew war")
@app_commands.describe(
    region="region where the war took place",
    opposing_crew="name of the opposing crew",
    fighters="list of your crew's fighters (comma separated)",
    opponents="list of opposing fighters (comma separated)",
    score="final score (e.g., 5-3)",
    winner="who won the war (your crew or opposing crew)",
    note="additional notes or observations",
    clip="link to war clip/video (optional)"
)
async def warlog(
    interaction: Interaction,
    region: str,
    opposing_crew: str,
    fighters: str,
    opponents: str,
    score: str,
    winner: str,
    note: str,
    clip: Optional[str] = None
):
    """Log a crew war."""
    try:
        embed = Embed(
            title="⚔️ Crew War Logged",
            color=0xED4245,
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="🌍 Region", value=region, inline=True)
        embed.add_field(name="🎯 Opposing Crew", value=opposing_crew, inline=True)
        embed.add_field(name="👥 Our Fighters", value=fighters, inline=False)
        embed.add_field(name="👥 Their Fighters", value=opponents, inline=False)
        embed.add_field(name="📊 Score", value=score, inline=True)
        embed.add_field(name="🏆 Winner", value=winner, inline=True)
        embed.add_field(name="📝 Notes", value=note, inline=False)
        
        if clip:
            embed.add_field(name="🎬 Clip", value=f"[Watch Here]({clip})", inline=False)
        
        embed.set_footer(text=f"Logged by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed)
        await log_activity(
            "crew war logged",
            f"{interaction.user} logged a crew war:\n"
            f"Region: {region}\nOpposing Crew: {opposing_crew}\n"
            f"Score: {score}\nWinner: {winner}",
            interaction.guild.id,
            interaction.user
        )
    except Exception as e:
        await interaction.response.send_message("❌ Failed to log crew war")
        await log_activity("war log error", f"error: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="verify_setup", description="set up verification system")
@app_commands.describe(
    channel="channel where verification message will be sent",
    unverified_role="role given to unverified members",
    verified_role="role given after verification",
    log_channel="channel for verification logs",
    title="title for verification embed",
    description="description for verification embed"
)
async def verify_setup(
    interaction: Interaction,
    channel: discord.TextChannel,
    unverified_role: discord.Role,
    verified_role: discord.Role,
    log_channel: discord.TextChannel,
    title: str,
    description: str
):
    """Set up verification system."""
    try:
        # Save configuration
        await set_verification_config(
            interaction.guild.id,
            channel.id,
            unverified_role.id,
            verified_role.id,
            log_channel.id,
            title,
            description
        )
        
        # Create verification embed
        embed = Embed(
            title=title,
            description=description,
            color=0x57F287
        )
        embed.set_footer(text="Verification System")
        
        # Send verification message
        view = VerificationView()
        verification_message = await channel.send(embed=embed, view=view)
        
        # Send confirmation
        confirm_embed = Embed(
            title="✅ Verification System Setup Complete",
            description=(
                f"**Verification Channel:** {channel.mention}\n"
                f"**Unverified Role:** {unverified_role.mention}\n"
                f"**Verified Role:** {verified_role.mention}\n"
                f"**Log Channel:** {log_channel.mention}\n\n"
                f"Verification message has been sent to {channel.mention}"
            ),
            color=0x57F287
        )
        
        await interaction.response.send_message(embed=confirm_embed)
        await log_activity(
            "verification setup",
            f"{interaction.user} set up verification system:\n"
            f"Channel: {channel.name}\nUnverified Role: {unverified_role.name}\n"
            f"Verified Role: {verified_role.name}\nLog Channel: {log_channel.name}",
            interaction.guild.id,
            interaction.user
        )
        
    except Exception as e:
        await interaction.response.send_message("❌ Failed to set up verification system")
        await log_activity("verification setup error", f"error: {str(e)}", interaction.guild.id, interaction.user)

# ===== FIXED GIVEAWAY REROLL COMMAND =====

@bot.tree.command(name="giveaway_reroll", description="reroll a giveaway with new winner")
@app_commands.describe(message_id="message ID of the ended giveaway")
async def giveaway_reroll(interaction: Interaction, message_id: str):
    """Reroll a giveaway with a new winner."""
    # Defer the response but NOT ephemeral so everyone can see
    await interaction.response.defer()
    
    try:
        try:
            message_id_int = int(message_id)
        except ValueError:
            await interaction.followup.send("❌ Invalid message ID format. Please provide a valid message ID.", ephemeral=True)
            return
        
        guild = interaction.guild
        message = None
        channel = None
        
        # Try to find the message in any text channel
        for ch in guild.text_channels:
            try:
                msg = await ch.fetch_message(message_id_int)
                if msg and msg.embeds:
                    message = msg
                    channel = ch
                    break
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue
        
        if not message:
            await interaction.followup.send(
                "❌ Could not find an ended giveaway with that message ID. Make sure:\n"
                "• The message ID is correct\n"
                "• The giveaway has ended\n"
                "• The message is in a channel I can access",
                ephemeral=True
            )
            return
        
        # Parse the embed to get giveaway information
        embed = message.embeds[0]
        description = embed.description or ""
        
        import re
        
        # EXTRACT PRIZE - Multiple methods to ensure we get it
        prize = "Unknown"
        
        # Method 1: Look for prize in the format "**Prize:** [prize name]"
        prize_match = re.search(r'\*\*Prize:\*\*\s*(.*?)(?:\n|$)', description)
        if prize_match:
            prize = prize_match.group(1).strip()
        
        # Method 2: Look in embed fields if not found in description
        if prize == "Unknown" and embed.fields:
            for field in embed.fields:
                if "prize" in field.name.lower():
                    prize = field.value
                    break
        
        # Method 3: Look for any text after "Prize:" in the entire embed
        if prize == "Unknown":
            full_text = f"{embed.title or ''} {description}"
            alt_prize_match = re.search(r'Prize:?\s*([^\n]+)', full_text, re.IGNORECASE)
            if alt_prize_match:
                prize = alt_prize_match.group(1).strip()
        
        # Get winners count
        winners_count = 1
        winners_match = re.search(r'\*\*Winners:\*\*\s*(\d+)', description)
        if winners_match:
            winners_count = int(winners_match.group(1))
        
        # Extract requirements
        min_invites = 0
        required_role_id = None
        min_messages = None
        bypass_role_id = None
        blocked_roles = []
        
        if "Requirements:" in description:
            invites_match = re.search(r'Minimum (\d+) invites', description)
            if invites_match:
                min_invites = int(invites_match.group(1))
            
            messages_match = re.search(r'Minimum (\d+) messages', description)
            if messages_match:
                min_messages = int(messages_match.group(1))
            
            role_match = re.search(r'Required role: <@&(\d+)>', description)
            if role_match:
                required_role_id = int(role_match.group(1))
            
            bypass_match = re.search(r'Bypass role: <@&(\d+)>', description)
            if bypass_match:
                bypass_role_id = int(bypass_match.group(1))
            
            blocked_match = re.search(r'Blocked Roles: (.*?)(?:\n|$)', description)
            if blocked_match and blocked_match.group(1) != 'None':
                blocked_roles = [int(rid) for rid in re.findall(r'<@&(\d+)>', blocked_match.group(1))]
        
        # Get previous winners
        previous_winners = []
        winner_section = re.search(r'\*\*Winner\(s\)\*\*\s*([^\n]+)', description)
        if winner_section:
            winner_text = winner_section.group(1)
            winner_ids = re.findall(r'<@!?(\d+)>', winner_text)
            previous_winners = [int(uid) for uid in winner_ids]
        
        # Try to find winners in embed fields if not found in description
        if not previous_winners and embed.fields:
            for field in embed.fields:
                if "winner" in field.name.lower():
                    winner_ids = re.findall(r'<@!?(\d+)>', field.value)
                    previous_winners = [int(uid) for uid in winner_ids]
                    break
        
        # Get participants from reactions
        participants = []
        for reaction in message.reactions:
            if str(reaction.emoji) == "🎉":
                async for user in reaction.users():
                    if not user.bot:
                        member = guild.get_member(user.id)
                        if member:
                            participants.append(member)
        
        # If no participants from reactions, try to get from cache
        if not participants:
            for guild_id, giveaways in bot.giveaway_participants.items():
                if message_id_int in giveaways:
                    cached_users = giveaways[message_id_int].get('users', [])
                    for user_id in cached_users:
                        member = guild.get_member(user_id)
                        if member:
                            participants.append(member)
                    break
        
        # If still no participants, check if embed shows participant count
        if not participants:
            participants_match = re.search(r'Total Participants:\s*(\d+)', description)
            if participants_match:
                total = int(participants_match.group(1))
                await interaction.followup.send(
                    f"❌ Cannot reroll this giveaway.\n"
                    f"The giveaway had **{total}** participants, but I can't access their information.\n"
                    f"This happens when reactions are cleared and the participant data is no longer available.\n\n"
                    f"**To fix this for future giveaways:** The bot has been updated to **NOT clear reactions** when giveaways end. Future giveaways will keep their reactions so rerolls will work.",
                    ephemeral=True
                )
                return
            
            await interaction.followup.send(
                "❌ No participants found for this giveaway.\n"
                "This likely happened because the reactions were cleared when the giveaway ended.\n\n"
                "**The bot has been updated to fix this issue.** Future giveaways will keep their reactions so rerolls will work properly.",
                ephemeral=True
            )
            return
        
        # Filter eligible participants based on requirements
        eligible_participants = []
        ineligible_count = 0
        
        for participant in participants:
            try:
                member = await guild.fetch_member(participant.id)
                if not member:
                    ineligible_count += 1
                    continue
                
                # Skip previous winners
                if member.id in previous_winners:
                    ineligible_count += 1
                    continue
                
                # Check all requirements
                eligible = True
                
                # Check bypass role first
                if bypass_role_id:
                    bypass_role = guild.get_role(bypass_role_id)
                    if bypass_role and bypass_role in member.roles:
                        eligible_participants.append(member)
                        continue
                
                # Check blocked roles
                if any(role.id in blocked_roles for role in member.roles):
                    eligible = False
                    ineligible_count += 1
                    continue
                
                # Check invite requirement
                if min_invites > 0:
                    invite_count = await get_invite_count(guild.id, member.id)
                    if invite_count < min_invites:
                        eligible = False
                        ineligible_count += 1
                        continue
                
                # Check required role
                if required_role_id:
                    required_role = guild.get_role(required_role_id)
                    if not required_role or required_role not in member.roles:
                        eligible = False
                        ineligible_count += 1
                        continue
                
                # Check message requirement
                if min_messages:
                    message_count = await get_message_count(guild.id, member.id)
                    if message_count < min_messages:
                        eligible = False
                        ineligible_count += 1
                        continue
                
                if eligible:
                    eligible_participants.append(member)
                    
            except Exception as e:
                logger.error(f"Error checking eligibility for {participant.id}: {e}")
                ineligible_count += 1
                continue
        
        # If no one is eligible but we have participants and no requirements, use all non-previous-winner participants
        if not eligible_participants and min_invites == 0 and not required_role_id and not min_messages:
            eligible_participants = [p for p in participants if p.id not in previous_winners]
        
        if not eligible_participants:
            await interaction.followup.send(
                f"❌ No eligible participants available for reroll.\n"
                f"**Stats:**\n"
                f"• Total participants found: {len(participants)}\n"
                f"• Ineligible/previous winners: {ineligible_count}\n"
                f"• Eligible for reroll: 0",
                ephemeral=True
            )
            return
        
        # Select new winner(s)
        num_winners = min(winners_count, len(eligible_participants))
        new_winners = random.sample(eligible_participants, num_winners)
        
        # Create reroll result embed
        winners_text = "\n".join([f"🏆 {winner.mention}" for winner in new_winners])
        
        # Build requirements text
        requirements_text = []
        if min_invites > 0:
            requirements_text.append(f"• {min_invites} invites")
        if required_role_id:
            role = guild.get_role(required_role_id)
            requirements_text.append(f"• Required role: {role.mention if role else 'Unknown'}")
        if min_messages:
            requirements_text.append(f"• {min_messages} messages")
        if bypass_role_id:
            role = guild.get_role(bypass_role_id)
            requirements_text.append(f"• Bypass role: {role.mention if role else 'Unknown'}")
        
        reroll_embed = Embed(
            title="🎉 Giveaway Rerolled!",
            description=(
                f"**🎁 Prize:** {prize}\n"
                f"**🏆 New Winner(s):**\n{winners_text}\n\n"
                f"**📊 Stats:**\n"
                f"• Total Participants: {len(participants)}\n"
                f"• Eligible for Reroll: {len(eligible_participants)}\n"
                f"• Previous Winners: {len(previous_winners)}\n"
                f"• Winners Selected: {len(new_winners)}\n"
            ),
            color=0xF47FFF,
            timestamp=datetime.utcnow()
        )
        
        if requirements_text:
            reroll_embed.add_field(
                name="📋 Requirements",
                value="\n".join(requirements_text),
                inline=False
            )
        
        reroll_embed.set_footer(text=f"Rerolled by {interaction.user.display_name}")
        
        # Send the result (not ephemeral so everyone can see)
        await interaction.followup.send(embed=reroll_embed)
        
        # Notify new winners
        for winner in new_winners:
            try:
                winner_embed = Embed(
                    title="🎉 You Won! (Reroll)",
                    description=(
                        f"Congratulations! You won **{prize}** in **{guild.name}**!\n\n"
                        f"This is a reroll of a previous giveaway.\n"
                        f"Please contact staff to claim your prize."
                    ),
                    color=0xF47FFF
                )
                winner_embed.set_footer(text="Congratulations from the server staff!")
                await winner.send(embed=winner_embed)
            except Exception as e:
                logger.error(f"Failed to DM winner {winner}: {e}")
        
        await log_activity(
            "giveaway rerolled",
            f"{interaction.user} rerolled giveaway for {prize}\n"
            f"New winner(s): {', '.join([str(w) for w in new_winners])}\n"
            f"Original giveaway: {message.jump_url}",
            interaction.guild.id,
            interaction.user
        )
        
    except Exception as e:
        logger.error(f"Giveaway reroll error: {e}")
        await interaction.followup.send(
            f"❌ Failed to reroll giveaway: {str(e)}",
            ephemeral=True
        )
        await log_activity("giveaway reroll error", f"error: {str(e)}", interaction.guild.id, interaction.user)

# ===== USER INFO COMMAND =====

@bot.tree.command(name="user_info", description="get information about a user")
@app_commands.describe(user="the user to get information about")
async def user_info(interaction: Interaction, user: Optional[discord.Member] = None):
    """Slash command to show user info."""
    target = user or interaction.user
    await show_user_info(interaction, target)

@bot.tree.command(name="server_info", description="get information about this server")
async def server_info(interaction: Interaction):
    """Display information about the server."""
    try:
        guild = interaction.guild
        
        embed = Embed(
            title=f"🏰 Server Info: {guild.name}",
            color=0x5865F2,
            timestamp=datetime.utcnow()
        )
        
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)
        
        embed.add_field(name="👑 Owner", value=guild.owner.mention, inline=True)
        embed.add_field(name="🆔 ID", value=f"`{guild.id}`", inline=True)
        embed.add_field(name="📅 Created", 
                      value=f"<t:{int(guild.created_at.timestamp())}:F>\n(<t:{int(guild.created_at.timestamp())}:R>)", 
                      inline=True)
        
        embed.add_field(name="👥 Members", value=f"`{guild.member_count}`", inline=True)
        embed.add_field(name="🎭 Roles", value=f"`{len(guild.roles)}`", inline=True)
        embed.add_field(name="💬 Channels", 
                       value=f"💬 Text: `{len(guild.text_channels)}`\n🎤 Voice: `{len(guild.voice_channels)}`", 
                       inline=True)
        
        if guild.banner:
            embed.set_image(url=guild.banner.url)
        
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed)
        await log_activity("server info viewed", 
                         f"{interaction.user} viewed server info",
                         interaction.guild.id, interaction.user)
    except Exception as e:
        await interaction.response.send_message("❌ Failed to fetch server info")
        await log_activity("server info error", f"error: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="clear", description="clear messages from a channel")
@app_commands.describe(
    amount="number of messages to clear (1-500)"
)
async def clear(
    interaction: Interaction, 
    amount: app_commands.Range[int, 1, 500]
):
    """Clear messages from a channel."""
    try:
        await interaction.response.defer(ephemeral=True)
        
        deleted = await interaction.channel.purge(limit=amount)
        
        embed = Embed(
            title="🧹 Messages Cleared",
            description=f"Deleted `{len(deleted)}` messages.",
            color=0x57F287
        )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_activity(
            "messages cleared", 
            f"{interaction.user} cleared {len(deleted)} messages",
            interaction.guild.id, 
            interaction.user
        )
    except Exception as e:
        await interaction.followup.send("❌ Failed to clear messages", ephemeral=True)
        await log_activity("clear error", f"error: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="help", description="show help information")
async def help_command(interaction: Interaction):
    """Display help information about the bot."""
    try:
        embed = Embed(
            title="🤖 Blaze-Bot Help",
            description=(
                "Thanks for using Blaze-Bot! Here are all available commands:\n\n"
                
                "🎉 **Giveaways**\n"
                "`/start_giveaway` - Start a new giveaway with requirements\n"
                "`/cancel_giveaway` - Cancel an active giveaway\n"
                "`/giveaway_reroll` - Reroll a giveaway with new winner\n\n"
                
                "⚙️ **Moderation**\n"
                "`/clear` - Delete messages (1-500)\n"
                "`/assign_role` - Assign a role to a user\n"
                "`/remove_role` - Remove a role from a user\n\n"
                
                "📊 **Polls**\n"
                "`/create_poll` - Create a new poll with options\n\n"
                
                "📝 **Logging**\n"
                "`/tryoutlog` - Log a tryout match\n"
                "`/warlog` - Log a crew war\n"
                "`/pvp` - Log a PvP match\n\n"
                
                "🎫 **Tickets**\n"
                "`/setup_tickets` - Set up the ticket system\n\n"
                
                "🛡️ **Verification**\n"
                "`/verify_setup` - Set up verification system\n\n"
                
                "ℹ️ **Info**\n"
                "`/user_info` - Get user information\n"
                "`/server_info` - Get server information\n"
                "`/help` - Show this message\n\n"
                
                "👋 **Fun**\n"
                "`/greeting` - Get a friendly greeting"
            ),
            color=0x5865F2
        )
        
        embed.set_footer(text="Use slash commands (/) to access these features")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        await log_activity("help command used", f"{interaction.user} viewed help", interaction.guild.id, interaction.user)
    except Exception as e:
        await interaction.response.send_message("❌ Failed to display help", ephemeral=True)
        await log_activity("help error", f"error: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="greeting", description="get a friendly greeting from the bot")
async def greeting(interaction: Interaction):
    """Send a friendly greeting."""
    try:
        # Define the four selectable greeting emojis
        greeting_emojis = ["👋", "🤗", "😊", "🌟"]
        
        # Track which emoji to use for this user/guild
        # Use a simple rotation based on command usage count
        user_key = f"{interaction.guild.id}_{interaction.user.id}"
        
        # Get or initialize the emoji index for this user
        if not hasattr(bot, 'greeting_counts'):
            bot.greeting_counts = {}
        
        if user_key not in bot.greeting_counts:
            bot.greeting_counts[user_key] = 0
        else:
            bot.greeting_counts[user_key] = (bot.greeting_counts[user_key] + 1) % len(greeting_emojis)
        
        # Get the current emoji
        current_emoji_index = bot.greeting_counts[user_key]
        selected_emoji = greeting_emojis[current_emoji_index]
        
        # Send greeting matching the image format: hello @username! with emoji
        greeting_message = f"hello {interaction.user.mention}! {selected_emoji}"
        
        await interaction.response.send_message(greeting_message)
        
        await log_activity(
            "greeting command used",
            f"{interaction.user} used the greeting command in {interaction.guild.name} (emoji: {selected_emoji})",
            interaction.guild.id,
            interaction.user
        )
    except Exception as e:
        await interaction.response.send_message("❌ Failed to send greeting")
        await log_activity("greeting error", f"error in {interaction.guild.name}: {e}", interaction.guild.id, interaction.user)

@bot.tree.command(name="assign_role", description="assign a role to a user")
@app_commands.describe(
    user="the user to assign the role to",
    role="the role to assign"
)
async def assign_role(interaction: Interaction, user: discord.Member, role: discord.Role):
    """Assign a role to a user."""
    try:
        # Check if bot can manage the role
        if role >= interaction.guild.me.top_role:
            return await interaction.response.send_message("❌ I can't assign that role as it's higher than my highest role.", ephemeral=True)
        
        # Check if user can assign this role (their highest role must be higher than the role being assigned)
        if interaction.user != interaction.guild.owner:
            user_highest_role = max(interaction.user.roles, key=lambda r: r.position)
            if role.position >= user_highest_role.position:
                return await interaction.response.send_message(
                    "❌ You can't assign a role that is higher than or equal to your highest role.",
                    ephemeral=True
                )
        
        # Check if user already has the role
        if role in user.roles:
            return await interaction.response.send_message(f"❌ {user.mention} already has the {role.mention} role.", ephemeral=True)
        
        # Assign the role
        await user.add_roles(role)
        
        embed = Embed(
            title="✅ Role Assigned",
            description=f"Successfully assigned {role.mention} to {user.mention}",
            color=0x57F287
        )
        
        await interaction.response.send_message(embed=embed)
        await log_activity(
            "role assigned", 
            f"{interaction.user} assigned {role.name} to {user} in {interaction.guild.name}", 
            interaction.guild.id, 
            interaction.user
        )
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ I don't have permission to assign that role.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("❌ Failed to assign role", ephemeral=True)
        await log_activity("role assign error", f"error in {interaction.guild.name}: {str(e)}", interaction.guild.id, interaction.user)

@bot.tree.command(name="remove_role", description="remove a role from a user")
@app_commands.describe(
    user="the user to remove the role from",
    role="the role to remove"
)
async def remove_role(interaction: Interaction, user: discord.Member, role: discord.Role):
    """Remove a role from a user."""
    try:
        # Check if bot can manage the role
        if role >= interaction.guild.me.top_role:
            return await interaction.response.send_message("❌ I can't remove that role as it's higher than my highest role.", ephemeral=True)
        
        # Check if user can remove this role (their highest role must be higher than the role being removed)
        if interaction.user != interaction.guild.owner:
            user_highest_role = max(interaction.user.roles, key=lambda r: r.position)
            if role.position >= user_highest_role.position:
                return await interaction.response.send_message(
                    "❌ You can't remove a role that is higher than or equal to your highest role.",
                    ephemeral=True
                )
        
        # Check if user has the role
        if role not in user.roles:
            return await interaction.response.send_message(f"❌ {user.mention} doesn't have the {role.mention} role.", ephemeral=True)
        
        # Remove the role
        await user.remove_roles(role)
        
        embed = Embed(
            title="✅ Role Removed",
            description=f"Successfully removed {role.mention} from {user.mention}",
            color=0x57F287
        )
        
        await interaction.response.send_message(embed=embed)
        await log_activity(
            "role removed", 
            f"{interaction.user} removed {role.name} from {user} in {interaction.guild.name}", 
            interaction.guild.id, 
            interaction.user
        )
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ I don't have permission to remove that role.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("❌ Failed to remove role", ephemeral=True)
        await log_activity("role remove error", f"error in {interaction.guild.name}: {str(e)}", interaction.guild.id, interaction.user)

# Event handlers
@bot.event
async def on_message(message: discord.Message):
    """Handle message events."""
    if message.author.bot or not message.guild:
        return
    
    # Track message count for user
    await increment_message_count(message.guild.id, message.author.id)
    
    # Track active users
    guild_id = message.guild.id
    channel_id = message.channel.id
    
    if guild_id not in bot.active_users:
        bot.active_users[guild_id] = {}
    if channel_id not in bot.active_users[guild_id]:
        bot.active_users[guild_id][channel_id] = {}
    
    bot.active_users[guild_id][channel_id][message.author.id] = datetime.utcnow()
    
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction: discord.Reaction, user: discord.User):
    """Handle reaction adds for giveaways."""
    if user.bot:
        return
        
    guild_id = reaction.message.guild.id if reaction.message.guild else None
    
    # Handle giveaway participation
    if str(reaction.emoji) == "🎉" and guild_id and guild_id in bot.giveaway_participants and reaction.message.id in bot.giveaway_participants[guild_id]:
        giveaway = bot.giveaway_participants[guild_id][reaction.message.id]
        member = reaction.message.guild.get_member(user.id)
        if not member:
            return
            
        # Prevent hoster from participating
        if member.id == giveaway['hoster'].id:
            try:
                await reaction.remove(user)
                embed = Embed(
                    title="❌ Giveaway Entry Denied",
                    description="You cannot participate in your own giveaway!",
                    color=0xED4245
                )
                await member.send(embed=embed)
            except discord.Forbidden:
                pass
            except Exception as e:
                logger.error(f"Error handling hoster reaction: {e}")
            return
            
        # Check for blocked roles
        if any(role.id in giveaway.get('blocked_roles', []) for role in member.roles):
            try:
                await reaction.remove(user)
                embed = Embed(
                    title="❌ Giveaway Entry Denied",
                    description=(
                        f"You cannot participate in the **{giveaway['prize']}** giveaway!\n"
                        f"**Reason:** Your roles prevent participation\n"
                        f"**Giveaway Link:** {reaction.message.jump_url}"
                    ),
                    color=0xED4245
                )
                await member.send(embed=embed)
            except discord.Forbidden:
                pass
            except Exception as e:
                logger.error(f"Error handling blocked user: {e}")
            return
    
        # Add user to participants if not already there
        if user.id not in giveaway['users']:
            giveaway['users'].append(user.id)
            view = GiveawayView(guild_id, giveaway['min_invites'], 
                              giveaway.get('required_role_id'), giveaway.get('min_messages'), 
                              reaction.message.id, giveaway.get('bypass_role_id'))
            await view.update_embed()
            await log_activity("giveaway entry",
                              f"{member} joined giveaway for {giveaway['prize']} in {member.guild.name}",
                              guild_id)
    
        # Check eligibility
        await check_giveaway_eligibility(guild_id, member, reaction.message.id)

@bot.event
async def on_reaction_remove(reaction: discord.Reaction, user: discord.User):
    """Handle reaction removes for giveaways."""
    if user.bot or str(reaction.emoji) != "🎉":
        return
        
    guild_id = reaction.message.guild.id if reaction.message.guild else None
    if not guild_id or guild_id not in bot.giveaway_participants or reaction.message.id not in bot.giveaway_participants[guild_id]:
        return
        
    # Remove user from participants if they remove their reaction
    giveaway = bot.giveaway_participants[guild_id][reaction.message.id]
    if user.id in giveaway['users']:
        giveaway['users'].remove(user.id)
        view = GiveawayView(guild_id, giveaway['min_invites'], 
                          giveaway.get('required_role_id'), giveaway.get('min_messages'), 
                          reaction.message.id, giveaway.get('bypass_role_id'))
        await view.update_embed()
        await log_activity("giveaway leave",
                         f"{user} left giveaway for {giveaway['prize']} in {reaction.message.guild.name}",
                         guild_id)

async def end_giveaway(guild_id: int, message_id: int, delay: int):
    """End a giveaway after the specified delay."""
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return
    
    if guild_id not in bot.giveaway_participants or message_id not in bot.giveaway_participants[guild_id]:
        return
    
    try:
        giveaway = bot.giveaway_participants[guild_id][message_id]
        channel = bot.get_channel(giveaway['channel_id'])
        if not channel:
            return
            
        message = await channel.fetch_message(message_id)
        # DO NOT clear reactions - we keep them for rerolls
        # await message.clear_reactions()  # <-- THIS LINE IS REMOVED
        
        # Get all participants and check eligibility
        participants = []
        eligible_participants = []
        guild = bot.get_guild(guild_id)
        for user_id in giveaway['users']:
            try:
                member = await guild.fetch_member(user_id)
                participants.append(member)
                
                # Check all eligibility requirements
                eligible = True
                
                # Check bypass role first
                if giveaway.get('bypass_role_id'):
                    bypass_role = guild.get_role(giveaway['bypass_role_id'])
                    if bypass_role and bypass_role in member.roles:
                        eligible_participants.append(member)
                        continue
                
                # Check invite requirement (only if min_invites > 0)
                if eligible and giveaway['min_invites'] > 0 and await get_invite_count(guild_id, user_id) < giveaway['min_invites']:
                    eligible = False
                
                # Check required role
                if eligible and giveaway.get('required_role_id'):
                    required_role = guild.get_role(giveaway['required_role_id'])
                    if not required_role or required_role not in member.roles:
                        eligible = False
                
                # Check message requirement
                if eligible and giveaway.get('min_messages'):
                    message_count = await get_message_count(guild_id, user_id)
                    if message_count < giveaway['min_messages']:
                        eligible = False
                
                if eligible:
                    eligible_participants.append(member)
            except Exception:
                continue

        # Create results embed with improved design
        embed = Embed(
            title="🎉 Giveaway Ended!",
            color=0xF47FFF,
            timestamp=datetime.utcnow()
        )
        
        base_description = (
            f"**🎁 Prize:** {giveaway['prize']}\n"
            f"**⏰ Duration:** {giveaway['duration']}\n"
            f"**🏆 Winners:** {giveaway['winners']}\n"
            f"**🎯 Hosted By:** {giveaway['hoster'].mention}\n\n"
            f"**📋 Requirements:**\n"
            + (f"- Minimum {giveaway['min_invites']} invites\n" if giveaway['min_invites'] > 0 else "")
            + (f"- Required role: <@&{giveaway['required_role_id']}>\n" if giveaway.get('required_role_id') else "")
            + (f"- Minimum {giveaway['min_messages']} messages\n" if giveaway.get('min_messages') else "")
            + (f"- Bypass role: <@&{giveaway['bypass_role_id']}> (bypasses all requirements)\n" if giveaway.get('bypass_role_id') else "")
            + f"**📊 Participation Stats:**\n"
            + f"• **Total Participants:** {len(participants)}\n"
            + f"• **Eligible Participants:** {len(eligible_participants)}\n\n"
        )
        
        if eligible_participants:
            # Select winners
            winners = random.sample(eligible_participants, min(giveaway['winners'], len(eligible_participants)))
            winners_text = "\n".join([f"🏆 {winner.mention}" for winner in winners])
            
            embed.add_field(
                name="🎊 Winner(s)",
                value=winners_text,
                inline=False
            )
            
            # Notify winners and reset their invite counts (except those with bypass role)
            for winner in winners:
                try:
                    # Only reset invite count if winner doesn't have bypass role
                    if not (giveaway.get('bypass_role_id') and guild.get_role(giveaway['bypass_role_id']) in winner.roles):
                        await set_invite_count(guild_id, winner.id, 0)
                        await log_activity("invite count reset", f"reset invite count for winner {winner} ({winner.id}) in {guild.name}", guild_id)
                    
                    # Send winner DM
                    winner_embed = Embed(
                        title="🎉 You Won!",
                        description=(
                            f"Congratulations! You won **{giveaway['prize']}** in {guild.name}!\n\n"
                            f"**Giveaway Details:**\n"
                            f"• Prize: {giveaway['prize']}\n"
                            f"• Hosted by: {giveaway['hoster'].mention}\n"
                            f"• Total participants: {len(participants)}\n"
                            f"• Eligible participants: {len(eligible_participants)}\n\n"
                            + ("Your invite count has been reset to 0." if not (giveaway.get('bypass_role_id') and guild.get_role(giveaway['bypass_role_id']) in winner.roles) else "Your invite count was not reset due to bypass role.")
                        ),
                        color=0xF47FFF
                    )
                    winner_embed.set_footer(text="Congratulations from the server staff!")
                    await winner.send(embed=winner_embed)
                except Exception:
                    pass
        else:
            embed.add_field(
                name="❌ No Winners",
                value="There were no eligible participants for this giveaway.",
                inline=False
            )
        
        embed.description = base_description
        
        if giveaway.get('image_url'):
            embed.set_image(url=giveaway['image_url'])
        
        await message.edit(embed=embed)
        
        await log_giveaway(
            "giveaway completed",
            f"**Guild:** {guild.name}\n"
            f"**Prize:** {giveaway['prize']}\n"
            f"**Participants:** {len(participants)}\n"
            f"**Eligible:** {len(eligible_participants)}\n"
            f"**Winners:** {len(winners) if eligible_participants else 0}\n"
            f"**Hoster:** {giveaway['hoster'].mention}\n"
            f"[Jump to Giveaway]({message.jump_url})",
            guild_id
        )

        # Clean up
        if guild_id in bot.giveaway_participants and message_id in bot.giveaway_participants[guild_id]:
            del bot.giveaway_participants[guild_id][message_id]
        if guild_id in bot.active_giveaways and message_id in bot.active_giveaways[guild_id]:
            del bot.active_giveaways[guild_id][message_id]
        if guild_id in bot.processed_users and message_id in bot.processed_users[guild_id]:
            del bot.processed_users[guild_id][message_id]
        if guild_id in bot.countdown_tasks and message_id in bot.countdown_tasks[guild_id]:
            if not bot.countdown_tasks[guild_id][message_id].done():
                bot.countdown_tasks[guild_id][message_id].cancel()
            del bot.countdown_tasks[guild_id][message_id]

    except Exception as e:
        await log_activity("giveaway end error", f"error in guild {guild_id}: {e}", guild_id)

@bot.tree.command(name="cancel_giveaway", description="cancel an active giveaway")
@app_commands.describe(message_id="the giveaway message id to cancel")
async def cancel_giveaway(interaction: Interaction, message_id: str):
    """Cancel an active giveaway."""
    try:
        try:
            message_id = int(message_id)
        except ValueError:
            return await interaction.response.send_message("❌ Invalid message id", ephemeral=True)
            
        if interaction.guild.id not in bot.giveaway_participants or message_id not in bot.giveaway_participants[interaction.guild.id]:
            return await interaction.response.send_message("❌ No active giveaway found with that id", ephemeral=True)
        
        # Cancel the giveaway task
        if interaction.guild.id in bot.active_giveaways and message_id in bot.active_giveaways[interaction.guild.id]:
            bot.active_giveaways[interaction.guild.id][message_id].cancel()
            del bot.active_giveaways[interaction.guild.id][message_id]
        
        # Cancel the countdown task
        if interaction.guild.id in bot.countdown_tasks and message_id in bot.countdown_tasks[interaction.guild.id]:
            if not bot.countdown_tasks[interaction.guild.id][message_id].done():
                bot.countdown_tasks[interaction.guild.id][message_id].cancel()
            del bot.countdown_tasks[interaction.guild.id][message_id]
        
        # Update the giveaway message
        giveaway = bot.giveaway_participants[interaction.guild.id][message_id]
        channel = bot.get_channel(giveaway['channel_id'])
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(
                embed=Embed(
                    title="❌ Giveaway Cancelled",
                    description="This giveaway has been cancelled by staff.",
                    color=0xED4245
                )
            )
            await message.clear_reactions()
        except Exception:
            pass
            
        # Clean up
        del bot.giveaway_participants[interaction.guild.id][message_id]
        
        embed = Embed(
            title="✅ Giveaway Cancelled",
            description=f"Giveaway for **{giveaway['prize']}** has been cancelled successfully.",
            color=0x57F287
        )
        
        await interaction.response.send_message(embed=embed)
        await log_activity("giveaway cancelled", f"Cancelled by {interaction.user} in {interaction.guild.name}", interaction.guild.id, interaction.user)
        
    except Exception as e:
        await interaction.response.send_message("❌ Failed to cancel giveaway", ephemeral=True)
        await log_activity("giveaway cancel error", f"error in {interaction.guild.name}: {e}", interaction.guild.id, interaction.user)

async def update_member_count():
    """Update the bot's presence with the total member count."""
    async with bot.presence_lock:
        total_members = sum(g.member_count for g in bot.guilds)
        if total_members != bot.current_member_count:
            bot.current_member_count = total_members
            formatted = "{:,}".format(total_members)
            await bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"{formatted} Inferno's across the server!"
                )
            )

@bot.event
async def on_member_join(member: discord.Member):
    """Handle new member joins and track invites."""
    await update_member_count()
    
    try:
        guild = member.guild
        
        # Compare old and new invites to find which one was used
        old_invites = bot.invite_cache.get(guild.id, {})
        new_invites = await guild.invites()
        
        used_invite = None
        inviter = None
        
        # First check for usage increases
        for inv in new_invites:
            old_inv = old_invites.get(inv.code)
            if old_inv and inv.uses > old_inv.uses:
                used_invite = inv
                inviter = inv.inviter
                break
        
        # If no invite found by usage increase, check for new invites
        if not used_invite:
            for inv in new_invites:
                if inv.code not in old_invites:
                    used_invite = inv
                    inviter = inv.inviter
                    break
        
        # If we still haven't found an inviter, try to find any existing invite that might have been used
        if not inviter:
            for inv in new_invites:
                if inv.inviter and inv.inviter in guild.members:
                    inviter = inv.inviter
                    used_invite = inv
                    break
        
        # If we found the used invite and inviter, update their count
        if used_invite and inviter and inviter.id != member.id:
            # Track the inviter for this user
            await add_invite_history(guild.id, inviter.id, member.id)
            
            # Update the inviter's count
            current_count = await get_invite_count(guild.id, inviter.id)
            await set_invite_count(guild.id, inviter.id, current_count + 1)
            
            await log_activity(
                "member joined",
                f"Guild: {guild.name}\n"
                f"{member.mention} joined via {used_invite.code if used_invite else 'unknown'}\n"
                f"Inviter: {inviter.mention}\n"
                f"Total invites: {current_count + 1}",
                guild.id
            )
            
            # Log the invite
            await log_invite(guild.id, inviter, member)
        else:
            await log_activity(
                "member joined",
                f"{member.mention} joined {guild.name} (inviter could not be determined)",
                guild.id
            )
        
        # Update invite cache
        bot.invite_cache[guild.id] = {inv.code: inv for inv in new_invites}
        
    except Exception as e:
        error_msg = str(e)
        if "'User' object has no attribute 'guild'" in error_msg:
            # Handle cases where inviter is not in the guild
            await log_activity(
                "member joined",
                f"{member.mention} joined but we couldn't track the invite (inviter left?)",
                member.guild.id
            )
        else:
            await log_activity("member join error", f"error tracking invites in {member.guild.name}: {e}", member.guild.id)

@bot.event
async def on_member_remove(member: discord.Member):
    """Handle member leaves."""
    await update_member_count()
    
    try:
        guild = member.guild
        
        # Get the inviter's ID for this member from the database
        inviter_id = await get_invite_history(guild.id, member.id)
        
        if inviter_id:
            # Decrement the inviter's count
            current_count = await get_invite_count(guild.id, inviter_id)
            if current_count > 0:
                await set_invite_count(guild.id, inviter_id, current_count - 1)
                
            # Remove the invite history record
            await remove_invite_history(guild.id, inviter_id, member.id)
            
            await log_activity(
                "member left",
                f"Guild: {guild.name}\n"
                f"{member.mention} left the server\n"
                f"Inviter's count decremented: {inviter_id}",
                guild.id
            )
    except Exception as e:
        await log_activity("member leave error", f"error updating join history in {member.guild.name}: {e}", member.guild.id)

@bot.event
async def on_invite_create(invite: discord.Invite):
    """Handle new invite creation."""
    try:
        guild = invite.guild
        if guild.id not in bot.invite_cache:
            bot.invite_cache[guild.id] = {}
        bot.invite_cache[guild.id][invite.code] = invite
        logger.info(f"New invite created in {guild.name}: {invite.code}")
        await log_activity("invite created", 
                         f"New invite created in {guild.name}: {invite.code} by {invite.inviter}",
                         guild.id)
    except Exception as e:
        await log_activity("invite create error", f"error in {invite.guild.name}: {e}", invite.guild.id)

@bot.event
async def on_invite_delete(invite: discord.Invite):
    """Handle invite deletion."""
    try:
        guild = invite.guild
        if guild.id in bot.invite_cache and invite.code in bot.invite_cache[guild.id]:
            del bot.invite_cache[guild.id][invite.code]
        logger.info(f"Invite deleted in {guild.name}: {invite.code}")
        await log_activity("invite deleted",
                         f"Invite deleted in {guild.name}: {invite.code}",
                         guild.id)
    except Exception as e:
        await log_activity("invite delete error", f"error in {invite.guild.name}: {e}", invite.guild.id)

@bot.event
async def on_guild_join(guild: discord.Guild):
    """Handle bot joining a new guild."""
    await cache_invites(guild)
    await log_activity("bot joined guild", f"Joined {guild.name} (id: {guild.id})", guild.id)
    await update_member_count()

@bot.event
async def on_guild_remove(guild: discord.Guild):
    """Handle bot leaving a guild."""
    for data in [bot.invite_cache, bot.giveaway_participants, bot.active_giveaways, 
                bot.processed_users, bot.active_polls, bot.countdown_tasks,
                bot.channel_cooldowns, bot.message_tracker, bot.verification_configs,
                bot.active_users, bot.ticket_categories, bot.ticket_logs]:
        if guild.id in data:
            del data[guild.id]
    await log_activity("bot left guild", f"Left {guild.name} (id: {guild.id})", guild.id)
    await update_member_count()

# Error handlers
@bot.event
async def on_command_error(ctx, error):
    """Handle command errors."""
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You don't have permission to use this command.")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("❌ I don't have permission to perform this action.")
    else:
        await log_activity("command error", f"Error in {ctx.guild if ctx.guild else 'DM'}: {str(error)}", ctx.guild.id if ctx.guild else None)
        logger.error(f"Ignoring exception in command {ctx.command}: {error}")

@bot.event
async def on_app_command_error(interaction: Interaction, error):
    """Handle slash command errors."""
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
    elif isinstance(error, app_commands.BotMissingPermissions):
        await interaction.response.send_message("❌ I don't have permission to perform this action.", ephemeral=True)
    else:
        await log_activity("slash command error", f"Error in {interaction.guild.name if interaction.guild else 'DM'}: {str(error)}", interaction.guild.id if interaction.guild else None, interaction.user if hasattr(interaction, 'user') else None)
        logger.error(f"Ignoring exception in command {interaction.command.name if interaction.command else 'unknown'}: {error}")

@bot.event
async def on_ready():
    """Handle bot startup."""
    logger.info(f'Logged in as {bot.user} (id: {bot.user.id})')
    await update_member_count()
    
    # Initialize database
    await init_db()
    
    # Load verification configs
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute("SELECT guild_id, channel_id, unverified_role_id, verified_role_id, log_channel_id, title, description FROM verification_configs") as cursor:
            async for row in cursor:
                bot.verification_configs[row[0]] = {
                    'channel_id': row[1],
                    'unverified_role_id': row[2],
                    'verified_role_id': row[3],
                    'log_channel_id': row[4],
                    'title': row[5],
                    'description': row[6]
                }
    
    # Cache invites for all guilds
    for guild in bot.guilds:
        await cache_invites(guild)
    
    # Add persistent views
    bot.add_view(VerificationView())
    bot.add_view(TicketDropdownView())
    bot.add_view(TicketCloseView())
    
    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global commands")
        await log_activity("bot started", f"Logged in as {bot.user} in {len(bot.guilds)} guilds", None)
    except Exception as e:
        logger.error(f"Command sync error: {e}")

async def monitor_slow_mode():
    """Background task to periodically clean up old active users."""
    while True:
        try:
            now = datetime.utcnow()
            
            # Clean up old active users (older than 60 seconds)
            for guild_id, channels in bot.active_users.items():
                for channel_id, users in channels.items():
                    # Remove users who haven't been active in the last 60 seconds
                    bot.active_users[guild_id][channel_id] = {
                        user_id: last_activity 
                        for user_id, last_activity in users.items() 
                        if (now - last_activity).total_seconds() <= 60
                    }
            
        except Exception as e:
            logger.error(f"Error in active users cleanup: {e}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

# Start the bot
if __name__ == "__main__":
    TOKEN = os.getenv('DISCORD_TOKEN')