from pyrogram import Client, filters
from config import Config
from database import Database
from queue_manager import QueueManager
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BotHandler:
    """Bot Command Handler for Monitoring"""
    
    def __init__(self, db: Database, queue_manager: QueueManager):
        self.db = db
        self.queue_manager = queue_manager
        self.client = None
        self.is_running = False
    
    async def start(self):
        """Start bot client"""
        if not Config.BOT_TOKEN:
            logger.info("No bot token provided, skipping bot commands")
            return False
        
        try:
            self.client = Client(
                name="monitor_bot",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=Config.BOT_TOKEN,
                in_memory=True
            )
            
            self._register_handlers()
            
            await self.client.start()
            self.is_running = True
            bot_me = await self.client.get_me()
            logger.info(f"✅ Monitor bot started: @{bot_me.username}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Bot setup error: {e}")
            return False
    
    def _register_handlers(self):
        """Register all bot command handlers"""
        
        @self.client.on_message(filters.command("start"))
        async def start_command(client, message):
            await message.reply_text(
                "🤖 **Telegram Copier Bot**\n\n"
                "Main monitoring bot for your copier system.\n\n"
                "Commands:\n"
                "/stats - Processing statistics\n"
                "/status - System status\n"
                "/channels - List channels\n"
                "/pause - Pause processing\n"
                "/resume - Resume processing\n"
                "/help - Show help"
            )
        
        @self.client.on_message(filters.command("stats"))
        async def stats_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return
            
            stats = self.db.get_stats()
            
            stats_text = (
                f"📊 **Processing Stats**\n\n"
                f"📁 Total Files: `{stats['total']}`\n"
                f"✅ Completed: `{stats['completed']}`\n"
                f"⏳ Pending: `{stats['pending']}`\n"
                f"🔄 Processing: `{stats['processing']}`\n"
                f"❌ Failed: `{stats['failed']}`\n"
                f"📦 Queue Size: `{self.queue_manager.qsize()}`\n\n"
            )
            
            if stats['channels']:
                stats_text += "**Per Channel Stats:**\n"
                for ch in stats['channels']:
                    stats_text += (
                        f"Channel `{ch['source_chat_id']}`:\n"
                        f"  Total: {ch['total']} | Completed: {ch['completed']} | Pending: {ch['pending']}\n"
                    )
            
            await message.reply_text(stats_text)
        
        @self.client.on_message(filters.command("status"))
        async def status_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return
            
            status_text = "✅ Running" if self.is_running else "⏸️ Paused"
            
            await message.reply_text(
                f"🔧 **System Status**\n\n"
                f"Status: `{status_text}`\n"
                f"Bot Active: `{self.client is not None}`\n"
                f"Queue Size: `{self.queue_manager.qsize()}`\n"
                f"Processing: `{self.queue_manager.processing_count()}`\n"
                f"Time: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
            )
        
        @self.client.on_message(filters.command("channels"))
        async def channels_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return
            
            channels_info = "📡 **Configured Channels**\n\n"
            for i, (source, dest) in enumerate(zip(Config.SOURCE_CHANNELS, Config.DESTINATION_CHANNELS), 1):
                channels_info += (
                    f"**Pair {i}:**\n"
                    f"📥 Source: `{source}`\n"
                    f"📤 Destination: `{dest}`\n\n"
                )
            
            await message.reply_text(channels_info)
        
        @self.client.on_message(filters.command("pause"))
        async def pause_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return
            
            self.is_running = False
            await message.reply_text("⏸️ Processing paused")
        
        @self.client.on_message(filters.command("resume"))
        async def resume_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return
            
            self.is_running = True
            await message.reply_text("▶️ Processing resumed")
        
        @self.client.on_message(filters.command("help"))
        async def help_command(client, message):
            help_text = (
                "📚 **Available Commands**\n\n"
                "/start - Start bot\n"
                "/stats - View processing statistics\n"
                "/status - Check system status\n"
                "/channels - List configured channels\n"
                "/pause - Pause processing\n"
                "/resume - Resume processing\n"
                "/help - Show this help"
            )
            await message.reply_text(help_text)
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        if not Config.ADMIN_USER_IDS:
            return True  # If no admins specified, allow all
        return user_id in Config.ADMIN_USER_IDS
    
    async def stop(self):
        """Stop bot client"""
        if self.client:
            await self.client.stop()
            self.is_running = False
