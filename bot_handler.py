from pyrogram import Client, filters
from config import Config
from database import Database
from queue_manager import QueueManager
import logging
from datetime import datetime, timedelta

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
                "/progress - Scan index & ETA\n"
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

            # Diagnostics — Render ka log-window seemit ho sakta hai, isliye
            # ye hamesha live MongoDB se aata hai, chahe FloodWait/stop
            # kitni bhi der pehle hua ho.
            diag = self.db.get_diagnostics()
            if diag:
                stats_text += "\n**Diagnostics:**\n"
                fw_count = diag.get('floodwait_count', 0)
                if fw_count:
                    last_at = diag.get('last_floodwait_at')
                    last_at_str = last_at.strftime('%Y-%m-%d %H:%M:%S') if last_at else 'unknown'
                    stats_text += (
                        f"⚠️ FloodWait total: `{fw_count}` | last: "
                        f"`{diag.get('last_floodwait_seconds')}s` on "
                        f"`{diag.get('last_floodwait_account')}` at `{last_at_str}`\n"
                    )
                else:
                    stats_text += "✅ Koi FloodWait record nahi hai\n"

                workers_diag = diag.get('workers', {})
                if workers_diag:
                    for wid, w in workers_diag.items():
                        icon = "🛑" if w.get('status') == 'stopped' else "✅"
                        stats_text += f"{icon} Worker {wid} ({w.get('account')}): `{w.get('status')}`"
                        if w.get('status') == 'stopped':
                            stats_text += f" — {w.get('reason')}"
                        stats_text += "\n"
            
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
        
        @self.client.on_message(filters.command("progress"))
        async def progress_command(client, message):
            if not self.is_admin(message.from_user.id):
                await message.reply_text("❌ Unauthorized")
                return

            progresses = self.db.get_all_progress()
            if not progresses:
                await message.reply_text("📇 No scan progress recorded yet.")
                return

            now = datetime.now()
            text = "📇 **Index Progress**\n\n"

            for p in progresses:
                source = p.get('source_chat_id')
                total = p.get('total_messages')
                last_id = p.get('last_message_id', 0) or 0
                scanned_count = p.get('scanned_count', 0)
                started = p.get('scan_started_at')

                elapsed = (now - started).total_seconds() if started else 0
                rate = (scanned_count / elapsed) if elapsed > 0 else 0

                # last_id ghatta ja raha hai jaise-jaise purani history scan
                # hoti hai (offset newest->oldest), isliye jitna last_id
                # utna hi "baaki" hai scan karne ko.
                remaining = last_id
                if total:
                    done_pct = round(100 * max(total - last_id, 0) / total, 1)
                    done_pct_text = f"`{done_pct}%`"
                else:
                    done_pct_text = "`unknown`"

                if rate > 0 and remaining > 0:
                    eta_str = str(timedelta(seconds=int(remaining / rate)))
                elif remaining == 0 and total:
                    eta_str = "✅ done"
                else:
                    eta_str = "calculating..."

                type_counts = p.get('type_counts', {}) or {}
                types_line = ""
                if type_counts:
                    top_types = sorted(type_counts.items(), key=lambda kv: kv[1], reverse=True)
                    types_line = "  Types found: " + ", ".join(
                        f"{t}=`{c}`" for t, c in top_types
                    ) + "\n"

                text += (
                    f"Channel `{source}`:\n"
                    f"  Total (approx): `{total if total else 'unknown'}`\n"
                    f"  Scanned so far: `{scanned_count}` msgs\n"
                    f"  Progress: {done_pct_text}\n"
                    f"  Speed: `{round(rate, 2)}` msg/s\n"
                    f"  ETA: `{eta_str}`\n"
                    f"{types_line}\n"
                )

            await message.reply_text(text)

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
                "/progress - Scan index & ETA per channel\n"
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
