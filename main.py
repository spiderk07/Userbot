import asyncio
import logging
import signal
import sys
import os
import threading
from flask import Flask, jsonify
from datetime import datetime

from config import Config
from database import Database
from queue_manager import QueueManager
from workers import WorkerPool
from scanner import Scanner
from bot_handler import BotHandler

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('copier.log')
    ]
)
logger = logging.getLogger(__name__)

# Flask app for health checks
app = Flask(__name__)
db = Database()
queue_manager = QueueManager()

@app.route('/')
def home():
    return jsonify({
        'status': 'running',
        'service': 'Telegram Copier',
        'version': '1.0.0',
        'sessions': len(Config.get_sessions()),
        'workers': len(Config.get_sessions()) + (1 if Config.BOT_TOKEN else 0)
    })

@app.route('/health')
def health():
    stats = db.get_stats()
    return jsonify({
        'status': 'healthy',
        'stats': stats,
        'queue_size': queue_manager.qsize(),
        'processing': queue_manager.processing_count(),
        'sessions': len(Config.get_sessions()),
        'timestamp': datetime.now().isoformat()
    })

class TelegramCopier:
    """Main Application Class"""
    
    def __init__(self):
        self.db = Database()
        self.queue_manager = QueueManager()
        self.worker_pool = WorkerPool()
        self.bot_handler = BotHandler(self.db, self.queue_manager)
        self.scanner = None
        self.clients = []
        self.is_running = True
        self.tasks = []
    
    def display_config(self):
        """Display current configuration"""
        logger.info("="*60)
        logger.info("📋 CURRENT CONFIGURATION")
        logger.info("="*60)
        logger.info(f"🔑 API ID: {Config.API_ID}")
        logger.info(f"🤖 Bot Token: {'✅ Set' if Config.BOT_TOKEN else '❌ Not set'}")
        
        sessions = Config.get_sessions()
        logger.info(f"👥 Available Sessions: {len(sessions)}")
        for i, session in enumerate(sessions, 1):
            # Show only first 10 chars for security
            session_preview = session[:10] + "..." if len(session) > 10 else "***"
            logger.info(f"   Session {i}: {session_preview}")
        
        logger.info(f"📥 Source Channels: {len(Config.SOURCE_CHANNELS)}")
        for ch in Config.SOURCE_CHANNELS:
            logger.info(f"   - {ch}")
        
        logger.info(f"📤 Destination Channels: {len(Config.DESTINATION_CHANNELS)}")
        for ch in Config.DESTINATION_CHANNELS:
            logger.info(f"   - {ch}")
        
        logger.info(f"🗄️ Database: {Config.DB_NAME}")
        logger.info(f"⚙️ Batch Size: {Config.BATCH_SIZE}")
        logger.info(f"📝 Copy Mode: {Config.COPY_MODE}")
        logger.info("="*60)
    
    async def initialize_clients(self):
        """Initialize all client instances"""
        logger.info("\n🔧 Initializing clients...")
        
        # Start bot for monitoring
        await self.bot_handler.start()
        
        # Create userbot clients for scanning
        sessions = Config.get_sessions()
        for i, session in enumerate(sessions, 1):
            try:
                from pyrogram import Client
                client = Client(
                    name=f"scanner_{i}",
                    api_id=Config.API_ID,
                    api_hash=Config.API_HASH,
                    session_string=session,
                    in_memory=True,
                    workers=10
                )
                await client.start()
                me = await client.get_me()
                logger.info(f"✅ Scanner {i} started: {me.first_name}")
                self.clients.append(client)
            except Exception as e:
                logger.error(f"❌ Scanner {i} failed: {e}")
        
        if not self.clients:
            logger.error("❌ No scanner clients available!")
            return False
        
        logger.info(f"✅ Total scanner clients: {len(self.clients)}")
        
        # Initialize scanner
        self.scanner = Scanner(self.clients, self.db, self.queue_manager)
        return True
    
    async def verify_access(self):
        """Verify access to all channels"""
        logger.info("\n📋 Verifying channel access...")
        
        for source_str, dest_str in zip(Config.SOURCE_CHANNELS, Config.DESTINATION_CHANNELS):
            source_chat_id = int(source_str.strip())
            destination_chat_id = int(dest_str.strip())
            
            # Check source access
            source_accessible = False
            for client in self.clients:
                try:
                    chat = await client.get_chat(source_chat_id)
                    logger.info(f"✅ Source '{chat.title}' accessible")
                    source_accessible = True
                    break
                except:
                    continue
            
            if not source_accessible:
                logger.warning(f"⚠️ No userbot can access source: {source_chat_id}")
            
            # Check destination access
            dest_accessible = False
            for client in self.clients:
                try:
                    chat = await client.get_chat(destination_chat_id)
                    logger.info(f"✅ Destination '{chat.title}' accessible")
                    dest_accessible = True
                    break
                except:
                    continue
            
            if not dest_accessible:
                logger.warning(f"⚠️ No userbot can access destination: {destination_chat_id}")
    
    async def start(self):
        """Start the application"""
        logger.info("\n🚀 Starting Telegram Channel Copier...")
        
        # Display configuration
        self.display_config()
        
        # Validate configuration
        errors = Config.validate()
        if errors:
            for error in errors:
                logger.error(f"❌ Configuration error: {error}")
            return False
        
        # Initialize clients
        if not await self.initialize_clients():
            return False
        
        # Verify channel access
        await self.verify_access()
        
        # Start workers
        await self.worker_pool.start_all()
        logger.info("✅ Workers started")
        
        # Start tasks
        self.tasks = [
            asyncio.create_task(self.scanner.scan_history()),
            asyncio.create_task(self.scanner.listen_new_messages()),
            asyncio.create_task(self.show_stats())
        ]
        
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self.graceful_shutdown(s))
                )
            except NotImplementedError:
                pass
        
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ Error in main loop: {e}")
        finally:
            await self.graceful_shutdown()
        
        return True
    
    async def show_stats(self):
        """Show periodic statistics"""
        while self.is_running:
            await asyncio.sleep(300)
            stats = self.db.get_stats()
            logger.info("\n" + "="*50)
            logger.info("📊 PROCESSING STATS")
            logger.info("="*50)
            logger.info(f"📁 Total Files: {stats['total']}")
            logger.info(f"✅ Completed: {stats['completed']}")
            logger.info(f"⏳ Pending: {stats['pending']}")
            logger.info(f"🔄 Processing: {stats['processing']}")
            logger.info(f"❌ Failed: {stats['failed']}")
            logger.info(f"📦 Queue Size: {self.queue_manager.qsize()}")
            logger.info(f"👥 Active Sessions: {len(self.clients)}")
            logger.info("="*50 + "\n")
    
    async def graceful_shutdown(self, signal=None):
        """Graceful shutdown"""
        if not self.is_running:
            return
        
        logger.info("\n🛑 Shutting down gracefully...")
        self.is_running = False
        
        if self.scanner:
            self.scanner.stop()
        
        await self.worker_pool.stop_all()
        await self.bot_handler.stop()
        
        for client in self.clients:
            try:
                await client.stop()
            except:
                pass
        
        stats = self.db.get_stats()
        logger.info("\n📊 Final Stats:")
        logger.info(f"Total Files: {stats['total']}")
        logger.info(f"Completed: {stats['completed']}")
        logger.info(f"Pending: {stats['pending']}")
        logger.info(f"Failed: {stats['failed']}")
        
        self.db.close()
        logger.info("✅ Shutdown complete")
        sys.exit(0)

def run_flask():
    """Run Flask app for health checks"""
    port = Config.PORT
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def main():
    """Main entry point"""
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    copier = TelegramCopier()
    
    try:
        asyncio.run(copier.start())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
