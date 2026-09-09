import asyncio
import logging
import sys
import os
import threading
from flask import Flask, jsonify

from config import Config
from database import Database
from queue_manager import QueueManager
from workers import WorkerPool
from scanner import Scanner
from bot_handler import BotHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

try:
    db = Database()
    queue_manager = QueueManager()
except Exception as e:
    logger.error(f"DB init failed: {e}")
    db = None
    queue_manager = None

@app.route('/')
def home():
    return jsonify({'status': 'running'})

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

@app.route('/stats')
def stats():
    if not db:
        return jsonify({'error': 'database not connected'}), 500
    return jsonify(db.get_stats())

class TelegramCopier:
    def __init__(self):
        self.db = Database()
        self.queue_manager = QueueManager()
        self.worker_pool = WorkerPool()
        self.scanner = None
        self.bot_handler = None
        self.clients = []
        self.is_running = True
    
    async def initialize_clients(self):
        logger.info("Starting clients...")
        
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
                logger.info(f"Scanner {i}: {me.first_name}")

                # Peer cache ko warm-up karo taaki raw chat IDs (jaise -100...) resolve ho sakein.
                # In-memory session me ye cache khali hota hai jab tak dialogs fetch na ho.
                dialog_count = 0
                async for _ in client.get_dialogs():
                    dialog_count += 1
                logger.info(f"Scanner {i}: cached {dialog_count} dialogs")

                self.clients.append(client)
            except Exception as e:
                logger.error(f"Scanner {i} failed: {e}")
        
        if not self.clients:
            logger.error("No clients!")
            return False
        
        self.scanner = Scanner(self.clients, self.db, self.queue_manager)
        return True
    
    async def start(self):
        logger.info("Starting Telegram Copier...")
        
        if not await self.initialize_clients():
            return False
        
        await self.worker_pool.start_all()

        # Monitor bot start karo (agar BOT_TOKEN set hai) — /stats, /status jaise commands ke liye
        self.bot_handler = BotHandler(self.db, self.queue_manager)
        await self.bot_handler.start()
        
        tasks = [
            asyncio.create_task(self.scanner.scan_history()),
            asyncio.create_task(self.scanner.listen_new_messages())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            await self.graceful_shutdown()
    
    async def graceful_shutdown(self):
        self.is_running = False
        if self.scanner:
            self.scanner.stop()
        if self.bot_handler:
            await self.bot_handler.stop()
        await self.worker_pool.stop_all()
        for client in self.clients:
            try:
                await client.stop()
            except:
                pass
        self.db.close()
        sys.exit(0)

def run_flask():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    copier = TelegramCopier()
    asyncio.run(copier.start())

if __name__ == "__main__":
    main()
