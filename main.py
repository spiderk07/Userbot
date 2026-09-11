import asyncio
import logging
import sys
import os
import signal
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
        self.stop_event = asyncio.Event()
    
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

        # Restart/crash ke baad atki hui 'processing' files ko queued me wapas laao
        self.db.recover_stuck_processing()
        
        if not await self.initialize_clients():
            return False
        
        # Worker pool ab TelegramCopier ke already-connected clients hi reuse
        # karta hai copy karne ke liye — alag se apni khud ki N connections
        # nahi banata. Isse total concurrent Telegram sessions aadhi ho jaati
        # hain (pehle N scanner + N worker = 2N; ab sirf N).
        await self.worker_pool.start_all(self.clients)

        # Monitor bot start karo (agar BOT_TOKEN set hai) — /stats, /status jaise commands ke liye
        self.bot_handler = BotHandler(self.db, self.queue_manager)
        await self.bot_handler.start()
        
        tasks = [
            asyncio.create_task(self.scanner.scan_history()),
            asyncio.create_task(self.scanner.listen_new_messages())
        ]
        
        # Koyeb (aur zyadatar platforms) deploy karte waqt PURANI instance ko
        # SIGTERM bhejta hai aur naya instance start karta hai — dono kuch der
        # (blue-green) overlap karte hain. Agar purana process SIGTERM ignore
        # karke chalta rehta hai, to same Telegram session do jagah se connect
        # rehti hai aur session-conflict/reconnect churn hota hai. stop_event
        # ke wait ko bhi race karte hain taaki SIGTERM milte hi turant saaf
        # shutdown ho — Telegram sessions properly disconnect ho jaayein,
        # aur nayi instance ko koi leftover-connection conflict na mile.
        stop_waiter = asyncio.create_task(self.stop_event.wait())
        
        try:
            done, pending = await asyncio.wait(
                [*tasks, stop_waiter],
                return_when=asyncio.FIRST_COMPLETED
            )
            if self.stop_event.is_set():
                logger.info("Shutdown signal received — stopping tasks...")
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            await self.graceful_shutdown()
    
    def request_shutdown(self):
        """SIGTERM/SIGINT handler se call hota hai — turant graceful
        shutdown trigger karta hai instead of process ko abruptly kill
        hone dene ke (jisse Telegram sessions politely disconnect nahi
        ho paate)."""
        self.stop_event.set()
    
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

def run_flask():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    copier = TelegramCopier()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Koyeb/Render/Docker jab is process ko rokna chahte hain (deploy,
    # restart, scale-down) to SIGTERM bhejte hain. Isse handle na karne par
    # process turant kill ho jaata hai aur Telegram sessions ko disconnect
    # hone ka mauka nahi milta — jisse deploy ke baad thodi der session-
    # conflict/reconnect churn dikhta hai. Yahan hook karke turant clean
    # shutdown karte hain.
    def _on_signal(signum, frame=None):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        loop.call_soon_threadsafe(copier.request_shutdown)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig)
        except NotImplementedError:
            # Kuch platforms (jaise Windows) add_signal_handler support nahi
            # karte — fallback thread-unsafe signal.signal() par.
            signal.signal(sig, _on_signal)

    try:
        loop.run_until_complete(copier.start())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
