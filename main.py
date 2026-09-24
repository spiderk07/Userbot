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
    
    async def self_ping_loop(self):
        """Render ka Free Web Service ~15 min inactivity ke baad sleep ho
        jaata hai. External pinger (UptimeRobot) kaafi na ho ya miss ho
        jaaye, isliye process khud apna public URL periodically hit
        karta hai — Render ke liye ye ek genuine inbound HTTP request
        hai, isliye sleep-timer reset ho jaata hai. RENDER_EXTERNAL_URL
        Render khud inject karta hai; koi extra config nahi chahiye."""
        url = os.environ.get('RENDER_EXTERNAL_URL') or os.environ.get('SELF_PING_URL')
        if not url:
            logger.warning("Self-ping: URL nahi mila (RENDER_EXTERNAL_URL/SELF_PING_URL) — disabled")
            return
        interval = int(os.environ.get('SELF_PING_INTERVAL_SECONDS', '240'))
        import urllib.request
        while self.is_running:
            try:
                await asyncio.to_thread(urllib.request.urlopen, url, None, 10)
            except Exception as e:
                logger.warning(f"Self-ping failed: {e}")
            await asyncio.sleep(interval)

    async def retry_failed_loop(self):
        """Periodically 'failed' items ko wapas 'queued' karta hai — is
        loop ke bina purana backlog jo ek baar (RETRY_LIMIT attempts ke
        baad) fail ho gaya, hamesha ke liye atka reh jaata, jabki nayi
        live files fresh 'queued' status ke saath turant copy ho jaati."""
        while self.is_running:
            await asyncio.sleep(Config.RETRY_FAILED_INTERVAL_SECONDS)
            try:
                await asyncio.to_thread(
                    self.db.retry_failed_items,
                    Config.MAX_QUEUE_RETRIES,
                    Config.RETRY_FAILED_INTERVAL_SECONDS
                )
            except Exception as e:
                logger.warning(f"Retry-failed loop error: {e}")

    async def recover_stuck_loop(self):
        """recover_stuck_processing() pehle SIRF startup par ek baar
        chalta tha. Agar beech session me koi worker crash/hang ho jaaye
        copy karte waqt (bina 'completed'/'failed' set kiye), wo item
        'processing' me hamesha ke liye atka reh jaata — claim_batch()
        sirf 'queued' dekhta hai, isliye wo kabhi dobara try hi nahi
        hota. Pehle jab bot baar-baar crash-restart hota tha, har
        restart par ye accidentally fix ho jaata; ab restart-loop fix
        hone ke baad iska periodic hona zaroori hai.

        SAFETY: startup wale short (60s) threshold ki jagah yahan LAMBA
        (RECOVER_STUCK_PERIODIC_SECONDS, default 15 min) threshold use
        karte hain — warna koi bada video jo transfer hone me thoda time
        le raha ho, use galti se "stuck" maan ke doosra worker dobara
        claim kar lega (duplicate copy)."""
        while self.is_running:
            await asyncio.sleep(300)
            try:
                await asyncio.to_thread(
                    self.db.recover_stuck_processing, Config.RECOVER_STUCK_PERIODIC_SECONDS
                )
            except Exception as e:
                logger.warning(f"Recover-stuck loop error: {e}")

    async def start(self):
        logger.info("Starting Telegram Copier...")

        # Restart/crash ke baad atki hui 'processing' files ko queued me wapas laao
        await asyncio.to_thread(self.db.recover_stuck_processing, Config.RECOVER_STUCK_AFTER_SECONDS)
        
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
        
        # Har background task ka apna factory rakha hai taaki agar koi
        # EK task crash/khatam ho jaaye (SIGTERM ki wajah se nahi), to
        # SIRF usi ko restart kiya ja sake — poora bot band karke Render
        # se restart karwana zaroori nahi. Isse pehle asyncio.wait() ek
        # bhi task khatam hote hi turant return kar deta tha, aur agar
        # wo stop_event nahi tha, code seedha graceful_shutdown() tak
        # pahunch jaata — matlab kisi ek chhoti si crash (jaise ek hung
        # Telegram call se scan_history() me exception) se POORA bot
        # band ho jaata aur Render use restart karta — baar-baar restart
        # hone ki yahi sabse badi wajah thi.
        task_factories = {
            'scan_history': self.scanner.scan_history,
            'listen_new_messages': self.scanner.listen_new_messages,
            'self_ping_loop': self.self_ping_loop,
            'retry_failed_loop': self.retry_failed_loop,
            'recover_stuck_loop': self.recover_stuck_loop,
        }
        running = {
            name: asyncio.create_task(factory(), name=name)
            for name, factory in task_factories.items()
        }
        
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
            while True:
                done, pending = await asyncio.wait(
                    [*running.values(), stop_waiter],
                    return_when=asyncio.FIRST_COMPLETED
                )
                if self.stop_event.is_set():
                    logger.info("Shutdown signal received — stopping tasks...")
                    for t in running.values():
                        t.cancel()
                    await asyncio.gather(*running.values(), return_exceptions=True)
                    break
                
                # stop_event nahi tha — matlab koi task khud khatam/crash
                # ho gaya. Poora bot band karne ke bajaye sirf usi task ko
                # naye sirey se restart karo.
                for name, t in list(running.items()):
                    if not t.done():
                        continue
                    if t.cancelled():
                        continue
                    exc = t.exception()
                    if exc:
                        logger.error(f"Background task '{name}' crashed: {exc!r} — restarting it")
                    else:
                        logger.warning(f"Background task '{name}' ended unexpectedly — restarting it")
                    await asyncio.sleep(2)  # tight crash-loop se bachne ke liye chhota backoff
                    running[name] = asyncio.create_task(task_factories[name](), name=name)
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
        await asyncio.to_thread(self.db.close)

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
