import asyncio
from pyrogram import Client
from config import Config
from copier import Copier
from queue_manager import QueueManager
import random
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class Worker:
    """Worker class for processing files"""
    
    def __init__(self, session_string: str, worker_id: int, is_bot: bool = False, 
                 bot_token: str = None):
        self.session_string = session_string
        self.worker_id = worker_id
        self.is_bot = is_bot
        self.bot_token = bot_token
        self.client = None
        self.copier = Copier()
        self.queue_manager = QueueManager()
        self.is_running = False
        self.error_count = 0
        self.max_errors = 10
        self.last_error_time = datetime.now()
        self.backoff_time = 60  # seconds
        self.worker_type = "Bot" if is_bot else "Userbot"
        self.processed_count = 0
        self.failed_count = 0
    
    async def start(self) -> bool:
        """Initialize worker client"""
        try:
            if self.is_bot:
                self.client = Client(
                    name=f"bot_worker_{self.worker_id}",
                    api_id=Config.API_ID,
                    api_hash=Config.API_HASH,
                    bot_token=self.bot_token,
                    in_memory=True,
                    workers=10,
                    sleep_threshold=30
                )
            else:
                self.client = Client(
                    name=f"userbot_{self.worker_id}",
                    api_id=Config.API_ID,
                    api_hash=Config.API_HASH,
                    session_string=self.session_string,
                    in_memory=True,
                    workers=10,
                    sleep_threshold=30
                )
            
            await self.client.start()
            self.is_running = True
            
            # Get client info
            me = await self.client.get_me()
            if self.is_bot:
                logger.info(f"🤖 Bot Worker {self.worker_id} started: @{me.username}")
            else:
                logger.info(f"👤 Userbot Worker {self.worker_id} started: {me.first_name}")
            
            return True
        except Exception as e:
            logger.error(f"❌ {self.worker_type} Worker {self.worker_id} failed to start: {e}")
            return False
    
    async def stop(self):
        """Stop worker client"""
        if self.client:
            try:
                await self.client.stop()
            except:
                pass
        self.is_running = False
        logger.info(f"{self.worker_type} Worker {self.worker_id} stopped")
    
    async def check_rate_limit(self):
        """Check if we should backoff due to rate limiting"""
        if self.error_count >= self.max_errors:
            time_since_last_error = datetime.now() - self.last_error_time
            if time_since_last_error < timedelta(seconds=self.backoff_time):
                wait_time = self.backoff_time - time_since_last_error.total_seconds()
                logger.warning(f"⏳ {self.worker_type} Worker {self.worker_id}: Backing off for {wait_time:.0f}s")
                await asyncio.sleep(wait_time)
                self.error_count = 0
                self.last_error_time = datetime.now()
    
    async def process_queue(self):
        """Process files from queue"""
        while self.is_running:
            try:
                await self.check_rate_limit()
                
                file_info = await self.queue_manager.get_file()
                
                if file_info is None:
                    # No files to process, wait a bit
                    await asyncio.sleep(random.uniform(5, 10))
                    continue
                
                # Random delay to avoid hitting rate limits
                await asyncio.sleep(random.uniform(1, 3))
                
                # Copy message
                destination_message_id = await self.copier.copy_message(
                    self.client,
                    file_info
                )
                
                if destination_message_id:
                    file_info['destination_message_id'] = destination_message_id
                    file_info['account'] = f"{self.worker_type}_{self.worker_id}"
                    await self.queue_manager.mark_completed(file_info)
                    self.processed_count += 1
                    self.error_count = 0  # Reset error count on success
                    logger.info(f"✅ {self.worker_type} Worker {self.worker_id}: Copied message {file_info['source_message_id']}")
                else:
                    await self.queue_manager.mark_failed(file_info, "Copy failed")
                    self.failed_count += 1
                    self.error_count += 1
                    self.last_error_time = datetime.now()
                    logger.error(f"❌ {self.worker_type} Worker {self.worker_id}: Failed to copy message {file_info['source_message_id']}")
                
                # Log progress every 10 files
                if (self.processed_count + self.failed_count) % 10 == 0:
                    logger.info(f"📊 Worker {self.worker_id} Stats - Processed: {self.processed_count}, Failed: {self.failed_count}")
                
            except Exception as e:
                logger.error(f"❌ {self.worker_type} Worker {self.worker_id} error: {e}")
                self.error_count += 1
                self.last_error_time = datetime.now()
                await asyncio.sleep(5)
    
    async def run(self):
        """Run worker"""
        if await self.start():
            await self.process_queue()

class WorkerPool:
    """Worker Pool Manager"""
    
    def __init__(self):
        self.workers = []
        self.sessions = Config.get_sessions()
    
    async def start_all(self):
        """Start all workers"""
        worker_id = 0
        
        # Bot worker (if token available)
        if Config.BOT_TOKEN:
            worker_id += 1
            bot_worker = Worker(
                session_string=None,
                worker_id=worker_id,
                is_bot=True,
                bot_token=Config.BOT_TOKEN
            )
            self.workers.append(bot_worker)
            asyncio.create_task(bot_worker.run())
            await asyncio.sleep(2)
            logger.info(f"🤖 Bot worker added as Worker {worker_id}")
        
        # Userbot workers
        for session in self.sessions:
            if session:
                worker_id += 1
                userbot_worker = Worker(
                    session_string=session,
                    worker_id=worker_id,
                    is_bot=False
                )
                self.workers.append(userbot_worker)
                asyncio.create_task(userbot_worker.run())
                await asyncio.sleep(2)
                logger.info(f"👤 Userbot worker added as Worker {worker_id}")
        
        logger.info(f"✅ Total workers started: {len(self.workers)}")
    
    async def stop_all(self):
        """Stop all workers"""
        for worker in self.workers:
            await worker.stop()
        logger.info("✅ All workers stopped")
