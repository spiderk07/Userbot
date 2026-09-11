import asyncio
from pyrogram import Client
import logging
import random
from config import Config
from copier import Copier
from queue_manager import QueueManager

logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, session_string, worker_id):
        self.session_string = session_string
        self.worker_id = worker_id
        self.client = None
        self.copier = Copier()
        self.queue_manager = QueueManager()
        self.is_running = False
        self.processed = 0
        self.failed = 0
    
    async def start(self):
        try:
            self.client = Client(
                name=f"worker_{self.worker_id}",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                session_string=self.session_string,
                in_memory=True,
                workers=10
            )
            await self.client.start()
            self.is_running = True
            me = await self.client.get_me()
            logger.info(f"Worker {self.worker_id}: {me.first_name}")
            return True
        except Exception as e:
            logger.error(f"Worker {self.worker_id} failed: {e}")
            return False
    
    async def stop(self):
        if self.client:
            try:
                await self.client.stop()
            except:
                pass
        self.is_running = False
    
    async def process_queue(self):
        while self.is_running:
            try:
                file_info = await self.queue_manager.get_file()
                
                if file_info is None:
                    await asyncio.sleep(5)
                    continue
                
                # Chhota sa stagger — bilkul zero delay se Telegram FloodWait
                # trigger hone ka risk badh jaata hai, lekin pehle wala 1-2s
                # bahut zyada conservative tha or copy speed 3-4x tak slow
                # kar raha tha. Agar FloodWait errors log me dikhein, ise
                # thoda badha dena (jaise 0.6-1.2).
                await asyncio.sleep(random.uniform(0.3, 0.6))
                
                destination_message_id = await self.copier.copy_message(
                    self.client,
                    file_info
                )
                
                if destination_message_id:
                    file_info['destination_message_id'] = destination_message_id
                    file_info['account'] = f"worker_{self.worker_id}"
                    await self.queue_manager.mark_completed(file_info)
                    self.processed += 1
                    logger.info(f"Worker {self.worker_id}: Copied {file_info['source_message_id']}")
                else:
                    await self.queue_manager.mark_failed(file_info, "Copy failed")
                    self.failed += 1
                    logger.error(f"Worker {self.worker_id}: Failed {file_info['source_message_id']}")
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id}: {e}")
                await asyncio.sleep(5)
    
    async def run(self):
        if await self.start():
            await self.process_queue()

class WorkerPool:
    def __init__(self):
        self.workers = []
        self.sessions = Config.get_sessions()
    
    async def start_all(self):
        for i, session in enumerate(self.sessions, 1):
            worker = Worker(session, i)
            self.workers.append(worker)
            asyncio.create_task(worker.run())
            await asyncio.sleep(2)
        
        logger.info(f"Total workers: {len(self.workers)}")
    
    async def stop_all(self):
        for worker in self.workers:
            await worker.stop()
