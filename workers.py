import asyncio
from pyrogram import Client
import logging
import random
from config import Config
from copier import Copier
from queue_manager import QueueManager

logger = logging.getLogger(__name__)

class Worker:
    """Ab apna alag Pyrogram Client nahi banata — TelegramCopier ka pehle se
    running 'scanner' client hi reuse karta hai. Pehle har account do
    connections maintain karta tha (scanner_i + worker_i), matlab N accounts
    ke liye 2N concurrent MTProto sessions — chhote (Free/Nano) instance par
    ye resource-heavy tha aur timeouts/reconnects ki wajah ban raha tha. Ek
    hi Client object dono kaam (scan/listen + copy) parallel handle kar sakta
    hai (Client(workers=10) isi liye hai), isliye ab connections seedhi aadhi
    ho jaati hain."""
    def __init__(self, client, worker_id, account_name=None):
        self.client = client
        self.worker_id = worker_id
        self.account_name = account_name or f"worker_{worker_id}"
        self.copier = Copier()
        self.queue_manager = QueueManager()
        self.is_running = True
        self.processed = 0
        self.failed = 0
    
    async def stop(self):
        # Client ka lifecycle ab TelegramCopier ke paas hai (wahi ise start/
        # stop karta hai) — yahan sirf apna processing loop rokna hai.
        self.is_running = False
    
    async def process_queue(self):
        while self.is_running:
            try:
                file_info = await self.queue_manager.get_file()
                
                if file_info is None:
                    await asyncio.sleep(5)
                    continue
                
                # Chhota sa stagger — bilkul zero delay se Telegram FloodWait
                # trigger hone ka risk badh jaata hai. Ab env vars
                # (WORKER_DELAY_MIN/MAX) se tune-able hai — code badle bina
                # Render me value change karke redeploy karo. FloodWait
                # errors dikhein to badha do, na dikhein to ghata ke test
                # karo.
                await asyncio.sleep(random.uniform(Config.WORKER_DELAY_MIN, Config.WORKER_DELAY_MAX))
                
                destination_message_id = await self.copier.copy_message(
                    self.client,
                    file_info
                )
                
                if destination_message_id:
                    file_info['destination_message_id'] = destination_message_id
                    file_info['account'] = self.account_name
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
        await self.process_queue()

class WorkerPool:
    def __init__(self):
        self.workers = []
    
    async def start_all(self, clients):
        """clients: TelegramCopier ke already-started scanner Client objects
        ki list — same connections copy karne ke liye bhi use hoti hain,
        naye separate connections nahi banate."""
        for i, client in enumerate(clients, 1):
            try:
                me = await client.get_me()
                account_name = me.first_name
            except Exception:
                account_name = f"worker_{i}"
            worker = Worker(client, i, account_name)
            self.workers.append(worker)
            asyncio.create_task(worker.run())
        
        logger.info(f"Total workers: {len(self.workers)} (sharing {len(clients)} scanner connections)")
    
    async def stop_all(self):
        for worker in self.workers:
            await worker.stop()
