import asyncio
import logging
from database import Database

logger = logging.getLogger(__name__)

class QueueManager:
    """Singleton: Scanner and every Worker must share the SAME in-memory
    queue, otherwise workers each poll Mongo independently and can race each
    other into claiming (and copying) the same file twice."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.db = Database()
        self.queue = asyncio.Queue(maxsize=1000)
        self.processing = {}
    
    async def add_file(self, file_info):
        try:
            await self.queue.put(file_info)
            self.db.add_to_queue(file_info)
            return True
        except Exception as e:
            logger.error(f"Add to queue error: {e}")
            return False
    
    async def get_file(self):
        try:
            if self.queue.empty():
                # claim_batch atomically flips status queued -> processing in
                # Mongo as it reads each item, so even if multiple workers hit
                # this refill at once, each queued doc can only be claimed once.
                batch = self.db.claim_batch(limit=50)
                for item in batch:
                    if self.queue.qsize() < 900:
                        await self.queue.put(item)
            
            if self.queue.empty():
                return None
            
            file_info = await self.queue.get()
            self.processing[file_info['source_message_id']] = file_info
            
            # Already marked 'processing' by claim_batch above; no extra write needed.
            
            return file_info
        except Exception as e:
            logger.error(f"Get from queue error: {e}")
            return None
    
    async def mark_completed(self, file_info):
        try:
            self.processing.pop(file_info['source_message_id'], None)
            self.db.update_status(
                file_info['source_chat_id'],
                file_info['source_message_id'],
                'completed',
                file_info.get('destination_message_id'),
                file_info.get('account')
            )
        except Exception as e:
            logger.error(f"Mark completed error: {e}")
    
    async def mark_failed(self, file_info, error=None):
        try:
            self.processing.pop(file_info['source_message_id'], None)
            self.db.update_status(
                file_info['source_chat_id'],
                file_info['source_message_id'],
                'failed',
                error=error
            )
            self.db.increment_retry(
                file_info['source_chat_id'],
                file_info['source_message_id']
            )
        except Exception as e:
            logger.error(f"Mark failed error: {e}")
    
    def qsize(self):
        return self.queue.qsize()
    
    def processing_count(self):
        return len(self.processing)
