import asyncio
import logging
from database import Database

logger = logging.getLogger(__name__)

class QueueManager:
    def __init__(self):
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
                batch = self.db.get_next_batch(status='queued', limit=50)
                for item in batch:
                    if self.queue.qsize() < 900:
                        await self.queue.put(item)
            
            if self.queue.empty():
                return None
            
            file_info = await self.queue.get()
            self.processing[file_info['source_message_id']] = file_info
            
            self.db.update_status(
                file_info['source_chat_id'],
                file_info['source_message_id'],
                'processing'
            )
            
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
