import asyncio
from typing import Optional, Dict
from database import Database
import logging

logger = logging.getLogger(__name__)

class QueueManager:
    """Async Queue Manager"""
    
    def __init__(self):
        self.db = Database()
        self.queue = asyncio.Queue(maxsize=1000)
        self.processing = {}
        self.lock = asyncio.Lock()
    
    async def add_file(self, file_info: Dict) -> bool:
        """Add file to queue"""
        try:
            # Add to in-memory queue
            await self.queue.put(file_info)
            
            # Add to database
            self.db.add_to_queue(file_info)
            
            logger.debug(f"Added to queue: {file_info.get('source_message_id')}")
            return True
        except Exception as e:
            logger.error(f"Error adding to queue: {e}")
            return False
    
    async def get_file(self) -> Optional[Dict]:
        """Get next file from queue"""
        try:
            if self.queue.empty():
                # Load more from database
                await self._load_from_db()
            
            if self.queue.empty():
                return None
            
            file_info = await self.queue.get()
            
            # Mark as processing
            async with self.lock:
                self.processing[file_info['source_message_id']] = file_info
            
            # Update in database
            self.db.update_status(
                file_info['source_chat_id'],
                file_info['source_message_id'],
                'processing'
            )
            
            return file_info
        except Exception as e:
            logger.error(f"Error getting from queue: {e}")
            return None
    
    async def mark_completed(self, file_info: Dict) -> bool:
        """Mark file as completed"""
        try:
            async with self.lock:
                self.processing.pop(file_info['source_message_id'], None)
            
            self.db.update_status(
                file_info['source_chat_id'],
                file_info['source_message_id'],
                'completed',
                file_info.get('destination_message_id'),
                file_info.get('account')
            )
            return True
        except Exception as e:
            logger.error(f"Error marking completed: {e}")
            return False
    
    async def mark_failed(self, file_info: Dict, error: str = None) -> bool:
        """Mark file as failed"""
        try:
            async with self.lock:
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
            return True
        except Exception as e:
            logger.error(f"Error marking failed: {e}")
            return False
    
    async def _load_from_db(self):
        """Load queued items from database"""
        try:
            batch = self.db.get_next_batch(status='queued', limit=50)
            for item in batch:
                if self.queue.qsize() < 900:  # Keep some space
                    await self.queue.put(item)
        except Exception as e:
            logger.error(f"Error loading from database: {e}")
    
    def qsize(self) -> int:
        """Get queue size"""
        return self.queue.qsize()
    
    def processing_count(self) -> int:
        """Get processing count"""
        return len(self.processing)
