from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from config import Config
import time
import logging

logger = logging.getLogger(__name__)

class Database:
    """MongoDB Database Handler"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.files = None
        self.queue = None
        self.progress = None
        self.channels = None
        self.connect()
    
    def connect(self):
        """Connect to MongoDB with retry logic"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.client = MongoClient(
                    Config.MONGO_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=10000,
                    maxPoolSize=10,
                    retryWrites=True,
                    w='majority'
                )
                
                # Test connection
                self.client.admin.command('ping')
                
                self.db = self.client[Config.DB_NAME]
                self.files = self.db['files']
                self.queue = self.db['queue']
                self.progress = self.db['progress']
                self.channels = self.db['channels']
                
                # Create indexes
                self._create_indexes()
                
                logger.info("✅ MongoDB connected successfully")
                return True
                
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                retry_count += 1
                logger.warning(f"MongoDB connection failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)  # Exponential backoff
                else:
                    logger.error("Failed to connect to MongoDB after all retries")
                    raise
    
    def _create_indexes(self):
        """Create database indexes"""
        try:
            # Files collection indexes
            self.files.create_index(
                [('source_chat_id', 1), ('source_message_id', 1)],
                unique=True,
                name='unique_file_idx'
            )
            self.files.create_index([('status', 1)], name='status_idx')
            self.files.create_index([('created_at', -1)], name='created_at_idx')
            self.files.create_index([('destination_chat_id', 1)], name='dest_chat_idx')
            
            # Queue collection indexes
            self.queue.create_index(
                [('status', 1), ('created_at', 1)],
                name='queue_status_idx'
            )
            self.queue.create_index(
                [('source_chat_id', 1), ('source_message_id', 1)],
                unique=True,
                name='unique_queue_idx'
            )
            
            # Progress collection indexes
            self.progress.create_index(
                [('source_chat_id', 1)],
                unique=True,
                name='unique_progress_idx'
            )
            
            # Channels collection indexes
            self.channels.create_index(
                [('source_chat_id', 1), ('destination_chat_id', 1)],
                unique=True,
                name='unique_channel_pair_idx'
            )
            
            logger.info("✅ Database indexes created")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    def ensure_connection(self):
        """Check and reconnect if needed"""
        try:
            self.client.admin.command('ping')
        except:
            logger.warning("MongoDB connection lost, reconnecting...")
            self.connect()
    
    # File Operations
    def add_file_record(self, source_chat_id: int, source_message_id: int, 
                       destination_chat_id: Optional[int] = None) -> bool:
        """Add file to database"""
        self.ensure_connection()
        try:
            result = self.files.update_one(
                {
                    'source_chat_id': source_chat_id,
                    'source_message_id': source_message_id
                },
                {
                    '$setOnInsert': {
                        'source_chat_id': source_chat_id,
                        'source_message_id': source_message_id,
                        'destination_chat_id': destination_chat_id,
                        'status': 'pending',
                        'retry_count': 0,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    }
                },
                upsert=True
            )
            return result.upserted_id is not None or result.modified_count > 0
        except Exception as e:
            logger.error(f"Error adding file record: {e}")
            return False
    
    def add_to_queue(self, file_info: Dict) -> bool:
        """Add file to processing queue"""
        self.ensure_connection()
        try:
            result = self.queue.update_one(
                {
                    'source_chat_id': file_info['source_chat_id'],
                    'source_message_id': file_info['source_message_id']
                },
                {
                    '$set': {
                        **file_info,
                        'status': 'queued',
                        'account': None,
                        'destination_message_id': None,
                        'retry_count': 0,
                        'updated_at': datetime.now()
                    },
                    '$setOnInsert': {
                        'created_at': datetime.now()
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding to queue: {e}")
            return False
    
    def get_next_batch(self, status: str = 'queued', limit: int = 10) -> List[Dict]:
        """Get next batch from queue"""
        self.ensure_connection()
        try:
            return list(self.queue.find(
                {'status': status}
            ).sort('created_at', 1).limit(limit))
        except Exception as e:
            logger.error(f"Error getting next batch: {e}")
            return []
    
    def update_status(self, source_chat_id: int, source_message_id: int, 
                     status: str, destination_message_id: Optional[int] = None,
                     account: Optional[str] = None, error: Optional[str] = None) -> bool:
        """Update file status"""
        self.ensure_connection()
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.now()
            }
            if destination_message_id:
                update_data['destination_message_id'] = destination_message_id
            if account:
                update_data['account'] = account
            if error:
                update_data['last_error'] = error
            
            # Update in files collection
            self.files.update_one(
                {
                    'source_chat_id': source_chat_id,
                    'source_message_id': source_message_id
                },
                {'$set': update_data}
            )
            
            # Update in queue
            self.queue.update_one(
                {
                    'source_chat_id': source_chat_id,
                    'source_message_id': source_message_id
                },
                {'$set': update_data}
            )
            return True
        except Exception as e:
            logger.error(f"Error updating status: {e}")
            return False
    
    def increment_retry(self, source_chat_id: int, source_message_id: int) -> bool:
        """Increment retry count"""
        self.ensure_connection()
        try:
            self.files.update_one(
                {
                    'source_chat_id': source_chat_id,
                    'source_message_id': source_message_id
                },
                {
                    '$inc': {'retry_count': 1},
                    '$set': {'updated_at': datetime.now()}
                }
            )
            return True
        except Exception as e:
            logger.error(f"Error incrementing retry: {e}")
            return False
    
    def is_duplicate(self, source_chat_id: int, source_message_id: int) -> bool:
        """Check if file already exists"""
        self.ensure_connection()
        try:
            return self.files.find_one({
                'source_chat_id': source_chat_id,
                'source_message_id': source_message_id,
                'status': 'completed'
            }) is not None
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False
    
    # Progress Operations
    def save_progress(self, source_chat_id: int, last_message_id: int, 
                     total_processed: int) -> bool:
        """Save progress for a source channel"""
        self.ensure_connection()
        try:
            self.progress.update_one(
                {'source_chat_id': source_chat_id},
                {
                    '$set': {
                        'last_message_id': last_message_id,
                        'total_processed': total_processed,
                        'updated_at': datetime.now()
                    },
                    '$setOnInsert': {
                        'created_at': datetime.now()
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
            return False
    
    def get_progress(self, source_chat_id: int) -> Optional[Dict]:
        """Get progress for a source channel"""
        self.ensure_connection()
        try:
            return self.progress.find_one({'source_chat_id': source_chat_id})
        except Exception as e:
            logger.error(f"Error getting progress: {e}")
            return None
    
    # Statistics Operations
    def get_stats(self) -> Dict:
        """Get overall statistics"""
        self.ensure_connection()
        try:
            total = self.files.count_documents({})
            completed = self.files.count_documents({'status': 'completed'})
            pending = self.files.count_documents({'status': {'$in': ['pending', 'queued']}})
            processing = self.files.count_documents({'status': 'processing'})
            failed = self.files.count_documents({'status': 'failed'})
            
            # Get per-channel stats
            channel_stats = []
            for source_chat_id in Config.SOURCE_CHANNELS:
                sid = int(source_chat_id.strip())
                channel_total = self.files.count_documents({'source_chat_id': sid})
                channel_completed = self.files.count_documents({
                    'source_chat_id': sid,
                    'status': 'completed'
                })
                channel_stats.append({
                    'source_chat_id': sid,
                    'total': channel_total,
                    'completed': channel_completed,
                    'pending': channel_total - channel_completed
                })
            
            return {
                'total': total,
                'completed': completed,
                'pending': pending,
                'processing': processing,
                'failed': failed,
                'channels': channel_stats
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {
                'total': 0,
                'completed': 0,
                'pending': 0,
                'processing': 0,
                'failed': 0,
                'channels': []
            }
    
    # Channel Operations
    def add_channel_pair(self, source_chat_id: int, destination_chat_id: int) -> bool:
        """Add channel pair"""
        self.ensure_connection()
        try:
            self.channels.update_one(
                {
                    'source_chat_id': source_chat_id,
                    'destination_chat_id': destination_chat_id
                },
                {
                    '$set': {
                        'source_chat_id': source_chat_id,
                        'destination_chat_id': destination_chat_id,
                        'active': True,
                        'updated_at': datetime.now()
                    },
                    '$setOnInsert': {
                        'created_at': datetime.now()
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding channel pair: {e}")
            return False
    
    def cleanup_stale_queue(self, hours: int = 24) -> int:
        """Clean up stale queue items"""
        self.ensure_connection()
        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            result = self.queue.delete_many({
                'status': 'queued',
                'updated_at': {'$lt': cutoff}
            })
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up queue: {e}")
            return 0
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
