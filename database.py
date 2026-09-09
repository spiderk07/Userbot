import ssl
import certifi
from pymongo import MongoClient
from datetime import datetime
import logging
from config import Config  # ✅ यह import add करें

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.files = None
        self.queue = None
        self.progress = None
        self.connect()
    
    def connect(self):
        try:
            self.client = MongoClient(
                Config.MONGO_URI,
                tls=True,
                tlsAllowInvalidCertificates=True,
                tlsAllowInvalidHostnames=True,
                tlsCAFile=certifi.where(),
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=15000,
                socketTimeoutMS=30000,
                maxPoolSize=10,
                retryWrites=True
            )
            
            self.client.admin.command('ping')
            
            self.db = self.client[Config.DB_NAME]
            self.files = self.db['files']
            self.queue = self.db['queue']
            self.progress = self.db['progress']
            
            self._create_indexes()
            logger.info("✅ MongoDB connected")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise
    
    def _create_indexes(self):
        try:
            self.files.create_index(
                [('source_chat_id', 1), ('source_message_id', 1)],
                unique=True
            )
            self.files.create_index([('status', 1)])
            self.queue.create_index([('status', 1), ('created_at', 1)])
            self.progress.create_index([('source_chat_id', 1)], unique=True)
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    def ensure_connection(self):
        try:
            self.client.admin.command('ping')
        except:
            logger.warning("MongoDB reconnecting...")
            self.connect()
    
    def add_file_record(self, source_chat_id, source_message_id, destination_chat_id=None):
        self.ensure_connection()
        return self.files.update_one(
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
    
    def add_to_queue(self, file_info):
        self.ensure_connection()
        return self.queue.update_one(
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
    
    def get_next_batch(self, status='queued', limit=10):
        self.ensure_connection()
        return list(self.queue.find({'status': status}).sort('created_at', 1).limit(limit))
    
    def update_status(self, source_chat_id, source_message_id, status, destination_message_id=None, account=None, error=None):
        self.ensure_connection()
        update_data = {'status': status, 'updated_at': datetime.now()}
        if destination_message_id:
            update_data['destination_message_id'] = destination_message_id
        if account:
            update_data['account'] = account
        if error:
            update_data['last_error'] = error
        
        self.files.update_one(
            {'source_chat_id': source_chat_id, 'source_message_id': source_message_id},
            {'$set': update_data}
        )
        self.queue.update_one(
            {'source_chat_id': source_chat_id, 'source_message_id': source_message_id},
            {'$set': update_data}
        )
    
    def increment_retry(self, source_chat_id, source_message_id):
        self.ensure_connection()
        self.files.update_one(
            {'source_chat_id': source_chat_id, 'source_message_id': source_message_id},
            {'$inc': {'retry_count': 1}, '$set': {'updated_at': datetime.now()}}
        )
    
    def is_duplicate(self, source_chat_id, source_message_id):
        self.ensure_connection()
        return self.files.find_one({
            'source_chat_id': source_chat_id,
            'source_message_id': source_message_id,
            'status': 'completed'
        }) is not None
    
    def save_progress(self, source_chat_id, last_message_id, total_processed):
        self.ensure_connection()
        self.progress.update_one(
            {'source_chat_id': source_chat_id},
            {
                '$set': {
                    'last_message_id': last_message_id,
                    'total_processed': total_processed,
                    'updated_at': datetime.now()
                }
            },
            upsert=True
        )
    
    def get_progress(self, source_chat_id):
        self.ensure_connection()
        return self.progress.find_one({'source_chat_id': source_chat_id})
    
    def get_stats(self):
        self.ensure_connection()
        total = self.files.count_documents({})
        completed = self.files.count_documents({'status': 'completed'})
        pending = self.files.count_documents({'status': {'$in': ['pending', 'queued']}})
        failed = self.files.count_documents({'status': 'failed'})
        return {'total': total, 'completed': completed, 'pending': pending, 'failed': failed}
    
    def close(self):
        if self.client:
            self.client.close()
