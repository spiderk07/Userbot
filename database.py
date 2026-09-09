import ssl
import re
import certifi
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from datetime import datetime
import logging
from config import Config  # ✅ यह import add करें

logger = logging.getLogger(__name__)

class Database:
    """Singleton: every Database() call anywhere in the process returns the
    same connection/instance, so we don't open 3-4 separate Mongo connections
    and don't run _create_indexes() repeatedly on every startup."""
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
    
    def _ensure_index(self, collection, keys, name, **kwargs):
        """Create an index, self-healing if a same-key index already exists
        under a different name in Atlas (code 85, IndexOptionsConflict).
        Drops whatever name Mongo reports and recreates it under `name`, so
        it converges to a single consistent index instead of warning on
        every restart forever."""
        try:
            collection.create_index(keys, name=name, **kwargs)
        except OperationFailure as e:
            if e.code == 85:
                existing_name = None
                match = re.search(r"different name:\s*(\S+)", str(e))
                if match:
                    existing_name = match.group(1)
                try:
                    if existing_name:
                        collection.drop_index(existing_name)
                    collection.create_index(keys, name=name, **kwargs)
                    logger.info(f"Reconciled index '{existing_name}' -> '{name}' on {collection.name}")
                except Exception as e2:
                    logger.warning(f"Could not reconcile index '{name}' on {collection.name}: {e2}")
            else:
                logger.warning(f"Index creation warning ({collection.name}.{name}): {e}")

    def _create_indexes(self):
        self._ensure_index(
            self.files,
            [('source_chat_id', 1), ('source_message_id', 1)],
            name='unique_file_idx',
            unique=True
        )
        self._ensure_index(self.files, [('status', 1)], name='status_idx')
        self._ensure_index(
            self.queue,
            [('status', 1), ('created_at', 1)],
            name='queue_status_created_idx'
        )
        self._ensure_index(
            self.progress,
            [('source_chat_id', 1)],
            name='progress_source_idx',
            unique=True
        )
    
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

    def claim_batch(self, limit=10):
        """Atomically claim up to `limit` queued items (find_one_and_update),
        flipping each to 'processing' as it's claimed. This prevents two
        workers from both picking up the same item, which a plain find()
        can't guarantee once you run more than one worker."""
        self.ensure_connection()
        claimed = []
        for _ in range(limit):
            doc = self.queue.find_one_and_update(
                {'status': 'queued'},
                {'$set': {'status': 'processing', 'updated_at': datetime.now()}},
                sort=[('created_at', 1)]
            )
            if not doc:
                break
            claimed.append(doc)
            self.files.update_one(
                {'source_chat_id': doc['source_chat_id'], 'source_message_id': doc['source_message_id']},
                {'$set': {'status': 'processing', 'updated_at': datetime.now()}}
            )
        return claimed
    
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
    
    def recover_stuck_processing(self):
        """Restart/crash ke waqt jo files 'processing' me atki reh gayi thi,
        unhe wapas 'queued' bana do taaki worker unhe dubara pick kar sake."""
        self.ensure_connection()
        result_files = self.files.update_many(
            {'status': 'processing'},
            {'$set': {'status': 'queued', 'updated_at': datetime.now()}}
        )
        result_queue = self.queue.update_many(
            {'status': 'processing'},
            {'$set': {'status': 'queued', 'updated_at': datetime.now()}}
        )
        recovered = max(result_files.modified_count, result_queue.modified_count)
        if recovered:
            logger.info(f"Recovered {recovered} stuck 'processing' file(s) back to queued")
        return recovered

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
        processing = self.files.count_documents({'status': 'processing'})
        pending = self.files.count_documents({'status': {'$in': ['pending', 'queued']}})
        failed = self.files.count_documents({'status': 'failed'})

        channels = []
        for source_chat_id in self.files.distinct('source_chat_id'):
            channels.append({
                'source_chat_id': source_chat_id,
                'total': self.files.count_documents({'source_chat_id': source_chat_id}),
                'completed': self.files.count_documents({'source_chat_id': source_chat_id, 'status': 'completed'}),
                'pending': self.files.count_documents({'source_chat_id': source_chat_id, 'status': {'$in': ['pending', 'queued']}}),
            })

        return {
            'total': total,
            'completed': completed,
            'processing': processing,
            'pending': pending,
            'failed': failed,
            'channels': channels,
        }
    
    def close(self):
        if self.client:
            self.client.close()
