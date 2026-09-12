import ssl
import re
import certifi
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from datetime import datetime, timedelta
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
        """update_one(upsert=True) MongoDB me ek single ATOMIC operation
        hota hai — chahe 4 alag threads/accounts isse EK HI waqt pe call
        karein, sirf EK hi call ka insert 'jeetega', baaki sab no-op
        honge. Isliye return value (True = ye call sach me nayi entry thi)
        ko hi "duplicate hai ya nahi" ka faisla lene do — is_duplicate()
        ka pehle se check karna ek check-THEN-act race tha: 4 threads
        pehle sab "duplicate nahi hai" dekh lete the (kyunki kisi ne abhi
        insert nahi kiya tha), phir sab aage badh jaate — isi se "New
        message queued" ek hi ID ke liye 3-4 baar print ho raha tha."""
        self.ensure_connection()
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
        # True sirf tabhi jab ye call KHUD naya document insert kar paya —
        # yahi caller ke liye "is message ko pehli baar discover kiya" ka
        # sahi (race-free) signal hai.
        return result.upserted_id is not None
    
    def add_to_queue(self, file_info):
        """Sirf INSERT karta hai agar ye message pehle se queue collection
        me exist NAHI karta — $setOnInsert use kiya hai, $set nahi. Pehle
        yahan har discovery par poora document $set se 'queued' status pe
        FORCE RESET ho jaata tha, chahe wo doc us waqt 'processing' ho ya
        'completed' — matlab agar wahi message dobara discover ho jaaye
        (jaise live-listener aur history-scan ka overlap, ya processing
        chalte hue hi rescan usse phir se utha le), to already-processing/
        completed item wapas 'queued' ban jaata aur ek doosra worker use
        DOBARA copy kar deta (same-file double forward). Ab agar doc
        pehle se kisi bhi status me exist karta hai, kuch bhi overwrite
        nahi hota — sirf bilkul naya discovery hi 'queued' insert karta hai."""
        self.ensure_connection()
        return self.queue.update_one(
            {
                'source_chat_id': file_info['source_chat_id'],
                'source_message_id': file_info['source_message_id']
            },
            {
                '$setOnInsert': {
                    **file_info,
                    'status': 'queued',
                    'account': None,
                    'destination_message_id': None,
                    'retry_count': 0,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
            },
            upsert=True
        )
    
    def get_next_batch(self, status='queued', limit=10):
        self.ensure_connection()
        # source_message_id ascending = oldest message pehle — Telegram history
        # hamesha newest->oldest milti hai, isliye discovery order (created_at)
        # se sort karna galat tha; ab hum message id se khud chronological
        # order banate hain.
        return list(self.queue.find({'status': status}).sort('source_message_id', 1).limit(limit))

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
                # oldest source_message_id sabse pehle claim ho — naye messages
                # baad me. created_at se sort karte to sirf "discovery order"
                # milta (jo scan ke uleta/backward direction ki wajah se
                # newest-first ban jaata tha).
                sort=[('source_message_id', 1)]
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
    
    def recover_stuck_processing(self, stuck_after_seconds=180):
        """Restart/crash ke waqt jo files 'processing' me atki reh gayi thi,
        unhe wapas 'queued' bana do taaki worker unhe dubara pick kar sake.

        SAFETY: sirf un items ko recover karo jo kam se kam
        `stuck_after_seconds` se processing me hain. Turant (0 second) sab
        kuch reset karna khatarnak hai — Render redeploy/restart ke waqt
        PURANA process kabhi-kabhi kuch second aur chalta rehta hai (naye
        process ke saath overlap), aur agar naya process turant sab
        'processing' ko queued bana de, to purana process jo message USI
        WAQT Telegram par bhej raha hai, use bhi ek doosra worker turant
        dubara claim karke DOBARA bhej deta hai — yahi asal duplicate-
        forward ka reason tha (log me Worker 2 aur Worker 1 dono ka same
        ID copy karna)."""
        self.ensure_connection()
        cutoff = datetime.now() - timedelta(seconds=stuck_after_seconds)
        result_files = self.files.update_many(
            {'status': 'processing', 'updated_at': {'$lt': cutoff}},
            {'$set': {'status': 'queued', 'updated_at': datetime.now()}}
        )
        result_queue = self.queue.update_many(
            {'status': 'processing', 'updated_at': {'$lt': cutoff}},
            {'$set': {'status': 'queued', 'updated_at': datetime.now()}}
        )
        recovered = max(result_files.modified_count, result_queue.modified_count)
        if recovered:
            logger.info(f"Recovered {recovered} stuck 'processing' file(s) back to queued")
        return recovered

    def is_duplicate(self, source_chat_id, source_message_id):
        """Agar is message ka record already 'pending'/'processing'/
        'completed' me hai, to True — discovery (scan ya live listener,
        dono clients se) use dubara add nahi karegi. 'failed' jaan-bujh
        kar exclude kiya hai taaki agli scan/discovery se wo naturally
        retry ho sake."""
        self.ensure_connection()
        return self.files.find_one({
            'source_chat_id': source_chat_id,
            'source_message_id': source_message_id,
            'status': {'$in': ['pending', 'processing', 'completed']}
        }) is not None
    
    def save_progress(self, source_chat_id, last_message_id, total_processed, total_messages=None, batch_type_counts=None):
        """total_processed yahan is batch ka scanned count hai (cumulative
        $inc hota hai — 'scanned_count' me), total_messages sirf pehli baar
        set hota hai (channel ka latest msg id), aur batch_type_counts se
        har message-type (video/document/photo/...) ka running total
        'type_counts.<type>' me jama hota hai — taaki baad me pata chal sake
        channel me asal me hai kya, bina saare logs khangaale."""
        self.ensure_connection()
        inc = {'scanned_count': total_processed}
        if batch_type_counts:
            for mtype, count in batch_type_counts.items():
                inc[f'type_counts.{mtype}'] = count
        update = {
            '$set': {
                'last_message_id': last_message_id,
                'updated_at': datetime.now()
            },
            '$inc': inc,
            '$setOnInsert': {
                'scan_started_at': datetime.now()
            }
        }
        if total_messages is not None:
            update['$set']['total_messages'] = total_messages
        self.progress.update_one(
            {'source_chat_id': source_chat_id},
            update,
            upsert=True
        )
    
    def get_progress(self, source_chat_id):
        self.ensure_connection()
        return self.progress.find_one({'source_chat_id': source_chat_id})

    def get_all_progress(self):
        self.ensure_connection()
        return list(self.progress.find({}))
    
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
