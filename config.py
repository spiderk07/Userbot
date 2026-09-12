import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    API_ID = int(os.getenv('API_ID', '0'))
    API_HASH = os.getenv('API_HASH', '')
    
    BOT_TOKEN = os.getenv('BOT_TOKEN', '')
    
    SESSION_1 = os.getenv('SESSION_1', '')
    SESSION_2 = os.getenv('SESSION_2', '')
    SESSION_3 = os.getenv('SESSION_3', '')
    SESSION_4 = os.getenv('SESSION_4', '')
    SESSION_5 = os.getenv('SESSION_5', '')
    SESSION_6 = os.getenv('SESSION_6', '')
    
    SOURCE_CHANNELS = [x.strip() for x in os.getenv('SOURCE_CHANNELS', '').split(',') if x.strip()]
    DESTINATION_CHANNELS = [x.strip() for x in os.getenv('DESTINATION_CHANNELS', '').split(',') if x.strip()]
    
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    DB_NAME = os.getenv('DB_NAME', 'telegram_copier')
    
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
    COPY_MODE = os.getenv('COPY_MODE', 'copy')
    RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', '3'))

    # Har file copy karne se pehle worker itna (random, in dono ke beech)
    # ruk'ta hai — Telegram FloodWait se bachne ke liye. Agar logs me
    # "FloodWait" dikhe to inhe badha do; agar bilkul na dikhe kaafi der
    # tak, to thoda kam karke test kar sakte ho.
    # Har file copy karne se pehle worker itna (random, in dono ke beech)
    # ruk'ta hai — Telegram FloodWait/PeerFlood se bachne ke liye. Account
    # ki safety speed se zyada zaroori hai, isliye defaults conservative
    # rakhe hain. Sirf tabhi kam karo jab kaafi der (ghanton) tak logs me
    # koi FloodWait na dikha ho.
    WORKER_DELAY_MIN = float(os.getenv('WORKER_DELAY_MIN', '0.4'))
    WORKER_DELAY_MAX = float(os.getenv('WORKER_DELAY_MAX', '0.9'))

    # Har account (worker) ek saath kitni files parallel copy kare.
    # DEFAULT 1 RAKHO — concurrency > 1 se ek session se ek saath kai
    # requests jaati hain, jo Telegram ke automation-detection ko
    # zyada aasani se trigger karta hai. Sirf tabhi 2 try karo jab
    # accounts purane/trusted hon aur aap FloodWait/PeerFlood ke liye
    # logs actively monitor kar rahe ho.
    WORKER_CONCURRENCY = int(os.getenv('WORKER_CONCURRENCY', '1'))
    
    ADMIN_USER_IDS = [int(x.strip()) for x in os.getenv('ADMIN_USER_IDS', '').split(',') if x.strip()]
    
    PORT = int(os.getenv('PORT', '8000'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @staticmethod
    def get_sessions():
        sessions = []
        for session in [
            Config.SESSION_1, Config.SESSION_2, Config.SESSION_3,
            Config.SESSION_4, Config.SESSION_5, Config.SESSION_6,
        ]:
            if session and session.strip():
                sessions.append(session.strip())
        return sessions
    
    @staticmethod
    def validate():
        errors = []
        if not Config.API_ID:
            errors.append("API_ID required")
        if not Config.API_HASH:
            errors.append("API_HASH required")
        if not Config.get_sessions():
            errors.append("At least one session required")
        if not Config.SOURCE_CHANNELS:
            errors.append("SOURCE_CHANNELS required")
        if not Config.DESTINATION_CHANNELS:
            errors.append("DESTINATION_CHANNELS required")
        if len(Config.SOURCE_CHANNELS) != len(Config.DESTINATION_CHANNELS):
            errors.append("Source and destination count must match")
        if not Config.MONGO_URI:
            errors.append("MONGO_URI required")
        return errors
