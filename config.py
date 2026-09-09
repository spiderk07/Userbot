import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

class Config:
    """Application Configuration"""
    
    # Telegram API Credentials
    API_ID = int(os.getenv('API_ID', '0'))
    API_HASH = os.getenv('API_HASH', '')
    
    # Bot Token (Optional - for monitoring and control)
    BOT_TOKEN = os.getenv('BOT_TOKEN', '')
    
    # Session Strings for 3 Userbot accounts
    SESSION_1 = os.getenv('SESSION_1', '')
    SESSION_2 = os.getenv('SESSION_2', '')
    SESSION_3 = os.getenv('SESSION_3', '')
    
    # Get all sessions as list
    @staticmethod
    def get_sessions() -> List[str]:
        sessions = []
        for session in [Config.SESSION_1, Config.SESSION_2, Config.SESSION_3]:
            if session and session.strip():
                sessions.append(session.strip())
        return sessions
    
    # Channels Configuration
    SOURCE_CHANNELS = [x.strip() for x in os.getenv('SOURCE_CHANNELS', '').split(',') if x.strip()]
    DESTINATION_CHANNELS = [x.strip() for x in os.getenv('DESTINATION_CHANNELS', '').split(',') if x.strip()]
    
    # MongoDB Configuration
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    DB_NAME = os.getenv('DB_NAME', 'telegram_copier')
    
    # Processing Settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
    COPY_MODE = os.getenv('COPY_MODE', 'copy')  # 'copy' or 'forward'
    RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', '3'))
    MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', '5'))
    
    # Bot Settings
    USE_BOT_FOR_MONITORING = os.getenv('USE_BOT_FOR_MONITORING', 'true').lower() == 'true'
    USE_BOT_FOR_POSTING = os.getenv('USE_BOT_FOR_POSTING', 'false').lower() == 'true'
    
    # Admin Settings (Bot commands ke liye)
    ADMIN_USER_IDS = [int(x.strip()) for x in os.getenv('ADMIN_USER_IDS', '').split(',') if x.strip()]
    
    # Supported Media Types
    SUPPORTED_TYPES = ['video', 'document', 'audio', 'photo', 'voice', 'video_note']
    
    # Flask Settings
    PORT = int(os.getenv('PORT', '8000'))
    
    # Logging Settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @staticmethod
    def validate():
        """Validate configuration"""
        errors = []
        
        if not Config.API_ID:
            errors.append("API_ID is required")
        if not Config.API_HASH:
            errors.append("API_HASH is required")
        if not Config.get_sessions():
            errors.append("At least one session string is required")
        if not Config.SOURCE_CHANNELS:
            errors.append("SOURCE_CHANNELS is required")
        if not Config.DESTINATION_CHANNELS:
            errors.append("DESTINATION_CHANNELS is required")
        if len(Config.SOURCE_CHANNELS) != len(Config.DESTINATION_CHANNELS):
            errors.append("Number of source and destination channels must match")
        if not Config.MONGO_URI:
            errors.append("MONGO_URI is required")
        
        return errors
