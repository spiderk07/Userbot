import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

class Config:
    """Application Configuration"""
    
    # Telegram API Credentials
    API_ID = int(os.getenv('API_ID', '0'))
    API_HASH = os.getenv('API_HASH', '')
    
    # Bot Token (Optional - for monitoring and control)
    BOT_TOKEN = os.getenv('BOT_TOKEN', '')
    
    # Session Strings (Flexible - 1 to 3 sessions)
    SESSION_1 = os.getenv('SESSION_1', '')
    SESSION_2 = os.getenv('SESSION_2', '')
    SESSION_3 = os.getenv('SESSION_3', '')
    SESSION_4 = os.getenv('SESSION_4', '')  # Optional 4th session
    SESSION_5 = os.getenv('SESSION_5', '')  # Optional 5th session
    
    # Get all available sessions as list
    @staticmethod
    def get_sessions() -> List[str]:
        """Get all non-empty session strings"""
        sessions = []
        for i in range(1, 6):  # Support up to 5 sessions
            session = getattr(Config, f'SESSION_{i}', '')
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
    
    # Bot Settings
    USE_BOT_FOR_MONITORING = os.getenv('USE_BOT_FOR_MONITORING', 'true').lower() == 'true'
    
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
        
        sessions = Config.get_sessions()
        if not sessions:
            errors.append("At least one session string is required")
        else:
            print(f"✅ Found {len(sessions)} session(s)")
        
        if not Config.SOURCE_CHANNELS:
            errors.append("SOURCE_CHANNELS is required")
        if not Config.DESTINATION_CHANNELS:
            errors.append("DESTINATION_CHANNELS is required")
        if len(Config.SOURCE_CHANNELS) != len(Config.DESTINATION_CHANNELS):
            errors.append("Number of source and destination channels must match")
        if not Config.MONGO_URI:
            errors.append("MONGO_URI is required")
        
        return errors
