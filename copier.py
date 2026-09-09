from pyrogram import Client
from pyrogram.errors import FloodWait
import asyncio
from typing import Optional, Dict
from config import Config
import logging

logger = logging.getLogger(__name__)

class Copier:
    def __init__(self):
        self.copy_mode = Config.COPY_MODE
        self.retry_limit = Config.RETRY_LIMIT
    
    async def copy_message(self, client: Client, file_info: Dict) -> Optional[int]:
        source_chat_id = int(file_info['source_chat_id'])
        source_message_id = int(file_info['source_message_id'])
        destination_chat_id = int(file_info['destination_chat_id'])
        
        for attempt in range(self.retry_limit):
            try:
                if self.copy_mode == 'forward':
                    sent_message = await client.forward_messages(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_ids=source_message_id
                    )
                else:
                    sent_message = await client.copy_message(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id
                    )
                
                if sent_message:
                    return sent_message.id
                return None
                
            except FloodWait as e:
                logger.warning(f"⏳ FloodWait: {e.value}s")
                await asyncio.sleep(e.value)
                continue
                
            except Exception as e:
                logger.error(f"❌ Copy error: {e}")
                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(2)
                    continue
                return None
        
        return None
