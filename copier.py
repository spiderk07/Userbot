from pyrogram import Client
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, UserNotParticipant,
    ChannelPrivate, PeerIdInvalid, MessageIdInvalid,
    BotMethodInvalid, ChatWriteForbidden, UserBannedInChannel,
    ChatSendMediaForbidden, MessageTooLong
)
import asyncio
from typing import Optional, Dict
from config import Config
import logging
import os

logger = logging.getLogger(__name__)

class Copier:
    """Message Copier with FloodWait handling"""
    
    def __init__(self):
        self.copy_mode = Config.COPY_MODE
        self.retry_limit = Config.RETRY_LIMIT
    
    async def copy_message(self, client: Client, file_info: Dict) -> Optional[int]:
        """Copy message from source to destination"""
        source_chat_id = int(file_info['source_chat_id'])
        source_message_id = int(file_info['source_message_id'])
        destination_chat_id = int(file_info['destination_chat_id'])
        
        for attempt in range(self.retry_limit):
            try:
                # Try to get the message first
                try:
                    message = await client.get_messages(source_chat_id, source_message_id)
                    if not message:
                        logger.error(f"Message {source_message_id} not found in {source_chat_id}")
                        return None
                except Exception as e:
                    logger.warning(f"Could not get message {source_message_id}: {e}")
                
                # Copy or forward based on mode
                if self.copy_mode == 'forward':
                    sent_message = await client.forward_messages(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_ids=source_message_id,
                        disable_notification=True
                    )
                else:
                    sent_message = await client.copy_message(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                        disable_notification=True
                    )
                
                if sent_message:
                    logger.info(f"✅ Copied message {source_message_id} -> {sent_message.id}")
                    return sent_message.id
                else:
                    logger.warning(f"Copy returned None for message {source_message_id}")
                    return None
                    
            except FloodWait as e:
                wait_time = e.value
                logger.warning(f"⏳ FloodWait: Waiting {wait_time} seconds (attempt {attempt + 1})")
                await asyncio.sleep(wait_time)
                continue
                
            except (ChatAdminRequired, ChatWriteForbidden, UserBannedInChannel, 
                    ChatSendMediaForbidden) as e:
                logger.error(f"❌ Permission error: {e}")
                return None
                
            except (ChannelPrivate, PeerIdInvalid) as e:
                logger.error(f"❌ Channel access error: {e}")
                return None
                
            except MessageIdInvalid as e:
                logger.error(f"❌ Message ID invalid: {e}")
                return None
                
            except MessageTooLong as e:
                logger.error(f"❌ Message too long: {e}")
                return None
                
            except Exception as e:
                logger.error(f"❌ Copy error (attempt {attempt + 1}): {e}")
                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return None
        
        return None
    
    async def check_access(self, client: Client, source_chat_id: int, 
                          destination_chat_id: int) -> bool:
        """Check access to both channels"""
        try:
            # Check source access
            try:
                await client.get_chat(source_chat_id)
                logger.info(f"✅ Can access source: {source_chat_id}")
            except Exception as e:
                logger.error(f"❌ Cannot access source {source_chat_id}: {e}")
                return False
            
            # Check destination access
            try:
                await client.get_chat(destination_chat_id)
                logger.info(f"✅ Can access destination: {destination_chat_id}")
            except Exception as e:
                logger.error(f"❌ Cannot access destination {destination_chat_id}: {e}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Access check error: {e}")
            return False
