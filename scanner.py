import asyncio
from pyrogram import Client
from config import Config
from database import Database
from queue_manager import QueueManager
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

class Scanner:
    def __init__(self, clients: List[Client], db: Database, queue_manager: QueueManager):
        self.clients = clients
        self.db = db
        self.queue_manager = queue_manager
        self.is_running = True
        self.client_access_map = {}
        logger.info(f"Scanner initialized with {len(clients)} client(s)")
    
    async def find_client_for_channel(self, chat_id: int) -> Optional[Client]:
        if chat_id in self.client_access_map:
            return self.client_access_map[chat_id]
        
        for client in self.clients:
            try:
                chat = await client.get_chat(chat_id)
                logger.info(f"✅ Access confirmed for {chat_id}: {chat.title}")
                self.client_access_map[chat_id] = client
                return client
            except Exception as e:
                continue
        
        return None
    
    async def scan_history(self):
        logger.info("🔄 Starting history scan...")
        
        for source_str, dest_str in zip(Config.SOURCE_CHANNELS, Config.DESTINATION_CHANNELS):
            try:
                source_chat_id = int(source_str.strip())
                destination_chat_id = int(dest_str.strip())
            except ValueError:
                logger.error(f"❌ Invalid channel ID")
                continue
            
            logger.info(f"\n📥 Processing: {source_chat_id} -> {destination_chat_id}")
            
            scanner_client = await self.find_client_for_channel(source_chat_id)
            
            if not scanner_client:
                logger.error(f"❌ No access to source: {source_chat_id}")
                continue
            
            progress = self.db.get_progress(source_chat_id)
            last_message_id = progress['last_message_id'] if progress else 0
            
            logger.info(f"Starting from message ID: {last_message_id}")
            
            batch_size = Config.BATCH_SIZE
            current_offset = last_message_id
            total_queued = 0
            
            while self.is_running:
                try:
                    messages = await scanner_client.get_chat_history(
                        chat_id=source_chat_id,
                        offset_id=current_offset,
                        limit=batch_size
                    )
                    
                    if not messages:
                        logger.info(f"✅ Scan complete for {source_chat_id}")
                        break
                    
                    queued_count = 0
                    for message in messages:
                        if self._has_media(message):
                            if not self.db.is_duplicate(source_chat_id, message.id):
                                file_info = {
                                    'source_chat_id': source_chat_id,
                                    'source_message_id': message.id,
                                    'destination_chat_id': destination_chat_id,
                                    'has_media': True
                                }
                                
                                self.db.add_file_record(source_chat_id, message.id, destination_chat_id)
                                await self.queue_manager.add_file(file_info)
                                queued_count += 1
                                total_queued += 1
                    
                    if messages:
                        current_offset = messages[-1].id
                        self.db.save_progress(source_chat_id, current_offset, queued_count)
                    
                    logger.info(f"📊 Scanned {len(messages)} messages, queued {queued_count} files")
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Scan error: {e}")
                    await asyncio.sleep(5)
            
            logger.info(f"✅ Total queued: {total_queued} files")
    
    def _has_media(self, message) -> bool:
        return (
            message.video or 
            message.document or 
            message.audio or 
            message.photo or 
            message.voice or 
            message.video_note
        )
    
    async def listen_new_messages(self):
        logger.info("👂 Starting new message listeners...")
        
        for client in self.clients:
            @client.on_message()
            async def handle_new_message(client, message):
                if not self.is_running:
                    return
                
                try:
                    source_chat_id = str(message.chat.id)
                    
                    if source_chat_id not in Config.SOURCE_CHANNELS:
                        return
                    
                    index = Config.SOURCE_CHANNELS.index(source_chat_id)
                    destination_chat_id = Config.DESTINATION_CHANNELS[index]
                    
                    if self._has_media(message):
                        if not self.db.is_duplicate(source_chat_id, message.id):
                            file_info = {
                                'source_chat_id': source_chat_id,
                                'source_message_id': message.id,
                                'destination_chat_id': destination_chat_id,
                                'has_media': True
                            }
                            
                            self.db.add_file_record(int(source_chat_id), message.id, int(destination_chat_id))
                            await self.queue_manager.add_file(file_info)
                            logger.info(f"🆕 New message queued: {message.id}")
                
                except Exception as e:
                    logger.error(f"❌ Handler error: {e}")
        
        while self.is_running:
            await asyncio.sleep(1)
    
    def stop(self):
        self.is_running = False
