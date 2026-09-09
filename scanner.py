import asyncio
from pyrogram import Client
import logging
from config import Config

logger = logging.getLogger(__name__)

class Scanner:
    def __init__(self, clients, db, queue_manager):
        self.clients = clients
        self.db = db
        self.queue_manager = queue_manager
        self.is_running = True
    
    def _has_media(self, message):
        return (
            message.video is not None or 
            message.document is not None or 
            message.audio is not None or 
            message.photo is not None or 
            message.voice is not None or 
            message.video_note is not None
        )
    
    async def scan_history(self):
        logger.info("Starting history scan...")
        
        for source_str, dest_str in zip(Config.SOURCE_CHANNELS, Config.DESTINATION_CHANNELS):
            source_chat_id = int(source_str.strip())
            destination_chat_id = int(dest_str.strip())
            
            # किसी भी client से try करें
            scanner_client = None
            last_error = None
            for client in self.clients:
                try:
                    await client.get_chat(source_chat_id)
                    scanner_client = client
                    break
                except Exception as e:
                    last_error = e
                    continue
            
            if not scanner_client:
                logger.error(f"No access to source: {source_chat_id} (last error: {last_error})")
                continue
            
            progress = self.db.get_progress(source_chat_id)
            last_message_id = progress['last_message_id'] if progress else 0
            
            logger.info(f"Scanning {source_chat_id} from {last_message_id}")
            
            current_offset = last_message_id
            total_queued = 0
            
            while self.is_running:
                try:
                    # Pyrogram 2.x me get_chat_history ek async generator hai,
                    # coroutine nahi — isliye await nahi, async for se iterate karna hai.
                    messages = []
                    async for message in scanner_client.get_chat_history(
                        chat_id=source_chat_id,
                        offset_id=current_offset,
                        limit=Config.BATCH_SIZE
                    ):
                        messages.append(message)
                    
                    if not messages:
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
                    
                    logger.info(f"Scanned {len(messages)}, queued {queued_count}")
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Scan error: {e}")
                    await asyncio.sleep(5)
            
            logger.info(f"Total queued from {source_chat_id}: {total_queued}")
    
    async def listen_new_messages(self):
        logger.info("Listening for new messages...")
        
        for client in self.clients:
            @client.on_message()
            async def handler(client, message):
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
                            logger.info(f"New message queued: {message.id}")
                
                except Exception as e:
                    logger.error(f"Handler error: {e}")
        
        while self.is_running:
            await asyncio.sleep(1)
    
    def stop(self):
        self.is_running = False
