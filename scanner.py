import asyncio
from pyrogram import Client
from config import Config
from database import Database
from queue_manager import QueueManager
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

class Scanner:
    """History Scanner and New Message Listener"""
    
    def __init__(self, clients: List[Client], db: Database, queue_manager: QueueManager):
        self.clients = clients
        self.db = db
        self.queue_manager = queue_manager
        self.is_running = True
        logger.info(f"Scanner initialized with {len(clients)} client(s)")
    
    def find_client_for_channel(self, chat_id: int) -> Optional[Client]:
        """Find a client that can access the channel"""
        for client in self.clients:
            try:
                # Will be checked asynchronously
                return client
            except:
                continue
        return None
    
    async def verify_channel_access(self, client: Client, chat_id: int) -> bool:
        """Verify client access to channel"""
        try:
            await client.get_chat(chat_id)
            return True
        except Exception as e:
            logger.debug(f"Client cannot access channel {chat_id}: {e}")
            return False
    
    async def scan_history(self):
        """Scan source channels for existing files"""
        logger.info("🔄 Starting history scan...")
        
        for source_str, dest_str in zip(Config.SOURCE_CHANNELS, Config.DESTINATION_CHANNELS):
            try:
                source_chat_id = int(source_str.strip())
                destination_chat_id = int(dest_str.strip())
            except ValueError as e:
                logger.error(f"❌ Invalid channel ID: {source_str} or {dest_str}")
                continue
            
            # Find accessible client
            scanner_client = None
            for client in self.clients:
                if await self.verify_channel_access(client, source_chat_id):
                    scanner_client = client
                    logger.info(f"✅ Using client for source: {source_chat_id}")
                    break
            
            if not scanner_client:
                logger.error(f"❌ No client can access source: {source_chat_id}")
                continue
            
            # Get last processed message ID
            progress = self.db.get_progress(source_chat_id)
            last_message_id = progress['last_message_id'] if progress else 0
            
            logger.info(f"📥 Scanning channel {source_chat_id} from message {last_message_id}")
            
            batch_size = Config.BATCH_SIZE
            current_offset = last_message_id
            total_queued = 0
            empty_batches = 0
            max_empty_batches = 3  # Stop after 3 empty batches
            
            while self.is_running:
                try:
                    # Get batch of messages
                    messages = await scanner_client.get_chat_history(
                        chat_id=source_chat_id,
                        offset_id=current_offset,
                        limit=batch_size
                    )
                    
                    if not messages:
                        empty_batches += 1
                        logger.info(f"Empty batch for channel {source_chat_id} (empty count: {empty_batches})")
                        
                        if empty_batches >= max_empty_batches:
                            logger.info(f"✅ History scan complete for channel {source_chat_id}")
                            break
                        
                        await asyncio.sleep(5)
                        continue
                    
                    # Reset empty batch counter
                    empty_batches = 0
                    
                    queued_count = 0
                    for message in messages:
                        # Check if message has supported media
                        if self._has_supported_media(message):
                            # Check for duplicates
                            if not self.db.is_duplicate(source_chat_id, message.id):
                                file_info = {
                                    'source_chat_id': source_chat_id,
                                    'source_message_id': message.id,
                                    'destination_chat_id': destination_chat_id,
                                    'has_media': True,
                                    'media_type': self._get_media_type(message)
                                }
                                
                                # Add to database and queue
                                self.db.add_file_record(
                                    source_chat_id,
                                    message.id,
                                    destination_chat_id
                                )
                                await self.queue_manager.add_file(file_info)
                                queued_count += 1
                                total_queued += 1
                    
                    # Save progress
                    last_message = messages[-1]
                    current_offset = last_message.id
                    self.db.save_progress(
                        source_chat_id,
                        current_offset,
                        queued_count
                    )
                    
                    logger.info(f"📊 Scanned {len(messages)} messages from {source_chat_id}, queued {queued_count} files")
                    
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Scan error for channel {source_chat_id}: {e}")
                    await asyncio.sleep(5)
            
            logger.info(f"✅ Total queued from {source_chat_id}: {total_queued} files")
    
    def _has_supported_media(self, message) -> bool:
        """Check if message has supported media"""
        return (
            message.video is not None or 
            message.document is not None or 
            message.audio is not None or 
            message.photo is not None or 
            message.voice is not None or 
            message.video_note is not None
        )
    
    def _get_media_type(self, message) -> str:
        """Get media type from message"""
        if message.video:
            return 'video'
        elif message.document:
            return 'document'
        elif message.audio:
            return 'audio'
        elif message.photo:
            return 'photo'
        elif message.voice:
            return 'voice'
        elif message.video_note:
            return 'video_note'
        return 'unknown'
    
    async def listen_new_messages(self):
        """Listen for new messages in source channels"""
        logger.info("👂 Starting new message listeners...")
        
        # Create message handlers for each client
        for idx, client in enumerate(self.clients, 1):
            @client.on_message()
            async def handle_new_message(client, message):
                if not self.is_running:
                    return
                
                try:
                    source_chat_id = str(message.chat.id)
                    
                    # Check if this is a source channel
                    if source_chat_id not in Config.SOURCE_CHANNELS:
                        return
                    
                    # Get destination channel
                    index = Config.SOURCE_CHANNELS.index(source_chat_id)
                    destination_chat_id = Config.DESTINATION_CHANNELS[index]
                    
                    # Check for supported media
                    if self._has_supported_media(message):
                        # Check for duplicates
                        if not self.db.is_duplicate(source_chat_id, message.id):
                            file_info = {
                                'source_chat_id': source_chat_id,
                                'source_message_id': message.id,
                                'destination_chat_id': destination_chat_id,
                                'has_media': True,
                                'media_type': self._get_media_type(message)
                            }
                            
                            # Add to database and queue
                            self.db.add_file_record(
                                int(source_chat_id),
                                message.id,
                                int(destination_chat_id)
                            )
                            await self.queue_manager.add_file(file_info)
                            
                            logger.info(f"🆕 New message queued from {source_chat_id}: {message.id}")
                
                except Exception as e:
                    logger.error(f"❌ New message handler error: {e}")
        
        # Keep the listener running
        while self.is_running:
            await asyncio.sleep(1)
    
    def stop(self):
        """Stop scanner"""
        self.is_running = False
        logger.info("Scanner stopped")
