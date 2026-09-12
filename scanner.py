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
        # Sirf video aur document forward karna hai — baaki types (photo, audio, voice,
        # video_note, animation) yahan intentionally skip kiye gaye hain.
        return (
            message.video is not None or 
            message.document is not None
        )
    
    def _media_type(self, message):
        # Debug ke liye — batata hai is message me kaunsa type detect hua (ya kuch nahi)
        if getattr(message, 'empty', False):
            return 'deleted/empty'
        if message.video is not None:
            return 'video'
        if message.document is not None:
            return 'document'
        if message.audio is not None:
            return 'audio'
        if message.photo is not None:
            return 'photo'
        if message.voice is not None:
            return 'voice'
        if message.video_note is not None:
            return 'video_note'
        if message.animation is not None:
            return 'animation'
        if message.text is not None:
            return 'text'
        if message.sticker is not None:
            return 'sticker'
        if message.service is not None:
            return 'service'
        # Agar Telegram ne koi media bheja hai lekin library ke schema me
        # match nahi hua (purani library / naya media type), to alag se
        # flag karo — 'other/empty' me chhupne se galat pata chalta tha
        # ki channel me kuch nahi hai, jab asal me parsing fail ho rahi thi.
        media = getattr(message, 'media', None)
        if media is not None:
            return f'unsupported_media({getattr(media, "value", media)})'
        return 'other/empty'
    
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

            # Channel ka total message count pata karne ke liye latest
            # message id nikalo (sirf ek dafa — jab tak progress me already
            # save nahi hai). Ye "index" ka denominator ban'ta hai.
            total_messages = progress.get('total_messages') if progress else None
            if not total_messages:
                try:
                    async for latest in scanner_client.get_chat_history(
                        chat_id=source_chat_id, limit=1
                    ):
                        total_messages = latest.id
                        break
                except Exception as e:
                    logger.warning(f"Could not fetch total message count for {source_chat_id}: {e}")
                    total_messages = None
                self.db.save_progress(source_chat_id, last_message_id, 0, total_messages=total_messages)

            logger.info(f"Scanning {source_chat_id} from {last_message_id} (total ~{total_messages})")
            
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
                    type_counts = {}
                    for message in messages:
                        mtype = self._media_type(message)
                        type_counts[mtype] = type_counts.get(mtype, 0) + 1
                        if self._has_media(message):
                            # add_file_record() ka return value hi source of
                            # truth hai (ek atomic upsert) — is_duplicate() ka
                            # pehle se check karna race-prone tha jab kai
                            # clients/threads same message discover karte hain.
                            is_new = self.db.add_file_record(source_chat_id, message.id, destination_chat_id)
                            if is_new:
                                file_info = {
                                    'source_chat_id': source_chat_id,
                                    'source_message_id': message.id,
                                    'destination_chat_id': destination_chat_id,
                                    'has_media': True
                                }
                                await self.queue_manager.add_file(file_info)
                                queued_count += 1
                                total_queued += 1
                    
                    if messages:
                        current_offset = messages[-1].id
                        self.db.save_progress(
                            source_chat_id, current_offset, len(messages),
                            batch_type_counts=type_counts
                        )
                    
                    remaining_pct = None
                    if total_messages:
                        scanned = max(total_messages - current_offset, 0)
                        remaining_pct = round(100 * scanned / total_messages, 1)
                    logger.info(
                        f"Scanned {len(messages)} (index {current_offset}/{total_messages}"
                        f"{f', {remaining_pct}%' if remaining_pct is not None else ''}), "
                        f"queued {queued_count}, types: {type_counts}"
                    )
                    await asyncio.sleep(0.5 if queued_count == 0 else 1)
                    
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
                        # add_file_record() ka return value hi race-free
                        # signal hai — 4 accounts isi channel ke members
                        # hain, isliye ek hi naya message har account ke
                        # apne on_message handler me deliver hota hai; is
                        # atomic upsert ke bina har account "duplicate
                        # nahi hai" dekh ke aage badh jaata (isi se log me
                        # "New message queued" 3-4 baar print ho raha tha).
                        is_new = self.db.add_file_record(int(source_chat_id), message.id, int(destination_chat_id))
                        if is_new:
                            file_info = {
                                'source_chat_id': source_chat_id,
                                'source_message_id': message.id,
                                'destination_chat_id': destination_chat_id,
                                'has_media': True
                            }
                            await self.queue_manager.add_file(file_info)
                            logger.info(f"New message queued: {message.id}")
                
                except Exception as e:
                    logger.error(f"Handler error: {e}")
        
        while self.is_running:
            await asyncio.sleep(1)
    
    def stop(self):
        self.is_running = False
