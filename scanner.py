import asyncio
from pyrogram import Client
import logging
from config import Config

logger = logging.getLogger(__name__)


async def _collect_with_timeout(async_gen, timeout):
    """get_chat_history() jaisa async generator consume karta hai, ek
    overall timeout ke saath. Bina isके, agar Telegram/network side kabhi
    hang ho jaaye (jawab hi na aaye), scan_history() ka poora task
    HAMESHA KE LIYE silently ruk jaata — na koi error print hota, na
    retry hota, bas 'Scanned ...' log line aana band ho jaata."""
    async def _drain():
        result = []
        async for item in async_gen:
            result.append(item)
        return result
    return await asyncio.wait_for(_drain(), timeout=timeout)

class Scanner:
    def __init__(self, clients, db, queue_manager):
        self.clients = clients
        self.db = db
        self.queue_manager = queue_manager
        self.is_running = True
        # Jab tak kisi channel ka poora historical backlog scan complete
        # na ho jaaye, us channel ke naye/live messages 'live_pending'
        # status me hold hote hain (claim_batch() unhe nahi dekhta) —
        # taaki purana backlog kabhi bhi live messages se peeche na rahe.
        self.backlog_complete = set()
    
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
                # History scan possible hi nahi hai is channel ke liye —
                # live messages ko hamesha ke liye 'live_pending' me hold
                # karna behtar nahi hai; is channel ke liye seedha 'complete'
                # maan lo taaki kam se kam live copying to chal sake.
                self.backlog_complete.add(source_chat_id)
                await asyncio.to_thread(self.db.promote_live_pending, source_chat_id)
                continue
            
            progress = await asyncio.to_thread(self.db.get_progress, source_chat_id)
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
                await asyncio.to_thread(
                    self.db.save_progress, source_chat_id, last_message_id, 0,
                    total_messages=total_messages
                )

            logger.info(f"Scanning {source_chat_id} from {last_message_id} (total ~{total_messages})")

            # BACKLOG_COMPLETE detection ke liye "messages empty aa jaaye"
            # wala signal bharose layak nahi hai agar channel bahut active
            # hai — scan chalte-chalte bhi naye messages aate rehte hain,
            # isliye get_chat_history kabhi bhi truly empty return nahi
            # karta aur backlog_complete kabhi trigger hi nahi hota (naye
            # messages hamesha ke liye 'live_pending' me atke reh jaate).
            # Isliye ek FIXED target lo — is run ke shuru me channel ka
            # latest message ID (stale cached total_messages se alag, jo
            # sirf progress-display ke liye hai aur purane runs se stale
            # ho chuka hota hai). Jaise hi scanner is target tak pahunch
            # jaaye, backlog complete maan lo — us target ke baad wale
            # sab kuch already live-listener handle kar raha hai.
            backlog_target_id = None
            try:
                latest_list = await _collect_with_timeout(
                    scanner_client.get_chat_history(chat_id=source_chat_id, limit=1),
                    timeout=30
                )
                if latest_list:
                    backlog_target_id = latest_list[0].id
            except asyncio.TimeoutError:
                logger.warning(
                    f"Backlog target fetch timed out (30s) for {source_chat_id} — "
                    f"will fall back to 'messages empty' detection"
                )
            except Exception as e:
                logger.warning(f"Could not fetch backlog target for {source_chat_id}: {e}")

            if backlog_target_id is not None and last_message_id >= backlog_target_id:
                # Is run ke shuru hote hi backlog already caught-up nikla
                # (e.g. pichhle deploy me hi khatam ho gaya tha).
                self.backlog_complete.add(source_chat_id)
                await asyncio.to_thread(self.db.promote_live_pending, source_chat_id)
                logger.info(f"Backlog already complete for {source_chat_id} (target {backlog_target_id})")
            
            current_offset = last_message_id
            total_queued = 0
            
            while self.is_running:
                try:
                    # Pyrogram 2.x me get_chat_history ek async generator hai,
                    # coroutine nahi — isliye await nahi, async for se iterate karna hai.
                    # 60s timeout: bina isके, ek hung/slow Telegram call is
                    # poore scan_history() task ko HAMESHA KE LIYE silently
                    # rok deta — koi error/log bhi nahi aata, sirf "Scanned
                    # ..." lines aana band ho jaata (exactly jo pehle observe
                    # hua tha).
                    try:
                        messages = await _collect_with_timeout(
                            scanner_client.get_chat_history(
                                chat_id=source_chat_id,
                                offset_id=current_offset,
                                limit=Config.BATCH_SIZE
                            ),
                            timeout=60
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Scan batch TIMED OUT (60s) for {source_chat_id} at offset "
                            f"{current_offset} — retrying"
                        )
                        await asyncio.sleep(5)
                        continue
                    
                    if not messages:
                        if source_chat_id not in self.backlog_complete:
                            self.backlog_complete.add(source_chat_id)
                            await asyncio.to_thread(self.db.promote_live_pending, source_chat_id)
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
                            is_new = await asyncio.to_thread(
                                self.db.add_file_record, source_chat_id, message.id, destination_chat_id
                            )
                            if is_new:
                                file_info = {
                                    'source_chat_id': source_chat_id,
                                    'source_message_id': message.id,
                                    'destination_chat_id': destination_chat_id,
                                    'has_media': True
                                }
                                ok = await self.queue_manager.add_file(file_info)
                                if ok:
                                    queued_count += 1
                                    total_queued += 1
                                else:
                                    logger.error(
                                        f"add_to_queue FAILED for {message.id} — files me "
                                        f"'pending' ban gaya par queue collection me nahi gaya "
                                        f"(exact wajah upar 'Add to queue error' line me dekho)"
                                    )
                    
                    if messages:
                        current_offset = messages[-1].id
                        await asyncio.to_thread(
                            self.db.save_progress,
                            source_chat_id, current_offset, len(messages),
                            batch_type_counts=type_counts
                        )

                    reached_target = (
                        backlog_target_id is not None and
                        current_offset >= backlog_target_id and
                        source_chat_id not in self.backlog_complete
                    )
                    if reached_target:
                        self.backlog_complete.add(source_chat_id)
                        await asyncio.to_thread(self.db.promote_live_pending, source_chat_id)
                    
                    remaining_pct = None
                    if total_messages:
                        scanned = max(total_messages - current_offset, 0)
                        remaining_pct = round(100 * scanned / total_messages, 1)
                    logger.info(
                        f"Scanned {len(messages)} (index {current_offset}/{total_messages}"
                        f"{f', {remaining_pct}%' if remaining_pct is not None else ''}), "
                        f"queued {queued_count}, types: {type_counts}"
                    )

                    if reached_target:
                        # Is run ke shuru me fix kiya gaya target reach ho
                        # gaya — backlog scan (is channel ke liye) is run
                        # me ab complete hai. Aage jitne bhi naye messages
                        # aayenge, wo already live-listener handle karega;
                        # dobara-dobara wahi range re-scan karte rehna
                        # faltu hai (aur duplicate-check overhead badhata).
                        break

                    await asyncio.sleep(0.2 if queued_count == 0 else 0.4)
                    
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
                    # int me normalize kiya — scan_history bhi int hi
                    # store karta hai; consistent type ke bina
                    # backlog_complete membership check aur per-channel
                    # stats/queries silently mismatch ho sakte the.
                    source_chat_id_int = int(source_chat_id)
                    destination_chat_id_int = int(destination_chat_id)
                    
                    if self._has_media(message):
                        # add_file_record() ka return value hi race-free
                        # signal hai — 4 accounts isi channel ke members
                        # hain, isliye ek hi naya message har account ke
                        # apne on_message handler me deliver hota hai; is
                        # atomic upsert ke bina har account "duplicate
                        # nahi hai" dekh ke aage badh jaata (isi se log me
                        # "New message queued" 3-4 baar print ho raha tha).
                        is_new = await asyncio.to_thread(
                            self.db.add_file_record, source_chat_id_int, message.id, destination_chat_id_int
                        )
                        if is_new:
                            file_info = {
                                'source_chat_id': source_chat_id_int,
                                'source_message_id': message.id,
                                'destination_chat_id': destination_chat_id_int,
                                'has_media': True
                            }
                            # Is channel ka historical backlog abhi tak
                            # scan complete nahi hua to naya message
                            # 'live_pending' me hold karo — claim_batch()
                            # ise tab tak nahi uthayega jab tak backlog
                            # fully queued/accounted-for na ho jaaye
                            # (strict old-before-new priority).
                            backlog_done = source_chat_id_int in self.backlog_complete
                            status = 'queued' if backlog_done else 'live_pending'
                            ok = await self.queue_manager.add_file(file_info, status=status)
                            if ok:
                                if backlog_done:
                                    logger.info(f"New message queued: {message.id}")
                                else:
                                    logger.info(
                                        f"New message held (backlog scan in progress): {message.id}"
                                    )
                            else:
                                logger.error(
                                    f"add_to_queue FAILED for {message.id} — files me "
                                    f"'pending' ban gaya par queue collection me nahi gaya "
                                    f"(exact wajah upar 'Add to queue error' line me dekho)"
                                )
                
                except Exception as e:
                    logger.error(f"Handler error: {e}")
        
        while self.is_running:
            await asyncio.sleep(1)
    
    def stop(self):
        self.is_running = False
