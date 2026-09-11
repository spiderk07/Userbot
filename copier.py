from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.enums import ParseMode
import asyncio
import logging
import re
import html
from config import Config
from database import Database

logger = logging.getLogger(__name__)

# Telegram username mention pattern: @ ke baad 4-32 alnum/underscore chars
MENTION_PATTERN = re.compile(r'@\w{3,32}')

LINK_URL = "https://t.me/+0iMDc7jCLThkNmRl"


def clean_caption(text):
    """@mentions hata deta hai aur extra blank lines/spaces saaf karta hai."""
    if not text:
        return text
    cleaned = MENTION_PATTERN.sub('', text)
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = '\n'.join(line.strip() for line in cleaned.split('\n'))
    return cleaned.strip()


def get_filename(message):
    """Media message se uska asli filename nikalta hai."""
    for attr in ('document', 'video', 'audio', 'animation', 'voice', 'video_note'):
        media = getattr(message, attr, None)
        if media is not None and getattr(media, 'file_name', None):
            return media.file_name
    return None


def build_caption_from_filename(filename):
    """Same as build_caption() lekin filename string se direct kaam karta hai
    — koi extra Telegram API call ki zaroorat nahi. Scanner ab file discover
    hote hi filename cache kar deta hai, isliye copy ke time doosri
    get_messages() call nahi lagti (jo pehle har file ke liye ek poori extra
    round-trip add kar rahi thi aur throughput ko slow kar rahi thi)."""
    if filename:
        hyperlinked_name = f'<a href="{LINK_URL}">{html.escape(filename)}</a>'
        return f"Uploaded by : @sk4film\n\n{hyperlinked_name}"
    return ""


def build_caption(message):
    """Fallback path — jab cached filename available na ho (jaise is update
    se pehle scan hue purane queue items)."""
    return build_caption_from_filename(get_filename(message))


class Copier:
    def __init__(self):
        self.copy_mode = Config.COPY_MODE
        self.retry_limit = Config.RETRY_LIMIT
        self.db = Database()  # singleton — Database() har jagah same instance deta hai

    async def _find_ghost_send(self, client, destination_chat_id, filename):
        """TimeoutError/network-error ke baad retry se PEHLE destination
        channel ke last kuch messages check karta hai — agar wahi filename
        already wahan mil jaaye, matlab pichla attempt Telegram server tak
        pahunch chuka tha, sirf response client tak wapas nahi aaya (network
        blip). Aisi 'ghost success' state me blind retry karne se WAKAI ek
        doosri copy chali jaati thi — ye check usko rokta hai."""
        if not filename:
            return None
        try:
            async for msg in client.get_chat_history(destination_chat_id, limit=8):
                if get_filename(msg) == filename:
                    return msg.id
        except Exception as e:
            logger.warning(f"Ghost-send check failed: {e}")
        return None

    async def copy_message(self, client, file_info):
        source_chat_id = int(file_info['source_chat_id'])
        source_message_id = int(file_info['source_message_id'])
        destination_chat_id = int(file_info['destination_chat_id'])

        # Duplicate-safety: agar ye message kisi wajah se pehle hi
        # 'completed' likha ja chuka hai (jaise ek race me doosra worker
        # abhi-abhi maar chuka), to dobara Telegram par bhejo hi mat.
        existing = self.db.files.find_one({
            'source_chat_id': source_chat_id,
            'source_message_id': source_message_id
        })
        if existing and existing.get('status') == 'completed':
            logger.info(f"Skip {source_message_id}: already completed (duplicate-safety check)")
            return existing.get('destination_message_id')

        # Retry-safety ke liye filename pehle hi nikaal lo (loop ke bahar) —
        # ghost-send check aur caption dono isi ko use karenge.
        filename = file_info.get('filename')

        for attempt in range(self.retry_limit):
            try:
                if self.copy_mode == 'forward':
                    # Native Telegram forward — caption edit yahan possible
                    # nahi hai (poora original message forward hota hai,
                    # "Forwarded from" tag ke saath).
                    sent = await client.forward_messages(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_ids=source_message_id
                    )
                else:
                    # Scanner ab filename already file_info me cache kar deta
                    # hai (scan/listen ke time hi padh liya jaata hai), isliye
                    # normally yahan get_messages() ki extra call NAHI lagti —
                    # ye ek poori network round-trip per-file bachaata hai aur
                    # copy speed badhata hai. Sirf purane (is update se pehle
                    # queue hue) items ke liye fallback lagti hai.
                    if 'filename' in file_info:
                        final_caption = build_caption_from_filename(file_info.get('filename'))
                    else:
                        source_msg = await client.get_messages(source_chat_id, source_message_id)
                        filename = get_filename(source_msg)
                        final_caption = build_caption_from_filename(filename)

                    if final_caption:
                        sent = await client.copy_message(
                            chat_id=destination_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=source_message_id,
                            caption=final_caption,
                            parse_mode=ParseMode.HTML
                        )
                    else:
                        # Filename na ho (jaise plain photo) to original caption
                        # override kiye bina copy karo.
                        sent = await client.copy_message(
                            chat_id=destination_chat_id,
                            from_chat_id=source_chat_id,
                            message_id=source_message_id
                        )

                if sent:
                    # Turant DB me likho — worker ke mark_completed() call
                    # tak wait karne se crash window badh jaata (agar isi
                    # beech restart ho jaye to recover_stuck_processing()
                    # ise wapas 'queued' bana kar DOBARA bhej deta —
                    # duplicate forward). Yahan turant likhne se ye window
                    # bas is ek DB write jitna reh jaata hai.
                    self.db.update_status(
                        source_chat_id, source_message_id, 'completed',
                        destination_message_id=sent.id
                    )
                    return sent.id
                return None

            except FloodWait as e:
                logger.warning(f"FloodWait: {e.value}s")
                await asyncio.sleep(e.value)
                continue

            except Exception as e:
                logger.error(f"Copy error: {e}")

                # Timeout/network-error ke baad blind retry karne se pehle
                # confirm karo ki pichla attempt secretly succeed to nahi
                # ho gaya (Telegram ne process kar diya ho, response hi
                # miss hua ho). Agar mil jaaye, use hi 'completed' maan lo —
                # dobara bhejne ki zaroorat nahi.
                ghost_id = await self._find_ghost_send(client, destination_chat_id, filename)
                if ghost_id:
                    logger.info(
                        f"Skip {source_message_id}: found matching send already in "
                        f"destination after {type(e).__name__} (ghost-send, no retry needed)"
                    )
                    self.db.update_status(
                        source_chat_id, source_message_id, 'completed',
                        destination_message_id=ghost_id
                    )
                    return ghost_id

                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(2)
                    continue
                return None

        return None
