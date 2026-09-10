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

LINK_URL = "https://t.me/skadminrobot"


def clean_caption(text):
    """@mentions hata deta hai (jaise @allhty4ku, @skilldev) aur unhe hatane
    se bache extra blank lines/spaces ko bhi saaf karta hai. Original
    formatting (bold/italic waghera) preserve nahi hoti kyunki hum plain
    text caption bhejte hain — agar wo bhi chahiye ho to bataana."""
    if not text:
        return text
    cleaned = MENTION_PATTERN.sub('', text)
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = '\n'.join(line.strip() for line in cleaned.split('\n'))
    return cleaned.strip()


def get_filename(message):
    """Media message se uska asli filename nikalta hai (document/video/audio/
    animation). Photo jaise media types me koi filename hota hi nahi, to
    None return hota hai."""
    for attr in ('document', 'video', 'audio', 'animation', 'voice', 'video_note'):
        media = getattr(message, attr, None)
        if media is not None and getattr(media, 'file_name', None):
            return media.file_name
    return None


def build_caption(message):
    """Caption banata hai jisme filename ek clickable hyperlink hota hai
    (tap karne par LINK_URL khulta hai), aur uske neeche original caption
    (mentions clean karke) agar ho to add ho jaata hai. Filename khud NAHI
    badalta — Telegram par attach hui file ka असली file_name copy_message
    ke through automatically preserve rehta hai, ye sirf visible caption
    text hai."""
    filename = get_filename(message)
    cleaned_caption = clean_caption(message.caption)

    if filename:
        hyperlinked_name = f'<a href="{LINK_URL}">{html.escape(filename)}</a>'
        if cleaned_caption:
            return f"{hyperlinked_name}\n\n{html.escape(cleaned_caption)}"
        return hyperlinked_name

    # Filename na ho (jaise plain photo) to purana behaviour: sirf cleaned caption
    return html.escape(cleaned_caption) if cleaned_caption else ""


class Copier:
    def __init__(self):
        self.copy_mode = Config.COPY_MODE
        self.retry_limit = Config.RETRY_LIMIT
        self.db = Database()  # singleton — Database() har jagah same instance deta hai
    
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
                    # copy_message file_id se hi resend karta hai (server-side
                    # copy) — download/re-encode nahi hota, isliye original
                    # quality bilkul same rehti hai, aur attached file ka asli
                    # filename bhi automatically preserve rehta hai. Caption
                    # ko override karke usme filename ko clickable link banate
                    # hain aur mentions clean karte hain.
                    source_msg = await client.get_messages(source_chat_id, source_message_id)
                    final_caption = build_caption(source_msg)

                    sent = await client.copy_message(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                        caption=final_caption if final_caption else "",
                        parse_mode=ParseMode.HTML
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
                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(2)
                    continue
                return None
        
        return None
