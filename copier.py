from pyrogram import Client
from pyrogram.errors import FloodWait
import asyncio
import logging
import re
from config import Config

logger = logging.getLogger(__name__)

# Telegram username mention pattern: @ ke baad 4-32 alnum/underscore chars
MENTION_PATTERN = re.compile(r'@\w{3,32}')


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


class Copier:
    def __init__(self):
        self.copy_mode = Config.COPY_MODE
        self.retry_limit = Config.RETRY_LIMIT
    
    async def copy_message(self, client, file_info):
        source_chat_id = int(file_info['source_chat_id'])
        source_message_id = int(file_info['source_message_id'])
        destination_chat_id = int(file_info['destination_chat_id'])
        
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
                    # quality bilkul same rehti hai. Caption ko explicitly
                    # override karke mentions clean karte hain.
                    source_msg = await client.get_messages(source_chat_id, source_message_id)
                    cleaned_caption = clean_caption(source_msg.caption)

                    sent = await client.copy_message(
                        chat_id=destination_chat_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                        caption=cleaned_caption if cleaned_caption else "",
                        parse_mode=None
                    )
                
                if sent:
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
