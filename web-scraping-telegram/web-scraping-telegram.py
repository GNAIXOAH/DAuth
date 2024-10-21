from datetime import datetime, timezone
import time
import openpyxl
import asyncio
from telethon import TelegramClient, errors

# Setup Google Sheets with OpenPyXL
workbook = openpyxl.Workbook()
sheet = workbook.active

# Define titles
titles = ['Scraping ID', 'Group', 'Author ID', 'Content', 'Date', 'Message ID', 'Author', 'Views', 'Reactions', 'Shares', 'Media']
sheet.append(titles)  # This will add titles as the first row in the worksheet

# Save the workbook initially to ensure the file is created
workbook.save('Scraped_Telegram_Data.xlsx')

# Setup Telegram client
username = ''  # your Telegram username
api_id = ''  # your api_id
api_hash = ''  # your api_hash

# Scraping settings
channel = ''  # channel or group to scrape (without https://t.me/)
start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
key_search = ''  # keyword to search (optional)

# Function to get the last processed message ID
def get_last_processed_message_id():
    try:
        with open('last_processed_message_id.txt', 'r') as file:
            return int(file.read().strip())
    except (FileNotFoundError, ValueError):
        return None

# Function to save the last processed message ID
def save_last_processed_message_id(message_id):
    with open('last_processed_message_id.txt', 'w') as file:
        file.write(str(message_id))

# Function to scrape Telegram messages
async def scrape_telegram():
    last_processed_message_id = get_last_processed_message_id()
    if last_processed_message_id is None:
        last_processed_message_id = 0
    index = 2  # Start from the second row since the first row contains titles

    async with TelegramClient(username, api_id, api_hash) as client:
        await client.start()
        # Setting iter_messages with min_id
        message_iter = client.iter_messages(channel, search=key_search, min_id=last_processed_message_id)

        async for message in message_iter:
            if start_date < message.date < end_date:
                try:
                    url = f'https://t.me/{channel}/{message.id}' if message.media else 'no media'
                    emoji_string = " ".join(
                        f"{reaction.reaction if hasattr(reaction.reaction, 'emoticon') else 'Custom Emoji'} {reaction.count}"
                        for reaction in (message.reactions.results if message.reactions else [])
                    )
                    
                    # Append row to sheet
                    content = [
                        f'#ID{index-1:05}', channel, message.sender_id or 'Unknown', message.text or 'No text', 
                        message.date.strftime('%Y-%m-%d %H:%M:%S'), message.id, message.post_author or 'Unknown', 
                        message.views or 0, emoji_string or 'No reactions', message.forwards or 0, url
                    ]
                    sheet.append(content)

                    # Save the workbook after processing each message
                    workbook.save('Scraped_Telegram_Data.xlsx')

                    index += 1
                    time.sleep(1)  # Be cautious with sleep time to avoid rate limits

                    # Save the last processed message ID
                    save_last_processed_message_id(message.id)

                except errors.RPCError as e:
                    pass

# Run the scraping function
asyncio.run(scrape_telegram())
