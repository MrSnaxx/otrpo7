import aiohttp
import asyncio
from aio_pika import connect_robust, Message
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
from dotenv import load_dotenv

# Загрузка параметров из .env
load_dotenv('par.env')
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME")
TIMEOUT = int(os.getenv("TIMEOUT", 10))

async def fetch(session, url):
    """Загрузка страницы по URL."""
    try:
        async with session.get(url, ssl=False) as response:
            response.raise_for_status()
            print(f"[INFO] Страница загружена: {url}")
            return await response.text()
    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке {url}: {e}")
        return None

async def extract_links(html, base_url):
    """Извлечение всех внутренних ссылок и медиа."""
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for tag in soup.find_all(["a", "img", "video", "audio"], href=True, src=True):
        href = tag.get("href") or tag.get("src")
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            links.add(full_url)
            print(f"Найдена ссылка: {tag.get_text(strip=True)} - {full_url}")
    return links

async def consume():
    """Основной процесс потребителя."""
    connection = await connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with queue.iterator(timeout=TIMEOUT) as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                url = message.body.decode()
                                print(f"[QUEUE] Обрабатывается: {url}")
                                html = await fetch(session, url)
                                if not html:
                                    continue
                                links = await extract_links(html, url)

                                for link in links:
                                    await channel.default_exchange.publish(
                                        Message(link.encode()),
                                        routing_key=queue.name,
                                    )
                                    print(f"[QUEUE] Добавлено: {link}")
                except asyncio.TimeoutError:
                    print("[INFO] Очередь пуста. Завершение работы.")
                    break
                except Exception as e:
                    print(f"[ERROR] Произошла ошибка: {e}")
                    break

if __name__ == "__main__":
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        print("\n[INFO] Завершение программы...")
