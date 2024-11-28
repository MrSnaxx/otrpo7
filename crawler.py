import sys
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
    """Извлечение всех внутренних ссылок."""
    soup = BeautifulSoup(html, "lxml")
    links = set()

    # Обработка тегов <a> с атрибутом href
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            links.add(full_url)
            print(f"Найдена ссылка: {tag.get_text(strip=True)} - {full_url}")

    # Обработка тегов <img>, <video>, <audio> с атрибутом src
    for tag in soup.find_all(["img", "video", "audio"], src=True):
        src = tag.get("src")
        full_url = urljoin(base_url, src)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            links.add(full_url)
            print(f"Найдена медиа-ссылка ({tag.name}): {full_url}")

    return links

async def main():
    """Основной процесс."""
    if len(sys.argv) != 2:
        print("Использование: python crawler.py <URL>")
        return

    url = sys.argv[1]

    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
        if not html:
            return
        links = await extract_links(html, url)

    connection = await connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        for link in links:
            await channel.default_exchange.publish(
                Message(link.encode()),
                routing_key=queue.name,
            )
            print(f"[QUEUE] Добавлено: {link}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Завершение программы...")
