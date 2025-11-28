#!/usr/bin/env python3
"""
Быстрое обогащение Instagram URLs - асинхронный батч-процессинг
Максимальная скорость с сохранением прогресса
"""

import asyncio
import aiohttp
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
RAPIDAPI_HOST = "google-search116.p.rapidapi.com"

# Настройки производительности
BATCH_SIZE = 5  # Запросов одновременно
SAVE_EVERY = 10  # Сохранять каждые N лидов
MAX_RETRIES = 2  # Повторных попыток при ошибке


class FastInstagramFinder:
    """Быстрый поиск Instagram профилей через async"""

    def __init__(self, api_key: str, max_concurrent: int = 5):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.total_requests = 0
        self.successful = 0
        self.failed = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def extract_instagram_username(self, url: str):
        """Извлечь username из URL"""
        pattern = r'instagram\.com/([a-zA-Z0-9_.]+)'
        match = re.search(pattern, url)
        if match:
            username = match.group(1).rstrip('/')
            if username not in ['reel', 'p', 'tv', 'stories', 'explore', 'reels']:
                return username
        return None

    async def search_google(self, query: str, retry: int = 0):
        """Асинхронный поиск через Google"""
        async with self.semaphore:
            try:
                self.total_requests += 1

                headers = {
                    "x-rapidapi-host": RAPIDAPI_HOST,
                    "x-rapidapi-key": self.api_key
                }

                params = {"query": query, "limit": 10}

                async with self.session.get(
                    f"https://{RAPIDAPI_HOST}",
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:

                    if response.status == 429:  # Rate limit
                        print(f"\n!!! RATE LIMIT! Zaprosov ispolzovano: {self.total_requests}")
                        return None

                    if response.status != 200:
                        if retry < MAX_RETRIES:
                            await asyncio.sleep(1)
                            return await self.search_google(query, retry + 1)
                        return None

                    data = await response.json()
                    return data

            except asyncio.TimeoutError:
                if retry < MAX_RETRIES:
                    await asyncio.sleep(1)
                    return await self.search_google(query, retry + 1)
                return None
            except Exception as e:
                print(f"\n  ! Oshibka: {e}")
                return None

    async def find_instagram_url(self, name: str, email: str):
        """Найти Instagram URL для лида"""
        # Стратегия: 1 запрос - по email (самый точный)
        # Если не найдено - 2й запрос по имени

        queries = [
            f'"{email}" instagram',
            f'"{name}" instagram influencer'
        ]

        for query in queries:
            result = await self.search_google(query)

            if result is None:  # Rate limit или критическая ошибка
                return None

            if not result.get('results'):
                await asyncio.sleep(0.2)  # Маленькая пауза между попытками
                continue

            # Ищем Instagram URL
            for item in result['results']:
                url = item.get('url', '')
                if '/reel/' in url or '/p/' in url or '/tv/' in url:
                    continue

                username = self.extract_instagram_username(url)
                if username:
                    self.successful += 1
                    return f"https://www.instagram.com/{username}/"

            await asyncio.sleep(0.2)

        self.failed += 1
        return None

    async def process_batch(self, leads_batch):
        """Обработать батч лидов"""
        tasks = []
        for idx, lead in leads_batch:
            name = lead.get('Person - Name', '')
            email = lead.get('Person - Email - Work', '')

            task = self.find_instagram_url(name, email)
            tasks.append((idx, task))

        results = []
        for idx, task in tasks:
            url = await task
            results.append((idx, url))

        return results


async def fast_enrich(input_csv: str, output_csv: str, max_leads: int = 100):
    """
    Быстрое обогащение с автосохранением

    Args:
        input_csv: Входной CSV
        output_csv: Выходной CSV
        max_leads: Максимум лидов для обработки
    """
    print(f"\n>> BYSTROE OBOGASHENIE Instagram URLs")
    print(f"Fail: {input_csv}")
    print(f"Obrabotka: {max_leads} lidov")
    print(f"Batchi po {BATCH_SIZE} zaprosov")
    print(f"Avtosohranenie kazhdye {SAVE_EVERY} lidov\n")

    # Загрузка CSV
    df = pd.read_csv(input_csv, encoding='utf-8-sig')

    # Ограничение
    df = df.head(max_leads)

    # Добавляем колонки
    if 'Instagram URL' not in df.columns:
        df['Instagram URL'] = ''
    if 'Status' not in df.columns:
        df['Status'] = ''

    # Асинхронная обработка
    async with FastInstagramFinder(RAPIDAPI_KEY, max_concurrent=BATCH_SIZE) as finder:

        # Создаем батчи
        total = len(df)
        processed = 0

        for batch_start in range(0, total, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total)
            batch = [(i, df.iloc[i]) for i in range(batch_start, batch_end)]

            print(f"[{batch_start + 1}-{batch_end}/{total}] Обрабатываю батч...")

            # Обработка батча
            results = await finder.process_batch(batch)

            # Сохранение результатов
            for idx, instagram_url in results:
                if instagram_url:
                    df.at[idx, 'Instagram URL'] = instagram_url
                    df.at[idx, 'Status'] = 'Found'
                    name = df.at[idx, 'Person - Name']
                    print(f"  + {name}: {instagram_url}")
                else:
                    df.at[idx, 'Status'] = 'Not Found'

            processed += len(batch)

            # Автосохранение
            if processed % SAVE_EVERY == 0:
                df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                print(f"  [SAVE] Sohraneno {processed}/{total}")

            # Пауза между батчами
            await asyncio.sleep(0.3)

    # Финальное сохранение
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    # Статистика
    found = len(df[df['Status'] == 'Found'])
    not_found = len(df[df['Status'] == 'Not Found'])

    print(f"\n=== STATISTIKA ===")
    print(f"  Vsego: {total}")
    print(f"  Naydeno: {found} ({found/total*100:.1f}%)")
    print(f"  Ne naydeno: {not_found}")
    print(f"  API zaprosov: {finder.total_requests}")
    print(f"\n[SAVE] Rezultaty: {output_csv}")

    return finder.total_requests


async def main():
    """Главная функция"""
    input_csv = r"C:\Users\79818\Downloads\leads_ai_girls - name+email.csv"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_csv = f"C:\\Users\\79818\\Desktop\\stxt.ai\\results\\fast_enriched_100_{timestamp}.csv"

    # Обработка первых 100
    total_requests = await fast_enrich(input_csv, output_csv, max_leads=100)

    print(f"\n=== GOTOVO! Ispolzovano {total_requests} API zaprosov ===")
    print(f"\nEsli vse OK, zapustite dlya vseh lidov:")
    print(f"python fast_enrich.py --all")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        # Обработка всех лидов до лимита
        input_csv = r"C:\Users\79818\Downloads\leads_ai_girls - name+email.csv"
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_csv = f"C:\\Users\\79818\\Desktop\\stxt.ai\\results\\fast_enriched_all_{timestamp}.csv"

        print("\n!!! REZHIM: VSE LIDY (do limita API)")
        print("Obrabotka ostanovitsya pri dostizhenii rate limit\n")

        asyncio.run(fast_enrich(input_csv, output_csv, max_leads=10000))
    else:
        # Только 100
        asyncio.run(main())
