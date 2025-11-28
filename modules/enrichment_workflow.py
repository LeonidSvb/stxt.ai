"""
Instagram Enrichment Workflow
Асинхронный workflow для обогащения лидов через RapidAPI Google Search
"""

import asyncio
import aiohttp
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Callable
import time


class AsyncGoogleSearch:
    """Асинхронный клиент для RapidAPI Google Search"""

    def __init__(self, api_key: str, host: str = "google-search116.p.rapidapi.com"):
        self.api_key = api_key
        self.host = host
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

    async def search(
        self,
        query: str,
        limit: int = 10,
        retry: int = 0,
        max_retries: int = 2
    ) -> Optional[Dict]:
        """Выполнить асинхронный поиск"""
        try:
            self.total_requests += 1

            headers = {
                "x-rapidapi-host": self.host,
                "x-rapidapi-key": self.api_key
            }

            params = {"query": query, "limit": limit}

            async with self.session.get(
                f"https://{self.host}",
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:

                if response.status == 429:
                    print(f"\n!!! RATE LIMIT! Requests: {self.total_requests}")
                    return None

                if response.status != 200:
                    if retry < max_retries:
                        await asyncio.sleep(1)
                        return await self.search(query, limit, retry + 1, max_retries)
                    return None

                data = await response.json()
                self.successful += 1
                return data

        except asyncio.TimeoutError:
            if retry < max_retries:
                await asyncio.sleep(1)
                return await self.search(query, limit, retry + 1, max_retries)
            self.failed += 1
            return None
        except Exception as e:
            print(f"\n  ! Error: {e}")
            self.failed += 1
            return None

    @staticmethod
    def extract_instagram_url(results: Dict) -> Optional[str]:
        """Извлечь Instagram URL из результатов поиска"""
        if not results or not results.get('results'):
            return None

        for result in results['results']:
            url = result.get('url', '')

            if '/reel/' in url or '/p/' in url or '/tv/' in url:
                continue

            pattern = r'instagram\.com/([a-zA-Z0-9_.]+)'
            match = re.search(pattern, url)
            if match:
                username = match.group(1).rstrip('/')
                if username not in ['reel', 'p', 'tv', 'stories', 'explore', 'reels']:
                    return f"https://www.instagram.com/{username}/"

        return None


class EnrichmentWorkflow:
    """
    Главный workflow для обогащения лидов
    Unix философия: делает одну вещь - обогащает лиды
    """

    def __init__(
        self,
        api_key: str,
        batch_size: int = 5,
        delay: float = 1.0,
        save_every: int = 10,
        max_retries: int = 2
    ):
        self.api_key = api_key
        self.batch_size = batch_size
        self.delay = delay
        self.save_every = save_every
        self.max_retries = max_retries

        self.stats = {
            'total': 0,
            'processed': 0,
            'found': 0,
            'not_found': 0,
            'errors': 0,
            'api_requests': 0
        }

    async def find_instagram_for_lead(
        self,
        searcher: AsyncGoogleSearch,
        name: str,
        email: str,
        query_template: Optional[str] = None
    ) -> Optional[str]:
        """
        Найти Instagram URL для одного лида

        Args:
            searcher: AsyncGoogleSearch instance
            name: Имя лида
            email: Email лида
            query_template: Кастомный шаблон запроса (опционально)

        Returns:
            Instagram URL или None
        """
        if query_template:
            query = query_template.format(name=name, email=email)
            result = await searcher.search(query)
            if result:
                return searcher.extract_instagram_url(result)
            return None

        queries = [
            f'"{email}" instagram',
            f'"{name}" instagram'
        ]

        for query in queries:
            result = await searcher.search(query)

            if result is None:
                return None

            if not result.get('results'):
                await asyncio.sleep(0.2)
                continue

            instagram_url = searcher.extract_instagram_url(result)
            if instagram_url:
                return instagram_url

            await asyncio.sleep(0.2)

        return None

    async def process_batch(
        self,
        searcher: AsyncGoogleSearch,
        leads_batch: List[tuple],
        query_template: Optional[str] = None
    ) -> List[tuple]:
        """Обработать батч лидов"""
        tasks = []
        for idx, lead in leads_batch:
            name = lead.get('Person - Name', '')
            email = lead.get('Person - Email - Work', '')

            task = self.find_instagram_for_lead(searcher, name, email, query_template)
            tasks.append((idx, task))

        results = []
        for idx, task in tasks:
            url = await task
            results.append((idx, url))

        return results

    async def enrich_csv(
        self,
        input_csv: str,
        output_csv: str,
        max_leads: Optional[int] = None,
        query_template: Optional[str] = None,
        progress_callback: Optional[Callable] = None
    ) -> Dict:
        """
        Обогатить CSV файл

        Args:
            input_csv: Путь к входному CSV
            output_csv: Путь к выходному CSV
            max_leads: Максимум лидов (None = все)
            query_template: Кастомный шаблон запроса
            progress_callback: Функция для обновления прогресса

        Returns:
            Статистика обработки
        """
        print(f"\n>> Instagram URL Enrichment (Async)")
        print(f"Input: {input_csv}")
        print(f"Batch size: {self.batch_size}")
        print(f"Delay: {self.delay}s\n")

        df = pd.read_csv(input_csv, encoding='utf-8-sig')

        if max_leads:
            df = df.head(max_leads)

        if 'Instagram URL' not in df.columns:
            df['Instagram URL'] = ''
        if 'Status' not in df.columns:
            df['Status'] = ''

        self.stats['total'] = len(df)

        async with AsyncGoogleSearch(self.api_key, max_concurrent=self.batch_size) as searcher:

            total = len(df)
            processed = 0

            for batch_start in range(0, total, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total)
                batch = [(i, df.iloc[i]) for i in range(batch_start, batch_end)]

                print(f"[{batch_start + 1}-{batch_end}/{total}] Processing batch...")

                results = await self.process_batch(searcher, batch, query_template)

                for idx, instagram_url in results:
                    if instagram_url:
                        df.at[idx, 'Instagram URL'] = instagram_url
                        df.at[idx, 'Status'] = 'Found'
                        self.stats['found'] += 1
                        name = df.at[idx, 'Person - Name']
                        print(f"  + {name}: {instagram_url}")
                    else:
                        df.at[idx, 'Status'] = 'Not Found'
                        self.stats['not_found'] += 1

                processed += len(batch)
                self.stats['processed'] = processed

                if processed % self.save_every == 0:
                    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                    print(f"  [SAVE] {processed}/{total}")

                if progress_callback:
                    progress_callback(processed, total)

                await asyncio.sleep(self.delay)

        df.to_csv(output_csv, index=False, encoding='utf-8-sig')

        self.stats['api_requests'] = searcher.total_requests

        print(f"\n=== STATISTICS ===")
        print(f"  Total: {total}")
        print(f"  Found: {self.stats['found']} ({self.stats['found']/total*100:.1f}%)")
        print(f"  Not found: {self.stats['not_found']}")
        print(f"  API requests: {self.stats['api_requests']}")
        print(f"\n[SAVE] Results: {output_csv}")

        return self.stats


def run_enrichment(
    input_csv: str,
    output_csv: str,
    api_key: str,
    max_leads: Optional[int] = None,
    batch_size: int = 5,
    delay: float = 1.0,
    query_template: Optional[str] = None
) -> Dict:
    """
    Запустить обогащение (синхронная обертка для async)

    Args:
        input_csv: Входной CSV
        output_csv: Выходной CSV
        api_key: RapidAPI ключ
        max_leads: Макс лидов
        batch_size: Размер батча
        delay: Задержка в секундах
        query_template: Шаблон запроса

    Returns:
        Статистика
    """
    workflow = EnrichmentWorkflow(
        api_key=api_key,
        batch_size=batch_size,
        delay=delay
    )

    return asyncio.run(
        workflow.enrich_csv(
            input_csv,
            output_csv,
            max_leads,
            query_template
        )
    )
