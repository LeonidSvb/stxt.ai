"""
RapidAPI Google Search Module
Поиск Instagram профилей через Google Search API
"""

import requests
import re
from typing import Optional, Dict, List
from urllib.parse import quote


class GoogleSearchClient:
    """Клиент для RapidAPI Google Search"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.host = "google-search116.p.rapidapi.com"
        self.base_url = f"https://{self.host}"

    def search(self, query: str, limit: int = 15) -> Dict:
        """Выполнить поиск через Google"""
        headers = {
            "x-rapidapi-host": self.host,
            "x-rapidapi-key": self.api_key
        }

        params = {
            "query": query,
            "limit": limit
        }

        response = requests.get(
            self.base_url,
            headers=headers,
            params=params,
            timeout=30
        )

        response.raise_for_status()
        return response.json()

    def extract_instagram_username(self, url: str) -> Optional[str]:
        """Извлечь username из Instagram URL"""
        pattern = r'instagram\.com/([a-zA-Z0-9_.]+)'
        match = re.search(pattern, url)

        if match:
            username = match.group(1).rstrip('/')
            # Исключаем не-профили
            if username not in ['reel', 'p', 'tv', 'stories', 'explore']:
                return username
        return None

    def find_instagram_profile(self, name: str, email: str) -> Optional[str]:
        """
        Найти Instagram профиль по имени и email

        Args:
            name: Имя лида
            email: Email лида

        Returns:
            Instagram URL или None
        """
        queries = [
            f'"{email}" instagram',
            f'"{name}" instagram influencer',
        ]

        for query in queries:
            try:
                results = self.search(query)

                if not results.get('results'):
                    continue

                # Ищем первый валидный Instagram профиль
                for result in results['results']:
                    url = result.get('url', '')

                    # Пропускаем posts/reels
                    if any(x in url for x in ['/reel/', '/p/', '/tv/']):
                        continue

                    username = self.extract_instagram_username(url)
                    if username:
                        return f"https://www.instagram.com/{username}/"

            except Exception as e:
                print(f"  ⚠ Ошибка поиска: {e}")
                continue

        return None
