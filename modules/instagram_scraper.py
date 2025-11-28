"""
Apify Instagram Scraper Module
Глубокое обогащение Instagram профилей
"""

import time
from typing import Optional, Dict, List
from apify_client import ApifyClient


class InstagramScraper:
    """Клиент для Apify Instagram Scraper"""

    def __init__(self, api_key: str):
        self.client = ApifyClient(api_key)

    def scrape_profile(self, instagram_url: str, wait_for_finish: bool = True) -> Optional[Dict]:
        """
        Скрейпить Instagram профиль

        Args:
            instagram_url: URL профиля Instagram
            wait_for_finish: Ждать завершения (True) или вернуть run_id (False)

        Returns:
            Данные профиля или None
        """
        try:
            # Запускаем актор
            run_input = {
                "directUrls": [instagram_url],
                "resultsType": "details",
                "resultsLimit": 1,
                "searchType": "user",
                "searchLimit": 1,
            }

            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)

            # Получаем результаты
            dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())

            if dataset_items and len(dataset_items) > 0:
                profile = dataset_items[0]
                return self._extract_profile_data(profile)

            return None

        except Exception as e:
            print(f"  ⚠ Ошибка Apify scraping: {e}")
            return None

    def scrape_profiles_batch(self, instagram_urls: List[str]) -> List[Dict]:
        """
        Скрейпить несколько профилей за один запуск

        Args:
            instagram_urls: Список URLs профилей

        Returns:
            Список данных профилей
        """
        try:
            run_input = {
                "directUrls": instagram_urls,
                "resultsType": "details",
                "resultsLimit": len(instagram_urls),
                "searchType": "user",
                "searchLimit": len(instagram_urls),
            }

            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())

            results = []
            for profile in dataset_items:
                results.append(self._extract_profile_data(profile))

            return results

        except Exception as e:
            print(f"  ⚠ Ошибка batch scraping: {e}")
            return []

    def _extract_profile_data(self, profile: Dict) -> Dict:
        """Извлечь нужные данные из профиля"""
        return {
            "username": profile.get("username", ""),
            "full_name": profile.get("fullName", ""),
            "biography": profile.get("biography", ""),
            "followers_count": profile.get("followersCount", 0),
            "following_count": profile.get("followsCount", 0),
            "posts_count": profile.get("postsCount", 0),
            "is_verified": profile.get("verified", False),
            "is_business": profile.get("isBusinessAccount", False),
            "business_category": profile.get("businessCategoryName", ""),
            "external_url": profile.get("externalUrl", ""),
            "profile_pic_url": profile.get("profilePicUrl", ""),
            "instagram_url": f"https://www.instagram.com/{profile.get('username', '')}/",
        }

    def format_number(self, num: int) -> str:
        """Форматировать число (1000 -> 1K)"""
        if num >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num / 1_000:.1f}K"
        return str(num)
