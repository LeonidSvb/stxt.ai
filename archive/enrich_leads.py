#!/usr/bin/env python3
"""
Instagram Lead Enrichment Workflow
Обогащение лидов данными из Instagram
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv
from modules.google_search import GoogleSearchClient
from modules.instagram_scraper import InstagramScraper
from modules.csv_handler import CSVHandler

# Загружаем переменные окружения
load_dotenv()


class LeadEnricher:
    """Основной класс для обогащения лидов"""

    def __init__(
        self,
        rapidapi_key: str,
        apify_key: str,
        enrich_profiles: bool = True,
        delay_between_requests: float = 2.0
    ):
        self.google_client = GoogleSearchClient(rapidapi_key)
        self.instagram_scraper = InstagramScraper(apify_key) if enrich_profiles else None
        self.csv_handler = CSVHandler()
        self.enrich_profiles = enrich_profiles
        self.delay = delay_between_requests

    def enrich_single_lead(self, name: str, email: str) -> dict:
        """
        Обогатить одного лида

        Args:
            name: Имя лида
            email: Email лида

        Returns:
            Словарь с результатами
        """
        result = {
            'instagram_url': None,
            'profile_data': None,
            'status': 'not_found'
        }

        # Шаг 1: Найти Instagram URL через Google
        print(f"  🔍 Поиск Instagram профиля...")
        instagram_url = self.google_client.find_instagram_profile(name, email)

        if not instagram_url:
            result['status'] = 'not_found'
            print(f"  ✗ Instagram не найден")
            return result

        result['instagram_url'] = instagram_url
        print(f"  ✓ Найден: {instagram_url}")

        # Шаг 2: Обогатить данными из Apify (опционально)
        if self.enrich_profiles and self.instagram_scraper:
            print(f"  🔎 Обогащаю профиль через Apify...")
            time.sleep(self.delay)  # Пауза перед Apify запросом

            profile_data = self.instagram_scraper.scrape_profile(instagram_url)

            if profile_data:
                result['profile_data'] = profile_data
                result['status'] = 'enriched'
                print(f"  ✓ Обогащен: @{profile_data['username']} ({profile_data['followers_count']} followers)")
            else:
                result['status'] = 'found_not_enriched'
                print(f"  ⚠ Найден, но не удалось обогатить")
        else:
            result['status'] = 'found_not_enriched'

        return result

    def enrich_csv(
        self,
        input_path: str,
        output_path: str = None,
        name_column: str = 'Person - Name',
        email_column: str = 'Person - Email - Work',
        max_rows: int = None
    ):
        """
        Обогатить CSV файл

        Args:
            input_path: Путь к входному CSV
            output_path: Путь к выходному CSV (опционально)
            name_column: Название колонки с именем
            email_column: Название колонки с email
            max_rows: Максимальное количество строк для обработки
        """
        print(f"\n📂 Загружаю CSV: {input_path}")
        df = self.csv_handler.load_csv(input_path)

        # Подготовка колонок
        df = self.csv_handler.prepare_enrichment_columns(df)

        total_rows = len(df) if not max_rows else min(len(df), max_rows)
        print(f"📊 Всего лидов: {len(df)}")
        print(f"🎯 Будет обработано: {total_rows}\n")

        # Обработка каждого лида
        for index, row in df.iterrows():
            if max_rows and index >= max_rows:
                break

            name = row.get(name_column, '')
            email = row.get(email_column, '')

            print(f"[{index + 1}/{total_rows}] {name} ({email})")

            if not name and not email:
                print(f"  ⚠ Пропущено: нет имени и email")
                self.csv_handler.mark_row_as_not_found(df, index, "No name/email")
                continue

            try:
                # Обогащаем лида
                result = self.enrich_single_lead(name, email)

                # Обновляем DataFrame
                if result['status'] == 'not_found':
                    self.csv_handler.mark_row_as_not_found(df, index)
                else:
                    self.csv_handler.update_row_with_instagram_data(
                        df, index,
                        result['instagram_url'],
                        result['profile_data']
                    )

                # Пауза между запросами
                time.sleep(self.delay)

            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                self.csv_handler.mark_row_as_not_found(df, index, f"Error: {str(e)}")

        # Сохраняем результаты
        if not output_path:
            output_path = self.csv_handler.generate_output_filename(input_path)

        print(f"\n💾 Сохраняю результаты: {output_path}")
        self.csv_handler.save_csv(df, output_path)

        # Статистика
        stats = self.csv_handler.get_statistics(df)
        print(f"\n📈 Статистика:")
        print(f"  Всего: {stats['total']}")
        print(f"  Обогащено: {stats['enriched']}")
        print(f"  Найдено (без обогащения): {stats['found_not_enriched']}")
        print(f"  Не найдено: {stats['not_found']}")
        print(f"  Success Rate: {stats['success_rate']:.1f}%")

        return output_path, stats


def main():
    """Главная функция для CLI использования"""
    import argparse

    parser = argparse.ArgumentParser(description='Обогащение лидов Instagram данными')
    parser.add_argument('input', help='Входной CSV файл')
    parser.add_argument('-o', '--output', help='Выходной CSV файл (опционально)')
    parser.add_argument('--no-enrich', action='store_true', help='Только найти URLs, не обогащать')
    parser.add_argument('--max-rows', type=int, help='Максимум строк для обработки')
    parser.add_argument('--delay', type=float, default=2.0, help='Задержка между запросами (сек)')

    args = parser.parse_args()

    # API ключи из .env
    rapidapi_key = os.getenv('RAPIDAPI_KEY')
    apify_key = os.getenv('APIFY_API_KEY')

    if not rapidapi_key:
        print("❌ RAPIDAPI_KEY не найден в .env")
        return

    if not args.no_enrich and not apify_key:
        print("❌ APIFY_API_KEY не найден в .env")
        return

    # Создаем enricher
    enricher = LeadEnricher(
        rapidapi_key=rapidapi_key,
        apify_key=apify_key,
        enrich_profiles=not args.no_enrich,
        delay_between_requests=args.delay
    )

    # Обогащаем
    enricher.enrich_csv(
        input_path=args.input,
        output_path=args.output,
        max_rows=args.max_rows
    )


if __name__ == '__main__':
    main()
