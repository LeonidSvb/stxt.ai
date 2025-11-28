"""
CSV Handler Module
Работа с CSV файлами для обогащения лидов
"""

import pandas as pd
from typing import List, Dict
from pathlib import Path
from datetime import datetime


class CSVHandler:
    """Обработчик CSV файлов"""

    @staticmethod
    def load_csv(file_path: str) -> pd.DataFrame:
        """Загрузить CSV файл"""
        return pd.read_csv(file_path, encoding='utf-8-sig')

    @staticmethod
    def save_csv(df: pd.DataFrame, file_path: str):
        """Сохранить CSV файл"""
        df.to_csv(file_path, index=False, encoding='utf-8-sig')

    @staticmethod
    def prepare_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Добавить колонки для обогащения"""
        new_columns = [
            'Instagram URL',
            'Instagram Username',
            'Full Name',
            'Bio',
            'Followers',
            'Following',
            'Posts',
            'Verified',
            'Business Account',
            'Business Category',
            'External URL',
            'Enrichment Status'
        ]

        for col in new_columns:
            if col not in df.columns:
                df[col] = ''

        return df

    @staticmethod
    def update_row_with_instagram_data(
        df: pd.DataFrame,
        index: int,
        instagram_url: str,
        profile_data: Dict = None
    ) -> pd.DataFrame:
        """Обновить строку данными Instagram"""
        df.at[index, 'Instagram URL'] = instagram_url

        if profile_data:
            df.at[index, 'Instagram Username'] = profile_data.get('username', '')
            df.at[index, 'Full Name'] = profile_data.get('full_name', '')
            df.at[index, 'Bio'] = profile_data.get('biography', '')
            df.at[index, 'Followers'] = profile_data.get('followers_count', 0)
            df.at[index, 'Following'] = profile_data.get('following_count', 0)
            df.at[index, 'Posts'] = profile_data.get('posts_count', 0)
            df.at[index, 'Verified'] = 'Yes' if profile_data.get('is_verified') else 'No'
            df.at[index, 'Business Account'] = 'Yes' if profile_data.get('is_business') else 'No'
            df.at[index, 'Business Category'] = profile_data.get('business_category', '')
            df.at[index, 'External URL'] = profile_data.get('external_url', '')
            df.at[index, 'Enrichment Status'] = 'Enriched'
        else:
            df.at[index, 'Instagram Username'] = instagram_url.split('/')[-2] if instagram_url else ''
            df.at[index, 'Enrichment Status'] = 'Found (Not Enriched)'

        return df

    @staticmethod
    def mark_row_as_not_found(df: pd.DataFrame, index: int, reason: str = '') -> pd.DataFrame:
        """Отметить строку как не найденную"""
        df.at[index, 'Enrichment Status'] = f'Not Found{" - " + reason if reason else ""}'
        return df

    @staticmethod
    def get_statistics(df: pd.DataFrame) -> Dict:
        """Получить статистику обогащения"""
        total = len(df)
        enriched = len(df[df['Enrichment Status'] == 'Enriched'])
        found_not_enriched = len(df[df['Enrichment Status'] == 'Found (Not Enriched)'])
        not_found = len(df[df['Enrichment Status'].str.startswith('Not Found', na=False)])
        pending = total - enriched - found_not_enriched - not_found

        return {
            'total': total,
            'enriched': enriched,
            'found_not_enriched': found_not_enriched,
            'not_found': not_found,
            'pending': pending,
            'success_rate': (enriched + found_not_enriched) / total * 100 if total > 0 else 0
        }

    @staticmethod
    def generate_output_filename(input_path: str, suffix: str = 'enriched') -> str:
        """Сгенерировать имя выходного файла с timestamp"""
        input_path = Path(input_path)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return str(input_path.parent / f"{input_path.stem}_{suffix}_{timestamp}.csv")
