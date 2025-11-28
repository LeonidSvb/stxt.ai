#!/usr/bin/env python3
"""
Instagram Lead Enrichment - Streamlit App
Обогащение лидов данными из Instagram через удобный UI
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import time
import io

from modules.google_search import GoogleSearchClient
from modules.instagram_scraper import InstagramScraper
from modules.csv_handler import CSVHandler

# Загружаем .env
load_dotenv()

# Пути
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Конфигурация страницы
st.set_page_config(
    page_title="Instagram Lead Enrichment",
    page_icon="📸",
    layout="wide"
)

# Инициализация session state
if 'current_results' not in st.session_state:
    st.session_state.current_results = None
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'enriched_df' not in st.session_state:
    st.session_state.enriched_df = None

# Заголовок
st.title("📸 Instagram Lead Enrichment")
st.markdown("Обогащение лидов Instagram профилями и данными")

# Табы
tab1, tab2 = st.tabs(["📤 Обогащение", "📊 Результаты"])

with tab1:
    # Sidebar - настройки
    with st.sidebar:
        st.header("⚙️ Настройки")

        # API ключи
        st.subheader("🔑 API Keys")
        rapidapi_key = st.text_input(
            "RapidAPI Key",
            value=os.getenv("RAPIDAPI_KEY", ""),
            type="password"
        )
        apify_key = st.text_input(
            "Apify API Key",
            value=os.getenv("APIFY_API_KEY", ""),
            type="password"
        )

        st.divider()

        # Опции обогащения
        st.subheader("⚡ Опции обогащения")

        enrich_profiles = st.checkbox(
            "Глубокое обогащение (Apify)",
            value=True,
            help="Получить полные данные профиля (followers, bio, etc.)"
        )

        if not enrich_profiles:
            st.info("Будут найдены только Instagram URLs без дополнительных данных")

        process_all = st.checkbox("Обработать все строки", value=False)
        if not process_all:
            sample_size = st.number_input(
                "Количество строк",
                min_value=1,
                max_value=1000,
                value=10
            )

        delay = st.slider(
            "Задержка между запросами (сек)",
            min_value=1.0,
            max_value=10.0,
            value=2.0,
            step=0.5
        )

        st.divider()

        # Стоимость
        if enrich_profiles:
            est_cost = sample_size * 0.0027 if not process_all else 0
            st.info(f"💰 Примерная стоимость Apify: ${est_cost:.2f}")

    # Загрузка файла
    st.subheader("📁 Загрузите CSV с лидами")
    uploaded_file = st.file_uploader(
        "CSV файл с колонками: имя, фамилия, email",
        type=['csv']
    )

    if uploaded_file:
        # Читаем CSV
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        st.success(f"✅ Загружено {len(df)} строк")

        # Предпросмотр
        with st.expander("👀 Предпросмотр данных"):
            st.dataframe(df.head(10))

        # Выбор колонок
        st.subheader("🔍 Выбор колонок")
        col1, col2 = st.columns(2)

        with col1:
            name_column = st.selectbox(
                "Колонка с именем",
                options=df.columns,
                index=0
            )

        with col2:
            email_column = st.selectbox(
                "Колонка с email",
                options=df.columns,
                index=min(2, len(df.columns) - 1)
            )

        # Кнопка запуска
        st.divider()

        if st.button("🚀 Начать обогащение", type="primary"):
            # Проверка API ключей
            if not rapidapi_key:
                st.error("❌ Введите RapidAPI ключ")
            elif enrich_profiles and not apify_key:
                st.error("❌ Введите Apify API ключ или отключите глубокое обогащение")
            else:
                # Инициализация клиентов
                google_client = GoogleSearchClient(rapidapi_key)
                instagram_scraper = InstagramScraper(apify_key) if enrich_profiles else None
                csv_handler = CSVHandler()

                # Подготовка данных
                df = csv_handler.prepare_enrichment_columns(df)
                max_rows = None if process_all else sample_size

                # Progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                result_container = st.container()

                total_to_process = len(df) if process_all else min(sample_size, len(df))

                # Обработка
                for index, row in df.iterrows():
                    if max_rows and index >= max_rows:
                        break

                    name = row.get(name_column, '')
                    email = row.get(email_column, '')

                    progress = (index + 1) / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Обрабатываю {index + 1}/{total_to_process}: {name}")

                    if not name and not email:
                        csv_handler.mark_row_as_not_found(df, index, "No name/email")
                        continue

                    try:
                        # Поиск Instagram
                        instagram_url = google_client.find_instagram_profile(name, email)

                        if not instagram_url:
                            csv_handler.mark_row_as_not_found(df, index)
                            continue

                        # Обогащение (опционально)
                        profile_data = None
                        if enrich_profiles and instagram_scraper:
                            time.sleep(delay)
                            profile_data = instagram_scraper.scrape_profile(instagram_url)

                        # Обновляем DataFrame
                        csv_handler.update_row_with_instagram_data(
                            df, index, instagram_url, profile_data
                        )

                        time.sleep(delay)

                    except Exception as e:
                        csv_handler.mark_row_as_not_found(df, index, str(e))

                # Завершено
                progress_bar.progress(1.0)
                status_text.text("✅ Обогащение завершено!")

                # Сохраняем результаты
                st.session_state.enriched_df = df
                st.session_state.processing_complete = True

                # Статистика
                stats = csv_handler.get_statistics(df)

                st.success("🎉 Обогащение завершено!")

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Всего", stats['total'])
                col2.metric("Обогащено", stats['enriched'])
                col3.metric("Найдено", stats['found_not_enriched'])
                col4.metric("Success Rate", f"{stats['success_rate']:.1f}%")

                # Предпросмотр
                st.subheader("📊 Результаты")
                st.dataframe(df)

                # Скачивание
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_filename = f"leads_enriched_{timestamp}.csv"

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

                st.download_button(
                    label="⬇️ Скачать обогащенный CSV",
                    data=csv_buffer.getvalue(),
                    file_name=output_filename,
                    mime='text/csv'
                )

with tab2:
    st.subheader("📂 История результатов")

    # Список результатов
    results_files = sorted(RESULTS_DIR.glob("*.csv"), key=os.path.getmtime, reverse=True)

    if results_files:
        selected_file = st.selectbox(
            "Выберите файл",
            options=results_files,
            format_func=lambda x: f"{x.name} ({datetime.fromtimestamp(x.stat().st_mtime).strftime('%Y-%m-%d %H:%M')})"
        )

        if selected_file:
            df_history = pd.read_csv(selected_file, encoding='utf-8-sig')

            # Статистика
            csv_handler = CSVHandler()
            stats = csv_handler.get_statistics(df_history)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Всего", stats['total'])
            col2.metric("Обогащено", stats['enriched'])
            col3.metric("Найдено", stats['found_not_enriched'])
            col4.metric("Success Rate", f"{stats['success_rate']:.1f}%")

            # Данные
            st.dataframe(df_history)

            # Скачать
            csv_buffer = io.StringIO()
            df_history.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

            st.download_button(
                label="⬇️ Скачать",
                data=csv_buffer.getvalue(),
                file_name=selected_file.name,
                mime='text/csv'
            )
    else:
        st.info("📭 Пока нет сохраненных результатов")

    # Последние результаты из session state
    if st.session_state.enriched_df is not None:
        st.divider()
        st.subheader("🆕 Последние результаты")
        st.dataframe(st.session_state.enriched_df)
