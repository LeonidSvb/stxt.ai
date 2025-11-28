#!/usr/bin/env python3
"""
Instagram Lead Enrichment PRO - Универсальный UI с настройками
Async batch processing + конфигурируемые параметры
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import asyncio
import aiohttp
import re
import json
from typing import Dict, List, Optional
import time

load_dotenv()

# Paths
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)
CHECKPOINTS_DIR = Path(__file__).parent / "checkpoints"
CHECKPOINTS_DIR.mkdir(exist_ok=True)

# Конфигурация страницы
st.set_page_config(
    page_title="Instagram Enrichment PRO",
    page_icon="⚡",
    layout="wide"
)

# ПРЕСЕТЫ КОНФИГУРАЦИЙ
PRESETS = {
    "Safe": {
        "name": "🛡️ Safe",
        "description": "Безопасный режим - минимальный риск блокировки",
        "batch_size": 1,
        "max_workers": 1,
        "delay_between": 3.0,
        "query_strategy": "sequential",
        "queries_per_lead": 2
    },
    "Balanced": {
        "name": "⚖️ Balanced",
        "description": "Сбалансированный - оптимальное соотношение скорости и надежности",
        "batch_size": 5,
        "max_workers": 3,
        "delay_between": 1.5,
        "query_strategy": "parallel",
        "queries_per_lead": 1
    },
    "Fast": {
        "name": "🚀 Fast",
        "description": "Быстрый режим - ускоренная обработка",
        "batch_size": 10,
        "max_workers": 5,
        "delay_between": 0.8,
        "query_strategy": "parallel",
        "queries_per_lead": 1
    },
    "Turbo": {
        "name": "⚡ Turbo",
        "description": "Турбо режим - максимальная скорость (риск rate limit)",
        "batch_size": 20,
        "max_workers": 10,
        "delay_between": 0.3,
        "query_strategy": "parallel",
        "queries_per_lead": 1
    }
}

# Session state
if 'enriched_df' not in st.session_state:
    st.session_state.enriched_df = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'checkpoint' not in st.session_state:
    st.session_state.checkpoint = None

st.title("⚡ Instagram Lead Enrichment PRO")
st.markdown("Универсальный обогащатель с async batch processing")

# Tabs
tab1, tab2, tab3 = st.tabs(["⚙️ Настройки и запуск", "📊 Результаты", "📖 Инструкция"])

with tab1:
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("⚙️ Конфигурация")

        # Пресеты
        st.markdown("### 🎛️ Пресеты")
        preset_choice = st.radio(
            "Выбери пресет",
            options=list(PRESETS.keys()),
            format_func=lambda x: PRESETS[x]["name"],
            index=1  # Balanced по умолчанию
        )

        preset = PRESETS[preset_choice]
        st.info(preset["description"])

        # Расширенные настройки
        with st.expander("🔧 Расширенные настройки"):
            st.markdown("**Performance**")
            batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                max_value=50,
                value=preset["batch_size"],
                help="Количество лидов в одном батче"
            )

            max_workers = st.number_input(
                "Max Workers",
                min_value=1,
                max_value=20,
                value=preset["max_workers"],
                help="Количество параллельных потоков"
            )

            delay_between = st.slider(
                "Delay (сек)",
                min_value=0.1,
                max_value=5.0,
                value=preset["delay_between"],
                step=0.1,
                help="Задержка между запросами"
            )

            st.markdown("**Query Strategy**")
            query_strategy = st.selectbox(
                "Стратегия запросов",
                ["sequential", "parallel"],
                index=0 if preset["query_strategy"] == "sequential" else 1
            )

            queries_per_lead = st.number_input(
                "Запросов на лид",
                min_value=1,
                max_value=3,
                value=preset["queries_per_lead"],
                help="1 = только email, 2 = email + имя"
            )

            st.markdown("**Query Templates**")
            query_template_email = st.text_input(
                "Template для email",
                value='"{email}" instagram',
                help="Шаблон запроса с {email}"
            )

            query_template_name = st.text_input(
                "Template для имени",
                value='"{name}" instagram influencer',
                help="Шаблон запроса с {name}"
            )

        # API настройки
        st.markdown("### 🔑 API")
        rapidapi_key = st.text_input(
            "RapidAPI Key",
            value=os.getenv("RAPIDAPI_KEY", ""),
            type="password"
        )

        # Лимиты обработки
        st.markdown("### 📊 Лимиты")
        process_all = st.checkbox("Обработать все строки", value=False)
        if not process_all:
            max_rows = st.number_input(
                "Количество строк",
                min_value=1,
                max_value=1000,
                value=10
            )

        # Resume
        st.markdown("### 💾 Resume")
        checkpoints = list(CHECKPOINTS_DIR.glob("*.json"))
        if checkpoints:
            resume_from = st.selectbox(
                "Продолжить с checkpoint",
                options=["Нет"] + [c.name for c in checkpoints]
            )
        else:
            resume_from = "Нет"

    with col_right:
        st.subheader("📁 Загрузка и обработка")

        # Upload
        uploaded_file = st.file_uploader(
            "CSV с лидами",
            type=['csv']
        )

        if uploaded_file:
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            st.success(f"✅ Загружено {len(df)} строк")

            # Preview
            with st.expander("👀 Предпросмотр"):
                st.dataframe(df.head(10))

            # Column selection
            col1, col2 = st.columns(2)
            with col1:
                name_col = st.selectbox("Колонка: Имя", df.columns, index=0)
            with col2:
                email_col = st.selectbox("Колонка: Email", df.columns, index=min(2, len(df.columns)-1))

            # Параметры запуска
            st.markdown("### 🚀 Запуск")

            # Estimated stats
            estimated_leads = len(df) if process_all else min(max_rows, len(df))
            estimated_requests = estimated_leads * queries_per_lead
            estimated_time_sec = (estimated_leads / batch_size) * (batch_size * delay_between)
            estimated_time_min = estimated_time_sec / 60

            col1, col2, col3 = st.columns(3)
            col1.metric("Лидов", estimated_leads)
            col2.metric("API запросов", estimated_requests)
            col3.metric("Время (мин)", f"{estimated_time_min:.1f}")

            # Start button
            if st.button("🚀 НАЧАТЬ ОБОГАЩЕНИЕ", type="primary", use_container_width=True):
                st.session_state.processing = True

                # TODO: Run async enrichment
                st.info("⚙️ Запуск async обогащения...")
                st.warning("🚧 Async processing в разработке. Используй CLI скрипт для максимальной скорости!")

                # Placeholder для будущего async кода
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_table = st.empty()

                status_text.text("Обработка начнется здесь...")

                # Здесь будет async код

with tab2:
    st.subheader("📊 История результатов")

    results_files = sorted(RESULTS_DIR.glob("*.csv"), key=os.path.getmtime, reverse=True)

    if results_files:
        selected_file = st.selectbox(
            "Выбери файл",
            options=results_files,
            format_func=lambda x: f"{x.name} ({datetime.fromtimestamp(x.stat().st_mtime).strftime('%Y-%m-%d %H:%M')})"
        )

        if selected_file:
            df_history = pd.read_csv(selected_file, encoding='utf-8-sig')

            # Stats
            total = len(df_history)
            found = len(df_history[df_history.get('Status') == 'Found']) if 'Status' in df_history.columns else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Всего", total)
            col2.metric("Найдено", found)
            col3.metric("Success Rate", f"{found/total*100:.1f}%" if total > 0 else "0%")

            # Data
            st.dataframe(df_history, use_container_width=True)

            # Download
            csv_buffer = df_history.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "⬇️ Скачать",
                data=csv_buffer,
                file_name=selected_file.name,
                mime='text/csv'
            )
    else:
        st.info("📭 Нет сохраненных результатов")

with tab3:
    st.subheader("📖 Инструкция")

    st.markdown("""
    ## Пресеты

    ### 🛡️ Safe
    - **Использование:** Тестирование, малые объемы (10-50 лидов)
    - **Скорость:** Медленная (100 лидов = 35-50 мин)
    - **Риск блокировки:** Минимальный
    - **API эффективность:** Низкая (2 запроса на лид)

    ### ⚖️ Balanced (Рекомендуется)
    - **Использование:** Ежедневная работа (50-200 лидов)
    - **Скорость:** Средняя (100 лидов = 8-12 мин)
    - **Риск блокировки:** Низкий
    - **API эффективность:** Средняя (1 запрос на лид)

    ### 🚀 Fast
    - **Использование:** Срочные задачи (100-500 лидов)
    - **Скорость:** Быстрая (100 лидов = 3-5 мин)
    - **Риск блокировки:** Средний
    - **API эффективность:** Высокая

    ### ⚡ Turbo
    - **Использование:** Массовая обработка (500+ лидов)
    - **Скорость:** Максимальная (100 лидов = 1-2 мин)
    - **Риск блокировки:** Высокий
    - **API эффективность:** Максимальная
    - ⚠️ **Может вызвать rate limit!**

    ## Настройки

    ### Batch Size
    - Количество лидов обрабатываемых одновременно
    - Больше = быстрее, но выше риск rate limit

    ### Max Workers
    - Количество параллельных потоков
    - Больше = быстрее, но требует больше ресурсов

    ### Delay
    - Задержка между запросами (сек)
    - Меньше = быстрее, но выше риск блокировки

    ### Query Strategy
    - **Sequential:** Запросы по очереди (надежнее)
    - **Parallel:** Запросы параллельно (быстрее)

    ### Queries Per Lead
    - **1:** Только email (экономия API, быстрее)
    - **2:** Email + имя (выше success rate)

    ## CLI Альтернатива

    Для максимальной скорости используй CLI скрипты:

    ```bash
    # Турбо режим
    node scripts/discovery/turbo-enrich.cjs

    # С настройками
    python fast_enrich.py --max-rows 100 --delay 1.0
    ```
    """)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Текущий сеанс")
if st.session_state.processing:
    st.sidebar.success("🟢 Обработка...")
else:
    st.sidebar.info("⚪ Ожидание")
