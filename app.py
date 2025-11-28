#!/usr/bin/env python3
"""
Instagram Lead Enrichment - Главное приложение
Модульная система обогащения лидов через Streamlit UI
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import asyncio
from modules.enrichment_workflow import EnrichmentWorkflow

load_dotenv()

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

st.set_page_config(
    page_title="Instagram Enrichment",
    page_icon="📸",
    layout="wide"
)

PRESETS = {
    "Safe": {
        "name": "🛡️ Safe (1 req/sec)",
        "description": "Безопасный режим - соответствует RapidAPI rate limit",
        "batch_size": 1,
        "delay": 1.0,
        "queries_per_lead": 2
    },
    "Balanced": {
        "name": "⚖️ Balanced (0.8 req/sec)",
        "description": "Сбалансированный - надежный с запасом",
        "batch_size": 5,
        "delay": 1.2,
        "queries_per_lead": 1
    }
}

if 'enriched_df' not in st.session_state:
    st.session_state.enriched_df = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'stats' not in st.session_state:
    st.session_state.stats = None

st.title("📸 Instagram Lead Enrichment")
st.markdown("Модульная система обогащения лидов через RapidAPI Google Search")

tab1, tab2, tab3 = st.tabs(["⚙️ Настройки", "📊 Результаты", "📖 Документация"])

with tab1:
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("⚙️ Конфигурация")

        st.markdown("### 🎛️ Пресеты")
        st.info("⚠️ RapidAPI лимит: **1 запрос/секунду**")

        preset_choice = st.radio(
            "Выбери режим",
            options=list(PRESETS.keys()),
            format_func=lambda x: PRESETS[x]["name"],
            index=0
        )

        preset = PRESETS[preset_choice]
        st.info(preset["description"])

        with st.expander("🔧 Расширенные настройки"):
            st.markdown("**Производительность**")

            batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                max_value=100,
                value=preset["batch_size"],
                help="Количество лидов в батче (рекомендуется 1-10 для rate limit 1 req/sec)"
            )

            delay = st.slider(
                "Delay между запросами (сек)",
                min_value=0.5,
                max_value=5.0,
                value=preset["delay"],
                step=0.1,
                help="Минимум 1.0 сек для RapidAPI (1 req/sec)"
            )

            if delay < 1.0:
                st.warning("⚠️ Задержка < 1 сек может вызвать rate limit!")

            st.markdown("**Query Templates**")

            use_custom_query = st.checkbox(
                "Использовать кастомный запрос",
                value=False,
                help="Отключи для автоматической стратегии (email -> name)"
            )

            if use_custom_query:
                st.markdown("**Доступные переменные:**")
                st.code("{name} - имя лида\n{email} - email лида")

                query_template = st.text_area(
                    "Шаблон запроса",
                    value='"{email}" instagram',
                    height=100,
                    help="Пример: \"{email}\" instagram\nПример: \"{name}\" instagram influencer"
                )

                st.markdown("**Примеры:**")
                st.code('"{email}" instagram')
                st.code('"{name}" instagram')
                st.code('"{name}" "{email}" instagram')
            else:
                query_template = None
                st.info("Стратегия: сначала email, потом name (fallback)")

        st.markdown("### 🔑 API")
        rapidapi_key = st.text_input(
            "RapidAPI Key",
            value=os.getenv("RAPIDAPI_KEY", ""),
            type="password"
        )

        st.markdown("### 📊 Лимиты обработки")
        process_all = st.checkbox("Обработать все строки", value=False)
        if not process_all:
            max_rows = st.number_input(
                "Количество строк",
                min_value=1,
                max_value=1000,
                value=10
            )
        else:
            max_rows = None

    with col_right:
        st.subheader("📁 Загрузка и запуск")

        uploaded_file = st.file_uploader(
            "CSV с лидами",
            type=['csv'],
            help="Загрузи CSV с колонками: Person - Name, Person - Email - Work"
        )

        if uploaded_file:
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            st.success(f"✅ Загружено {len(df)} строк")

            with st.expander("👀 Предпросмотр (первые 10 строк)"):
                st.dataframe(df.head(10))

            col1, col2 = st.columns(2)
            with col1:
                name_col = st.selectbox(
                    "Колонка: Имя",
                    df.columns,
                    index=0 if 'Person - Name' not in df.columns else list(df.columns).index('Person - Name')
                )
            with col2:
                email_col = st.selectbox(
                    "Колонка: Email",
                    df.columns,
                    index=2 if len(df.columns) > 2 else 0
                )

            st.markdown("### 📊 Оценка")
            estimated_leads = len(df) if process_all else min(max_rows or 10, len(df))
            estimated_requests = estimated_leads * (2 if not use_custom_query else 1)
            estimated_time_sec = estimated_requests * delay
            estimated_time_min = estimated_time_sec / 60

            col1, col2, col3 = st.columns(3)
            col1.metric("Лидов", estimated_leads)
            col2.metric("API запросов", f"~{estimated_requests}")
            col3.metric("Время (мин)", f"~{estimated_time_min:.1f}")

            st.markdown("### 🚀 Запуск")

            if st.button("🚀 НАЧАТЬ ОБОГАЩЕНИЕ", type="primary", use_container_width=True, disabled=st.session_state.processing):
                if not rapidapi_key:
                    st.error("❌ Укажи RapidAPI ключ!")
                else:
                    st.session_state.processing = True

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    results_placeholder = st.empty()

                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    temp_input = f"temp_input_{timestamp}.csv"
                    output_file = RESULTS_DIR / f"enriched_{timestamp}.csv"

                    df_renamed = df.rename(columns={
                        name_col: 'Person - Name',
                        email_col: 'Person - Email - Work'
                    })
                    df_renamed.to_csv(temp_input, index=False, encoding='utf-8-sig')

                    try:
                        status_text.info("⚙️ Инициализация workflow...")

                        workflow = EnrichmentWorkflow(
                            api_key=rapidapi_key,
                            batch_size=batch_size,
                            delay=delay
                        )

                        def update_progress(processed, total):
                            progress_bar.progress(processed / total)
                            status_text.info(f"Обработано: {processed}/{total}")

                        status_text.info("🚀 Запуск обогащения...")

                        stats = asyncio.run(
                            workflow.enrich_csv(
                                input_csv=temp_input,
                                output_csv=str(output_file),
                                max_leads=max_rows,
                                query_template=query_template if use_custom_query else None,
                                progress_callback=update_progress
                            )
                        )

                        os.remove(temp_input)

                        progress_bar.progress(1.0)
                        status_text.success("✅ Обогащение завершено!")

                        st.session_state.enriched_df = pd.read_csv(output_file, encoding='utf-8-sig')
                        st.session_state.stats = stats

                        col1, col2, col3 = st.columns(3)
                        col1.metric("Обработано", stats['processed'])
                        col2.metric("Найдено", stats['found'])
                        col3.metric("Success Rate", f"{stats['found']/stats['processed']*100:.1f}%")

                        st.dataframe(st.session_state.enriched_df.head(20))

                        csv_data = st.session_state.enriched_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "⬇️ Скачать результаты",
                            data=csv_data,
                            file_name=f"enriched_{timestamp}.csv",
                            mime='text/csv'
                        )

                    except Exception as e:
                        status_text.error(f"❌ Ошибка: {e}")
                        import traceback
                        st.error(traceback.format_exc())

                    finally:
                        st.session_state.processing = False

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

            total = len(df_history)
            found = len(df_history[df_history.get('Status') == 'Found']) if 'Status' in df_history.columns else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Всего", total)
            col2.metric("Найдено", found)
            col3.metric("Success Rate", f"{found/total*100:.1f}%" if total > 0 else "0%")

            st.dataframe(df_history, use_container_width=True)

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
    st.subheader("📖 Документация")

    st.markdown("""
    ## 🎯 Быстрый старт

    1. **Загрузи CSV** с лидами (колонки: Person - Name, Person - Email - Work)
    2. **Выбери пресет** (Safe для новичков)
    3. **Настрой параметры** (опционально)
    4. **Запусти обогащение**

    ## 📊 Пресеты

    ### 🛡️ Safe (Рекомендуется)
    - **Delay:** 1.0 сек (строго соответствует RapidAPI лимиту)
    - **Batch:** 1 лид
    - **Queries:** 2 на лид (email + name fallback)
    - **Скорость:** ~30 лидов/час
    - **Риск:** Минимальный

    ### ⚖️ Balanced
    - **Delay:** 1.2 сек (с запасом)
    - **Batch:** 5 лидов
    - **Queries:** 1 на лид
    - **Скорость:** ~50 лидов/час
    - **Риск:** Низкий

    ## 🔧 Настройки

    ### Batch Size
    Количество лидов обрабатываемых параллельно в асинхронном режиме.
    - **1-5:** Оптимально для rate limit 1 req/sec
    - **Больше:** Выше риск блокировки

    ### Delay
    Задержка между запросами к API.
    - **Минимум 1.0 сек** для RapidAPI (1 request/second)
    - **Рекомендуется 1.1-1.2 сек** для безопасности

    ### Кастомный Query Template
    Позволяет полностью контролировать поисковый запрос.

    **Переменные:**
    - `{name}` - имя лида
    - `{email}` - email лида

    **Примеры:**
    ```
    "{email}" instagram
    "{name}" instagram influencer
    "{name}" "{email}" instagram site:instagram.com
    ```

    **Когда использовать:**
    - Для специфических ниш (e.g., "fitness instagram")
    - Для точного таргетинга
    - Для экспериментов с разными стратегиями

    ## ⚡ RapidAPI Лимиты

    **Твой план:**
    - 500,000 запросов/месяц
    - **1 запрос/секунду**
    - Bandwidth: 10 GB/месяц

    **Рекомендации:**
    - Используй delay ≥ 1.0 сек
    - Начинай с малых тестов (10-50 лидов)
    - Мониторь rate limit в логах

    ## 🐛 Troubleshooting

    **Rate Limit Error:**
    - Увеличь delay до 1.5-2.0 сек
    - Уменьши batch size до 1
    - Подожди 1-2 минуты между запусками

    **Низкий Success Rate:**
    - Используй кастомный query template
    - Попробуй стратегию с 2 queries (email + name)
    - Проверь качество данных в CSV

    **Timeout Errors:**
    - Проверь интернет соединение
    - Проверь валидность RapidAPI ключа
    """)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Статус")
if st.session_state.processing:
    st.sidebar.success("🟢 Обработка...")
else:
    st.sidebar.info("⚪ Готов к запуску")

if st.session_state.stats:
    st.sidebar.markdown("### 📈 Последний запуск")
    st.sidebar.metric("Найдено", st.session_state.stats['found'])
    st.sidebar.metric("API запросов", st.session_state.stats['api_requests'])
