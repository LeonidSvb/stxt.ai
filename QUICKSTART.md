# Quick Start

## Запуск Streamlit UI

```bash
streamlit run app.py
```

Откроется браузер на `http://localhost:8501`

## Использование CLI

### 1. Только поиск Instagram URLs (быстро и дешево)

```bash
python enrich_leads.py "C:\Users\79818\Downloads\leads_ai_girls - test.csv" --no-enrich --max-rows 10
```

### 2. Полное обогащение (с Apify)

```bash
python enrich_leads.py "C:\Users\79818\Downloads\leads_ai_girls - test.csv" --max-rows 10
```

### 3. Все строки с кастомной задержкой

```bash
python enrich_leads.py "C:\Users\79818\Downloads\leads_ai_girls - test.csv" --delay 3.0
```

## Результаты

Обогащенные файлы сохраняются в `results/` с timestamp:
- `leads_enriched_20251127_151234.csv`

## API Keys

Проверьте что в `.env` есть:
```
RAPIDAPI_KEY=your_rapidapi_key_here
APIFY_API_KEY=your_apify_key_here
```

## Структура проекта

```
stxt.ai/
├── app.py                     # Streamlit UI (ЗАПУСКАТЬ ОТСЮДА)
├── enrich_leads.py            # CLI скрипт
├── modules/                   # Атомарные модули
│   ├── google_search.py       # Поиск через Google
│   ├── instagram_scraper.py   # Обогащение через Apify
│   └── csv_handler.py         # Работа с CSV
├── results/                   # Результаты обогащения
├── .env                       # API ключи (НЕ коммитить!)
└── requirements.txt           # Python зависимости
```

## Workflow

1. Загружаешь CSV с лидами (имя + email)
2. Скрипт ищет Instagram профили через Google
3. (Опционально) Обогащает профили через Apify
4. Сохраняет результаты с Instagram URLs и данными

## Стоимость

- **Только поиск**: ~$0.004 за лид
- **С обогащением**: ~$0.007 за лид
- **10 лидов**: ~$0.07
- **100 лидов**: ~$0.70

## Что добавляется в CSV

✅ Instagram URL
✅ Username (@username)
✅ Full Name
✅ Bio
✅ Followers count
✅ Following count
✅ Posts count
✅ Verified status
✅ Business account info
✅ External URL

## Troubleshooting

### Нет Instagram профиля
- Проверьте имя/email в исходном CSV
- Попробуйте разные варианты запросов
- Некоторые профили могут быть приватными

### Apify ошибка
- Проверьте APIFY_API_KEY в .env
- Убедитесь что есть кредиты на балансе
- Попробуйте `--no-enrich` для быстрого теста

### RapidAPI лимит
- Бесплатный план: 100 запросов/месяц
- При превышении нужно апгрейд
