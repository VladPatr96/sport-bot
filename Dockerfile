# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Установка системных зависимостей для Chrome и Selenium
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /app

# Копирование файлов зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Загрузка spaCy моделей
RUN python -m spacy download ru_core_news_sm && \
    python -m spacy download en_core_web_sm

# Копирование исходного кода
COPY . .

# Создание директорий для данных (если не существуют)
RUN mkdir -p database database/logs mappings

# Переменные окружения по умолчанию
ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Moscow
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Healthcheck для контейнера
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Точка входа по умолчанию
CMD ["python", "bot/scheduler.py", "--loop"]
