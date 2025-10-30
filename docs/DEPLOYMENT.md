# Deployment Guide — Развёртывание на сервере

Руководство по деплою sport-news-bot (PROSPORT) на production сервере.

## 📋 Оглавление

- [Требования](#требования)
- [Вариант 1: Docker (рекомендуется)](#вариант-1-docker-рекомендуется)
- [Вариант 2: Systemd без Docker](#вариант-2-systemd-без-docker)
- [Nginx конфигурация](#nginx-конфигурация)
- [SSL/TLS (Let's Encrypt)](#ssltls-lets-encrypt)
- [Мониторинг и логи](#мониторинг-и-логи)
- [Резервное копирование](#резервное-копирование)
- [Обновление проекта](#обновление-проекта)

---

## Требования

### Сервер

**Минимальные требования:**
- OS: Ubuntu 22.04 LTS / Debian 11+ / CentOS 8+
- RAM: 2GB (рекомендуется 4GB)
- CPU: 2 cores (рекомендуется 4)
- Disk: 10GB (для БД, логов, Docker образов)
- Python: 3.12+ (если без Docker)

**Рекомендуемые провайдеры:**
- DigitalOcean (Droplet $12/месяц)
- Hetzner Cloud (CX21 €5.83/месяц)
- AWS EC2 (t3.small)
- Selectel (VDS)

### Доменное имя (опционально)

Для веб-интерфейса с HTTPS:
- Зарегистрировать домен (example.com)
- Настроить A-запись: `prosport.example.com → IP_СЕРВЕРА`

---

## Вариант 1: Docker (рекомендуется)

### 1. Подготовка сервера

```bash
# Подключиться к серверу
ssh root@your-server-ip

# Обновить систему
apt update && apt upgrade -y

# Установить Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Установить Docker Compose
apt install docker-compose-plugin -y

# Проверить установку
docker --version
docker compose version

# Создать пользователя для приложения (безопасность)
useradd -m -s /bin/bash prosport
usermod -aG docker prosport
```

### 2. Клонирование проекта

```bash
# Переключиться на пользователя prosport
su - prosport

# Клонировать репозиторий
git clone https://github.com/VladPatr96/sport-bot.git /home/prosport/sport-news-bot
cd /home/prosport/sport-news-bot

# Переключиться на production ветку
git checkout main  # или stable
```

### 3. Конфигурация

```bash
# Создать .env файл
cp .env.example .env
nano .env

# Заполнить переменные:
# TG_BOT_TOKEN=ваш_токен_от_BotFather
# TG_CHANNEL_ID=-100XXXXXXXXXXXXX
# WEBAPP_PORT=8000
# WEBAPP_BASIC_AUTH=username:password  # опционально
```

### 4. Инициализация БД

```bash
# Создать директории
mkdir -p database database/logs mappings

# Применить миграции
docker compose run --rm bot python scripts/db_migrate.py

# Или вручную скопировать существующую БД
scp database/prosport.db prosport@your-server:/home/prosport/sport-news-bot/database/
```

### 5. Запуск сервисов

```bash
# Запустить все сервисы
docker compose up -d

# Проверить статус
docker compose ps

# Посмотреть логи
docker compose logs -f bot
docker compose logs -f webapp

# Проверить, что контейнеры работают
docker compose ps
# Должно быть:
# prosport-bot      running
# prosport-webapp   running
```

### 6. Автозапуск при перезагрузке

Docker Compose уже настроен с `restart: unless-stopped`, но для гарантии:

```bash
# Включить автозапуск Docker
sudo systemctl enable docker

# Создать systemd service для docker-compose
sudo nano /etc/systemd/system/prosport.service
```

Содержимое файла:

```ini
[Unit]
Description=Prosport News Bot
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/prosport/sport-news-bot
User=prosport
Group=prosport

ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down

[Install]
WantedBy=multi-user.target
```

Активировать:

```bash
sudo systemctl daemon-reload
sudo systemctl enable prosport
sudo systemctl start prosport
```

### 7. Первый запуск парсера

```bash
# Запустить парсер вручную (первый раз)
docker compose run --rm parser

# Или настроить cron для регулярного запуска
crontab -e

# Добавить строку (каждые 15 минут):
*/15 * * * * cd /home/prosport/sport-news-bot && /usr/bin/docker compose run --rm parser >> /home/prosport/logs/parser.log 2>&1
```

---

## Вариант 2: Systemd без Docker

Если Docker не подходит, можно запустить через systemd.

### 1. Установка зависимостей

```bash
# Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.12 python3.12-venv python3.12-dev

# Chromium и ChromeDriver
sudo apt install chromium-browser chromium-chromedriver

# Системные библиотеки
sudo apt install build-essential libssl-dev libffi-dev python3-dev
```

### 2. Настройка проекта

```bash
# Создать виртуальное окружение
cd /home/prosport/sport-news-bot
python3.12 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -e .

# Загрузить spaCy модели
python -m spacy download ru_core_news_sm
python -m spacy download en_core_web_sm

# Применить миграции
python scripts/db_migrate.py
```

### 3. Systemd сервисы

#### Telegram Bot

```bash
sudo nano /etc/systemd/system/prosport-bot.service
```

```ini
[Unit]
Description=Prosport Telegram Bot
After=network.target

[Service]
Type=simple
User=prosport
Group=prosport
WorkingDirectory=/home/prosport/sport-news-bot
Environment="PATH=/home/prosport/sport-news-bot/venv/bin"
EnvironmentFile=/home/prosport/sport-news-bot/.env
ExecStart=/home/prosport/sport-news-bot/venv/bin/python bot/scheduler.py --loop
Restart=always
RestartSec=10

StandardOutput=append:/home/prosport/logs/bot-stdout.log
StandardError=append:/home/prosport/logs/bot-stderr.log

[Install]
WantedBy=multi-user.target
```

#### Web Application

```bash
sudo nano /etc/systemd/system/prosport-webapp.service
```

```ini
[Unit]
Description=Prosport Web Application
After=network.target

[Service]
Type=simple
User=prosport
Group=prosport
WorkingDirectory=/home/prosport/sport-news-bot
Environment="PATH=/home/prosport/sport-news-bot/venv/bin"
EnvironmentFile=/home/prosport/sport-news-bot/.env
ExecStart=/home/prosport/sport-news-bot/venv/bin/python scripts/run_webapp_bot.py --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

StandardOutput=append:/home/prosport/logs/webapp-stdout.log
StandardError=append:/home/prosport/logs/webapp-stderr.log

[Install]
WantedBy=multi-user.target
```

#### Активация

```bash
# Создать директорию для логов
mkdir -p /home/prosport/logs

# Активировать сервисы
sudo systemctl daemon-reload
sudo systemctl enable prosport-bot prosport-webapp
sudo systemctl start prosport-bot prosport-webapp

# Проверить статус
sudo systemctl status prosport-bot
sudo systemctl status prosport-webapp

# Посмотреть логи
journalctl -u prosport-bot -f
journalctl -u prosport-webapp -f
```

---

## Nginx конфигурация

Для доступа к веб-интерфейсу через доменное имя.

### 1. Установка Nginx

```bash
sudo apt install nginx -y
sudo systemctl enable nginx
sudo systemctl start nginx
```

### 2. Конфигурация

```bash
sudo nano /etc/nginx/sites-available/prosport
```

```nginx
server {
    listen 80;
    server_name prosport.example.com;  # Ваш домен

    # Редирект на HTTPS (после настройки SSL)
    # return 301 https://$server_name$request_uri;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support (если понадобится)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Статические файлы (если есть)
    location /static/ {
        alias /home/prosport/sport-news-bot/webapp/static/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # Логи
    access_log /var/log/nginx/prosport-access.log;
    error_log /var/log/nginx/prosport-error.log;
}
```

### 3. Активация

```bash
# Создать символическую ссылку
sudo ln -s /etc/nginx/sites-available/prosport /etc/nginx/sites-enabled/

# Проверить конфигурацию
sudo nginx -t

# Перезагрузить Nginx
sudo systemctl reload nginx
```

Теперь веб-интерфейс доступен по адресу: <http://prosport.example.com>

---

## SSL/TLS (Let's Encrypt)

Для HTTPS с бесплатным сертификатом.

```bash
# Установить certbot
sudo apt install certbot python3-certbot-nginx -y

# Получить сертификат
sudo certbot --nginx -d prosport.example.com

# Certbot автоматически обновит nginx конфигурацию

# Проверить автообновление
sudo certbot renew --dry-run

# Готово! Теперь сайт доступен по HTTPS
```

---

## Мониторинг и логи

### 1. Логи Docker

```bash
# Логи контейнеров
docker compose logs -f bot
docker compose logs -f webapp

# Последние 100 строк
docker compose logs --tail=100 bot

# Логи с временными метками
docker compose logs -t bot
```

### 2. Логи Systemd

```bash
# Просмотр логов
journalctl -u prosport-bot -f
journalctl -u prosport-webapp -f

# Последние 100 строк
journalctl -u prosport-bot -n 100

# Логи за сегодня
journalctl -u prosport-bot --since today
```

### 3. Ротация логов

```bash
# Настроить logrotate
sudo nano /etc/logrotate.d/prosport
```

```
/home/prosport/logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 prosport prosport
    sharedscripts
    postrotate
        systemctl reload prosport-bot prosport-webapp
    endscript
}
```

### 4. Мониторинг скриптом

```bash
# Запустить monitor с loop
docker compose --profile monitoring up -d

# Или через systemd
sudo systemctl start prosport-monitor
```

Метрики будут сохраняться в `database/prosport.db` (таблица `monitor_logs`).

---

## Резервное копирование

### 1. Автоматический backup БД

```bash
# Создать скрипт backup
nano /home/prosport/backup.sh
```

```bash
#!/bin/bash
BACKUP_DIR="/home/prosport/backups"
DB_PATH="/home/prosport/sport-news-bot/database/prosport.db"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# Создать backup
sqlite3 $DB_PATH ".backup $BACKUP_DIR/prosport_$DATE.db"

# Сжать
gzip $BACKUP_DIR/prosport_$DATE.db

# Удалить старые backup (старше 30 дней)
find $BACKUP_DIR -name "prosport_*.db.gz" -mtime +30 -delete

echo "Backup created: prosport_$DATE.db.gz"
```

```bash
# Сделать исполняемым
chmod +x /home/prosport/backup.sh

# Добавить в cron (каждый день в 3:00)
crontab -e
0 3 * * * /home/prosport/backup.sh >> /home/prosport/logs/backup.log 2>&1
```

### 2. Backup на внешний сервер

```bash
# Через rsync (на другой сервер)
rsync -avz /home/prosport/backups/ backup-server:/backups/prosport/

# Или в облако (AWS S3, Yandex Object Storage)
aws s3 sync /home/prosport/backups/ s3://my-bucket/prosport-backups/
```

---

## Обновление проекта

### Обновление через Git

```bash
cd /home/prosport/sport-news-bot

# Остановить сервисы
docker compose down

# Обновить код
git pull origin main

# Пересобрать образы (если изменились зависимости)
docker compose build

# Применить миграции (если есть новые)
docker compose run --rm bot python scripts/db_migrate.py

# Запустить сервисы
docker compose up -d

# Проверить логи
docker compose logs -f
```

### Zero-downtime deployment

Для обновления без остановки:

```bash
# 1. Запустить новый контейнер на другом порту
docker compose -f docker-compose.prod.yml up -d

# 2. Проверить, что новый контейнер работает
curl http://localhost:8001/health

# 3. Переключить nginx на новый порт
sudo nano /etc/nginx/sites-available/prosport
# proxy_pass http://127.0.0.1:8001;

sudo nginx -t && sudo systemctl reload nginx

# 4. Остановить старый контейнер
docker compose down
```

---

## Troubleshooting

### Проблема: Контейнер не запускается

```bash
# Посмотреть логи
docker compose logs bot

# Проверить .env файл
cat .env | grep TG_BOT_TOKEN

# Пересоздать контейнер
docker compose up -d --force-recreate bot
```

### Проблема: БД locked

```bash
# Проверить, кто держит БД
lsof database/prosport.db

# Остановить все процессы
docker compose down

# Удалить lock файлы
rm -f database/*.db-wal database/*.db-shm
```

### Проблема: Парсер не работает

```bash
# Проверить ChromeDriver
docker compose run --rm parser which chromedriver

# Запустить в debug режиме
docker compose run --rm parser python scripts/sync_champ_news.py --debug
```

---

## Checklist развёртывания

- [ ] Сервер настроен (Docker / Python 3.12)
- [ ] Проект склонирован
- [ ] `.env` файл создан и заполнен
- [ ] БД инициализирована (миграции применены)
- [ ] Docker контейнеры запущены
- [ ] Nginx настроен (если нужен веб-доступ)
- [ ] SSL сертификат установлен
- [ ] Автозапуск при перезагрузке настроен
- [ ] Backup скрипт настроен
- [ ] Логирование работает
- [ ] Мониторинг настроен

---

## Полезные команды

```bash
# Статус всех сервисов
docker compose ps

# Перезапуск сервиса
docker compose restart bot

# Просмотр ресурсов
docker stats

# Вход в контейнер
docker compose exec bot bash

# Очистка старых образов
docker system prune -a

# Backup БД
sqlite3 database/prosport.db ".backup database/backup.db"

# Проверка здоровья
curl http://localhost:8000/
```

---

**Готово!** Теперь проект развёрнут на production сервере. 🚀
