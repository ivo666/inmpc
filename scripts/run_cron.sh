#!/bin/bash
# Обертка для запуска скрипта через cron

# Переходим в директорию проекта
cd /opt/inmpc

# Активируем виртуальное окружение
source venv/bin/activate

# Создаем директорию для логов если нет
mkdir -p logs

# Запускаем скрипт с логированием
LOG_FILE="logs/cron_$(date +\%Y\%m\%d).log"
TIMESTAMP=$(date +"\%Y-\%m-\%d \%H:\%M:\%S")

echo "=== Запуск синхронизации $TIMESTAMP ===" >> "$LOG_FILE"
python scripts/cron_runner.py >> "$LOG_FILE" 2>&1

# Добавляем разделитель в лог
echo -e "\n---\n" >> "$LOG_FILE"
