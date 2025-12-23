#!/usr/bin/env python3
"""Скрипт для запуска через cron"""

import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.collector import DataCollector
from src.utils.logger import setup_logger
from datetime import datetime

def main():
    """Основная функция для cron"""
    # Настройка логирования
    setup_logger()
    
    print(f"\n{'='*60}")
    print(f"АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    try:
        # Создаем сборщик
        collector = DataCollector()
        
        # Запускаем синхронизацию
        results = collector.run_sync()
        
        # Выводим итоги
        total_records = sum(results.values())
        if total_records > 0:
            print(f"✅ Успешно загружено {total_records} записей за {len(results)} дат")
        else:
            print("ℹ️  Нет новых данных для загрузки")
            
        return 0
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
