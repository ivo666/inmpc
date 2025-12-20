#!/usr/bin/env python3
import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.collector import DataCollector


def main():
    print("=" * 60)
    print("СБОР ДАННЫХ ЯНДЕКС.ВЕБМАСТЕР (ООП версия)")
    print("=" * 60)
    
    try:
        collector = DataCollector()
        
        if not collector.test_connection():
            print("❌ Ошибка подключения")
            return 1
        
        results = collector.run_sync()
        
        total = sum(results.values())
        print(f"\nИтог: загружено {total} записей")
        return 0
        
    except KeyboardInterrupt:
        print("\nПрервано")
        return 130
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())