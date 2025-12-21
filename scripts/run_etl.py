#!/usr/bin/env python3
"""Точка входа для запуска ETL пайплайна"""

import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.coordinator import ETLCoordinator
from src.utils.logger import setup_logger


def main():
    """Основная функция"""
    # Настройка логирования
    setup_logger()
    
    print(f"\n{'='*60}")
    print("ETL ПАЙПЛАЙН: RDL → PPL")
    print("=" * 60)
    
    try:
        # Создаем координатор
        coordinator = ETLCoordinator()
        
        # Запускаем полный пайплайн
        results = coordinator.run_full_pipeline()
        
        # Проверяем согласованность
        coordinator.check_data_consistency()
        
        print(f"\n✅ ETL пайплайн успешно завершен!")
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Прервано пользователем")
        return 130
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())