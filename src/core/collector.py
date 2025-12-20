import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.api.webmaster_client import WebmasterClient
from src.services.date_manager import DateManager
from src.services.data_loader import DataLoader


class DataCollector:
    def __init__(self):
        print("Инициализация сборщика данных...")
        self.client = WebmasterClient()
        self.date_manager = DateManager(self.client)
        self.loader = DataLoader(self.client)
        print("Сборщик готов")
    
    def run_sync(self) -> dict:
        print("Запуск синхронизации...")
        
        # Получаем недостающие даты
        missing_dates = self.date_manager.get_missing_dates()
        print(f"Найдено недостающих дат: {len(missing_dates)}")
        
        # Загружаем данные
        results = {}
        for date_str in missing_dates:
            print(f"Обработка даты: {date_str}")
            records_count = self.loader.load_data_for_date(date_str)
            results[date_str] = records_count
        
        return results
    
    def test_connection(self) -> bool:
        print("Тест подключения к API...")
        print(f"USER_ID: {self.client.user_id}")
        print(f"HOST_ID: {self.client.host_id}")
        return True