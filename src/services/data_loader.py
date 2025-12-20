from typing import List, Dict, Any
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.models.database import get_db, WebmasterData
from src.api.webmaster_client import WebmasterClient


class DataLoader:
    def __init__(self, client: WebmasterClient):
        self.client = client
        self.device_types = ['DESKTOP', 'MOBILE', 'TABLET']
    
    def load_data_for_date(self, target_date: str) -> int:
        print(f"Загрузка данных за {target_date}...")
        total_records = 0
        
        # Временно: берем только тестовый URL для проверки
        test_urls = ['/sale']  # Позже заменим на получение всех URL
        
        for url in test_urls:
            for device in self.device_types:
                records = self.client.get_queries_for_url_and_date(target_date, url, device)
                saved = self._save_records(records)
                total_records += saved
        
        print(f"Загружено {total_records} записей за {target_date}")
        return total_records
    
    def _save_records(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0
        
        saved_count = 0
        try:
            with get_db() as db:
                for record_data in records:
                    # Проверяем дубликаты
                    exists = db.query(WebmasterData).filter(
                        WebmasterData.date == record_data['date'],
                        WebmasterData.page_path == record_data['page_path'],
                        WebmasterData.query == record_data['query'],
                        WebmasterData.device == record_data['device']
                    ).first()
                    
                    if not exists:
                        record = WebmasterData(**record_data)
                        db.add(record)
                        saved_count += 1
                
                print(f"Добавлено {saved_count} новых записей")
                
        except Exception as e:
            print(f"Ошибка при сохранении: {e}")
        
        return saved_count