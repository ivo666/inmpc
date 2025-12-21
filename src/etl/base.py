import sys
from pathlib import Path
from abc import ABC, abstractmethod
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import settings
from src.models.database import get_db


class BaseETL(ABC):
    """Базовый класс для всех ETL процессов"""
    
    def __init__(self):
        self.logger = logger
        self.schema = "ppl"
    
    @abstractmethod
    def extract(self):
        """Извлечение данных из источника"""
        pass
    
    @abstractmethod
    def transform(self, data):
        """Преобразование данных"""
        pass
    
    @abstractmethod
    def load(self, data):
        """Загрузка данных в целевую таблицу"""
        pass
    
    def run(self):
        """Запуск полного ETL процесса"""
        try:
            self.logger.info(f"🚀 Запуск {self.__class__.__name__}")
            
            # Extract
            extracted_data = self.extract()
            if extracted_data.empty:
                self.logger.info("ℹ️ Нет новых данных для обработки")
                return 0
            
            # Transform
            transformed_data = self.transform(extracted_data)
            
            # Load
            loaded_count = self.load(transformed_data)
            
            self.logger.success(f"✅ {self.__class__.__name__} завершен: {loaded_count} строк")
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка в {self.__class__.__name__}: {e}")
            raise