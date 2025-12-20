import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Загружаем .env файл
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

class DatabaseSettings(BaseSettings):
    """Настройки базы данных"""
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', 5432))
    database: str = os.getenv('DB_NAME', 'inmpc')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', '')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class APISettings(BaseSettings):
    """Настройки API Яндекс.Вебмастер"""
    token: str = os.getenv('API_TOKEN', '')
    base_url: str = os.getenv('BASE_URL', 'https://api.webmaster.yandex.net/v4')
    user_id: str = os.getenv('USER_ID', '')
    host_id: str = os.getenv('HOST_ID', '')

class AppSettings(BaseSettings):
    """Настройки приложения"""
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    days_back: int = int(os.getenv('DAYS_BACK', 20))
    batch_size: int = int(os.getenv('BATCH_SIZE', 500))
    
    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"OAuth {self.api.token}",
            "Content-Type": "application/json"
        }

class Settings(BaseSettings):
    """Общие настройки приложения"""
    db: DatabaseSettings = DatabaseSettings()
    api: APISettings = APISettings()
    app: AppSettings = AppSettings()
    
    class Config:
        env_file = '.env'

# Создаем глобальный экземпляр настроек
settings = Settings()