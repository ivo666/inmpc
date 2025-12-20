import sys
from pathlib import Path
from loguru import logger

from config.settings import settings


def setup_logger():
    """Настройка логирования"""
    log_path = Path("logs")
    log_path.mkdir(exist_ok=True)
    
    # Удаляем стандартный обработчик
    logger.remove()
    
    # Консольный вывод
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=settings.app.log_level,
        colorize=True
    )
    
    # Файловый вывод
    logger.add(
        log_path / "webmaster_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=settings.app.log_level,
        rotation="00:00",
        retention="30 days",
        compression="zip"
    )
    
    return logger