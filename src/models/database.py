from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator
from config.settings import settings

# Создаем базовый класс для моделей
Base = declarative_base()

class WebmasterData(Base):
    """Модель для данных Яндекс.Вебмастер"""
    __tablename__ = 'webmaster'
    __table_args__ = {'schema': 'rdl'}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    page_path = Column(Text, nullable=False, index=True)
    query = Column(Text, nullable=False, index=True)
    demand = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    position = Column(Float, nullable=False)
    device = Column(String(20), nullable=False, index=True)
    
    def __repr__(self):
        return f"<WebmasterData(date={self.date}, query={self.query[:30]}..., device={self.device})>"

# Создаем движок базы данных
engine = create_engine(
    settings.db.connection_string,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

# Создаем фабрику сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db() -> Generator:
    """Контекстный менеджер для работы с БД"""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def create_tables():
    """Создает таблицы в БД"""
    Base.metadata.create_all(bind=engine)