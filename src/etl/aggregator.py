import pandas as pd
import numpy as np
from typing import Tuple
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.etl.base import BaseETL
from src.models.database import get_db


class AggregatedETL(BaseETL):
    """ETL для загрузки данных в ppl.webmaster_aggregated"""
    
    def __init__(self):
        super().__init__()
        self.source_table = "rdl.webmaster"
        self.target_table = f"{self.schema}.webmaster_aggregated"
    
    def extract(self) -> pd.DataFrame:
        """Извлекаем новые данные из rdl.webmaster"""
        self.logger.info("🔍 Извлечение новых данных...")
        
        with get_db() as db:
            # Получаем максимальную дату из целевой таблицы
            max_date_query = f"""
                SELECT COALESCE(MAX(date), '2000-01-01') as max_date 
                FROM {self.target_table}
            """
            max_date = db.execute(max_date_query).fetchone()[0]
            self.logger.info(f"📅 Последняя дата в целевой таблице: {max_date}")
            
            # Загружаем данные, которых нет в целевой таблице
            query = f"""
            SELECT * FROM {self.source_table} 
            WHERE date > :max_date 
               OR (date = :max_date AND NOT EXISTS (
                   SELECT 1 FROM {self.target_table} p 
                   WHERE p.date = {self.source_table}.date 
                     AND p.query = {self.source_table}.query 
                     AND p.page_path = {self.source_table}.page_path
                     AND p.device = {self.source_table}.device
               ))
            ORDER BY date, query, page_path, device
            """
            
            result = db.execute(query, {"max_date": max_date})
            columns = result.keys()
            data = result.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=columns)
                self.logger.info(f"🆕 Найдено {len(df)} новых строк")
                return df
            else:
                return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяем бизнес-логику и готовим данные"""
        if df.empty:
            return df
        
        self.logger.info("🔧 Применение бизнес-логики...")
        df_processed = df.copy()
        
        # Заполняем NaN
        numeric_cols = ['demand', 'impressions', 'clicks', 'position']
        for col in numeric_cols:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna(0)
        
        # Преобразуем типы
        df_processed['demand'] = df_processed['demand'].astype(int)
        df_processed['impressions'] = df_processed['impressions'].astype(int)
        df_processed['clicks'] = df_processed['clicks'].astype(int)
        df_processed['position'] = df_processed['position'].astype(float)
        
        # Бизнес-логика
        df_processed['demand'] = np.where(
            df_processed['impressions'] > df_processed['demand'], 
            df_processed['impressions'], 
            df_processed['demand']
        )
        df_processed['clicks'] = np.where(
            df_processed['clicks'] > df_processed['impressions'], 
            df_processed['impressions'], 
            df_processed['clicks']
        )
        
        return df_processed
    
    def load(self, df: pd.DataFrame) -> int:
        """Загружаем данные в ppl.webmaster_aggregated"""
        if df.empty:
            return 0
        
        self.logger.info(f"💾 Сохранение {len(df)} строк...")
        
        with get_db() as db:
            # Получаем последний ID
            last_id_query = f"SELECT COALESCE(MAX(id), 0) FROM {self.target_table}"
            last_id = db.execute(last_id_query).fetchone()[0]
            
            # Создаем новые ID
            df['id'] = range(last_id + 1, last_id + len(df) + 1)
            
            # Вставляем данные
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {self.target_table} 
                (id, date, query, page_path, device, demand, impressions, clicks, position)
                VALUES (:id, :date, :query, :page_path, :device, :demand, :impressions, :clicks, :position)
                """
                
                db.execute(insert_query, {
                    'id': int(row['id']),
                    'date': row['date'],
                    'query': row['query'] if pd.notna(row['query']) else None,
                    'page_path': row['page_path'] if pd.notna(row['page_path']) else None,
                    'device': row['device'] if pd.notna(row['device']) else None,
                    'demand': int(row['demand']) if pd.notna(row['demand']) else 0,
                    'impressions': int(row['impressions']) if pd.notna(row['impressions']) else 0,
                    'clicks': int(row['clicks']) if pd.notna(row['clicks']) else 0,
                    'position': float(row['position']) if pd.notna(row['position']) else 0.0
                })
            
            self.logger.info(f"📈 Новый диапазон ID: {last_id + 1} - {last_id + len(df)}")
            return len(df)