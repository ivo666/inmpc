import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
import sys
from pathlib import Path
from sqlalchemy import text  # Добавляем импорт

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.etl.base import BaseETL
from src.models.database import get_db


class ClicksGeneratorETL(BaseETL):
    """Генерация кликов для ppl.webmaster_clicks"""
    
    def __init__(self):
        super().__init__()
        self.source_table = f"{self.schema}.webmaster_aggregated"
        self.positions_table = f"{self.schema}.webmaster_positions"
        self.target_table = f"{self.schema}.webmaster_clicks"
    
    def extract(self) -> pd.DataFrame:
        """Извлекаем строки с кликами без сгенерированных кликов"""
        self.logger.info("🔍 Поиск данных для генерации кликов...")
        
        with get_db() as db:
            query = text(f"""
            SELECT 
                wa.id, wa.clicks
            FROM {self.source_table} wa
            WHERE wa.clicks > 0 
              AND NOT EXISTS (
                  SELECT 1 FROM {self.target_table} wc 
                  WHERE wc.id = wa.id
              )
            ORDER BY wa.id
            """)
            
            result = db.execute(query)
            columns = result.keys()
            data = result.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=columns)
                df['id'] = df['id'].astype(int)
                df['clicks'] = df['clicks'].astype(int)
                
                self.logger.info(f"📈 Найдено {len(df)} строк для генерации кликов")
                return df
            else:
                return pd.DataFrame()
    
    def _get_positions_for_id(self, row_id: int) -> List[Tuple[int, int]]:
        """Получаем позиции для конкретного ID"""
        with get_db() as db:
            query = text(f"""
            SELECT impression_position, impression_order 
            FROM {self.positions_table}
            WHERE id = :row_id
            ORDER BY impression_order
            """)
            
            result = db.execute(query, {'row_id': row_id})
            return [(int(row[0]), int(row[1])) for row in result.fetchall()]
    
    def _distribute_clicks(self, row_id: int, clicks: int, positions_with_order: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
        """Распределение кликов по показам"""
        if clicks == 0 or len(positions_with_order) == 0:
            return []
        
        position_weights = {
            1: 0.30, 2: 0.15, 3: 0.08, 4: 0.05, 5: 0.03,
            6: 0.02, 7: 0.015, 8: 0.012, 9: 0.01, 10: 0.008
        }
        
        weights = []
        for pos, order in positions_with_order:
            weight = position_weights.get(pos, 0.005)
            time_weight = 1.0 / (order * 0.1 + 1)
            weights.append(weight * time_weight)
        
        total_weight = sum(weights)
        if total_weight == 0:
            weights = [1.0 / len(positions_with_order)] * len(positions_with_order)
        else:
            weights = [w / total_weight for w in weights]
        
        if clicks <= len(positions_with_order):
            chosen_indices = np.random.choice(
                len(positions_with_order), 
                size=clicks, 
                replace=False, 
                p=weights
            )
        else:
            chosen_indices = np.random.choice(
                len(positions_with_order), 
                size=clicks, 
                replace=True, 
                p=weights
            )
        
        result = []
        for idx in chosen_indices:
            pos, order = positions_with_order[idx]
            result.append({
                'id': row_id,
                'click_position': int(pos),
                'impression_order': int(order)
            })
        
        return result
    
    def transform(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Генерируем клики для всех строк"""
        if df.empty:
            return []
        
        self.logger.info("🎲 Распределение кликов...")
        
        all_clicks = []
        for _, row in df.iterrows():
            row_id = int(row['id'])
            clicks = int(row['clicks'])
            
            # Получаем позиции для этого ID
            positions = self._get_positions_for_id(row_id)
            
            if positions:
                click_assignments = self._distribute_clicks(row_id, clicks, positions)
                all_clicks.extend(click_assignments)
        
        self.logger.info(f"🎯 Сгенерировано {len(all_clicks)} кликов")
        return all_clicks
    
    def load(self, data: List[Dict[str, Any]]) -> int:
        """Сохраняем клики в БД"""
        if not data:
            return 0
        
        self.logger.info(f"💾 Сохранение {len(data)} кликов...")
        
        with get_db() as db:
            for item in data:
                insert_query = text(f"""
                INSERT INTO {self.target_table} 
                (id, click_position, impression_order)
                VALUES (:id, :click_position, :impression_order)
                """)
                
                db.execute(insert_query, {
                    'id': int(item['id']),
                    'click_position': int(item['click_position']),
                    'impression_order': int(item['impression_order'])
                })
        
        return len(data)