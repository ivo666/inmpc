import pandas as pd
import numpy as np
import math
from typing import List, Dict, Any, Tuple
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.etl.base import BaseETL
from src.models.database import get_db


class PositionsGeneratorETL(BaseETL):
    """Генерация позиций для ppl.webmaster_positions"""
    
    def __init__(self):
        super().__init__()
        self.source_table = f"{self.schema}.webmaster_aggregated"
        self.target_table = f"{self.schema}.webmaster_positions"
    
    def extract(self) -> pd.DataFrame:
        """Извлекаем строки без сгенерированных позиций"""
        self.logger.info("🔍 Поиск данных для генерации позиций...")
        
        with get_db() as db:
            query = f"""
            SELECT 
                wa.id, wa.impressions, wa.clicks, wa.position
            FROM {self.source_table} wa
            WHERE wa.impressions > 0 
              AND NOT EXISTS (
                  SELECT 1 FROM {self.target_table} wp 
                  WHERE wp.id = wa.id
              )
            ORDER BY wa.id
            """
            
            result = db.execute(query)
            columns = result.keys()
            data = result.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=columns)
                # Преобразуем типы
                df['id'] = df['id'].astype(int)
                df['impressions'] = df['impressions'].astype(int)
                df['clicks'] = df['clicks'].astype(int)
                df['position'] = df['position'].astype(float)
                
                self.logger.info(f"📈 Найдено {len(df)} строк для генерации позиций")
                return df
            else:
                return pd.DataFrame()
    
    def _generate_positions_array(self, impressions: int, avg_position: float) -> List[int]:
        """Генерация массива позиций"""
        if impressions == 0:
            return []
        
        round_position = int(round(avg_position - 0.01))
        sum_of_positions = int(math.ceil(avg_position * impressions))
        
        min_pos = max(1, math.floor(avg_position - 1.5))
        max_pos = math.ceil(avg_position + 1.5)
        
        p = max(0.05, min(0.95, (avg_position - min_pos) / (max_pos - min_pos)))
        
        positions = []
        for _ in range(impressions):
            binomial_result = 0
            for _ in range(max_pos - min_pos):
                if np.random.random() < p:
                    binomial_result += 1
            position = min_pos + binomial_result
            positions.append(int(position))
        
        # Корректируем сумму
        current_sum = sum(positions)
        diff = sum_of_positions - current_sum
        
        if diff > 0:
            sorted_indices = np.argsort(positions)
            for i in range(min(diff, len(positions))):
                positions[sorted_indices[i]] += 1
        elif diff < 0:
            sorted_indices = np.argsort(positions)[::-1]
            for i in range(min(abs(diff), len(positions))):
                positions[sorted_indices[i]] = max(1, positions[sorted_indices[i]] - 1)
        
        return positions
    
    def transform(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Генерируем позиции для всех строк"""
        if df.empty:
            return []
        
        self.logger.info("🎲 Генерация позиций...")
        
        all_positions = []
        for _, row in df.iterrows():
            row_id = int(row['id'])
            impressions = int(row['impressions'])
            avg_position = float(row['position'])
            
            positions = self._generate_positions_array(impressions, avg_position)
            
            # Сохраняем позиции с порядковыми номерами
            for order, pos in enumerate(positions, 1):
                all_positions.append({
                    'id': row_id,
                    'impression_position': int(pos),
                    'impression_order': int(order)
                })
        
        self.logger.info(f"🎯 Сгенерировано {len(all_positions)} позиций")
        return all_positions
    
    def load(self, data: List[Dict[str, Any]]) -> int:
        """Сохраняем позиции в БД"""
        if not data:
            return 0
        
        self.logger.info(f"💾 Сохранение {len(data)} позиций...")
        
        with get_db() as db:
            for item in data:
                insert_query = f"""
                INSERT INTO {self.target_table} 
                (id, impression_position, impression_order)
                VALUES (:id, :impression_position, :impression_order)
                """
                
                db.execute(insert_query, {
                    'id': int(item['id']),
                    'impression_position': int(item['impression_position']),
                    'impression_order': int(item['impression_order'])
                })
        
        return len(data)