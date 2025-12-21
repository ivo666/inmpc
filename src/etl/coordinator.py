import sys
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.etl.aggregator import AggregatedETL
from src.etl.positions_generator import PositionsGeneratorETL
from src.etl.clicks_generator import ClicksGeneratorETL


class ETLCoordinator:
    """Координатор всех ETL процессов"""
    
    def __init__(self):
        self.logger = logger
        self.aggregator = AggregatedETL()
        self.positions_generator = PositionsGeneratorETL()
        self.clicks_generator = ClicksGeneratorETL()
    
    def run_full_pipeline(self) -> dict:
        """Запуск полного ETL пайплайна"""
        self.logger.info("=" * 60)
        self.logger.info("🚀 ЗАПУСК ПОЛНОГО ETL ПАЙПЛАЙНА")
        self.logger.info("=" * 60)
        
        results = {}
        
        try:
            # Шаг 1: Загрузка агрегированных данных
            self.logger.info("\n📊 ШАГ 1: Загрузка в webmaster_aggregated")
            results['aggregated'] = self.aggregator.run()
            
            # Шаг 2: Генерация позиций
            self.logger.info("\n🎯 ШАГ 2: Генерация позиций")
            results['positions'] = self.positions_generator.run()
            
            # Шаг 3: Генерация кликов
            self.logger.info("\n🖱️ ШАГ 3: Генерация кликов")
            results['clicks'] = self.clicks_generator.run()
            
            # Финальная статистика
            self._print_statistics(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка в пайплайне: {e}")
            raise
    
    def _print_statistics(self, results: dict):
        """Печать статистики выполнения"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📈 СТАТИСТИКА ВЫПОЛНЕНИЯ")
        self.logger.info("=" * 60)
        
        total_rows = sum(results.values())
        self.logger.info(f"Всего обработано строк: {total_rows}")
        
        for process, count in results.items():
            self.logger.info(f"  • {process}: {count} строк")
    
    def check_data_consistency(self):
        """Проверка согласованности данных"""
        self.logger.info("\n🔍 ПРОВЕРКА СОГЛАСОВАННОСТИ ДАННЫХ")
        
        checks = [
            ("Строки без позиций", """
                SELECT COUNT(*) as missing_positions
                FROM ppl.webmaster_aggregated wa
                WHERE wa.impressions > 0 
                  AND NOT EXISTS (
                      SELECT 1 FROM ppl.webmaster_positions wp 
                      WHERE wp.id = wa.id
                  )
            """),
            ("Клики без позиций", """
                SELECT COUNT(*) as orphaned_clicks
                FROM ppl.webmaster_clicks wc
                WHERE NOT EXISTS (
                    SELECT 1 FROM ppl.webmaster_positions wp 
                    WHERE wp.id = wc.id AND wp.impression_order = wc.impression_order
                )
            """)
        ]
        
        from src.models.database import get_db
        
        with get_db() as db:
            for check_name, query in checks:
                result = db.execute(query).fetchone()
                self.logger.info(f"  • {check_name}: {result[0]}")