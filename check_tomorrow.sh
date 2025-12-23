#!/bin/bash
echo "=== Проверка выполнения задач за 22 декабря ==="
echo "Время: $(date)"
echo

echo "1. Файлы логов:"
ls -la /opt/inmpc/logs/cron_*.log /opt/inmpc/logs/etl_*.log 2>/dev/null
echo

echo "2. Содержимое логов сборщика:"
if [ -f /opt/inmpc/logs/cron_20251222.log ]; then
    echo "Последние 20 строк cron_20251222.log:"
    tail -20 /opt/inmpc/logs/cron_20251222.log
else
    echo "Лог cron_20251222.log не найден"
fi
echo

echo "3. Содержимое логов ETL:"
if [ -f /opt/inmpc/logs/etl_20251222.log ]; then
    echo "Последние 20 строк etl_20251222.log:"
    tail -20 /opt/inmpc/logs/etl_20251222.log
else
    echo "Лог etl_20251222.log не найден"
fi
echo

echo "4. Данные за 21 декабря:"
psql -U postgres -d inmpc -c "
SELECT 
    'rdl.webmaster' as source,
    COUNT(*) as records,
    CASE WHEN COUNT(*) > 0 THEN '✅ Есть данные' ELSE '❌ Нет данных' END as status
FROM rdl.webmaster 
WHERE date = '2025-12-21'
UNION ALL
SELECT 
    'ppl.webmaster_aggregated',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ Есть данные' ELSE '❌ Нет данных' END
FROM ppl.webmaster_aggregated 
WHERE date = '2025-12-21';
"
