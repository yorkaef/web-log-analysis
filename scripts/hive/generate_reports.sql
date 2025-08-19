USE eclog_analysis;

-- Reporte Ejecutivo
SELECT '=== REPORTE EJECUTIVO EClog ===' as report_section;

SELECT 
    'Período de Análisis' as metric,
    CONCAT(MIN(date), ' a ', MAX(date)) as value
FROM eclog_processed;

SELECT 
    'Total de Requests' as metric,
    FORMAT_NUMBER(COUNT(*), 0) as value
FROM eclog_processed
UNION ALL
SELECT 
    'IPs Únicas' as metric,
    FORMAT_NUMBER(COUNT(DISTINCT ip_id), 0) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Usuarios Únicos' as metric,
    FORMAT_NUMBER(COUNT(DISTINCT user_id), 0) as value  
FROM eclog_processed
UNION ALL
SELECT 
    'Datos Transferidos (GB)' as metric,
    FORMAT_NUMBER(SUM(bytes) / (1024*1024*1024), 2) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Tasa de Error Global (%)' as metric,
    FORMAT_NUMBER(
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as value
FROM eclog_processed;

-- Top 5 países por tráfico
SELECT '=== TOP 5 PAÍSES ===' as report_section;
SELECT 
    country,
    FORMAT_NUMBER(total_requests, 0) as requests,
    FORMAT_NUMBER(unique_ips, 0) as unique_ips,
    CONCAT(error_rate_percent, '%') as error_rate
FROM country_stats
LIMIT 5;

-- Horas pico de tráfico
SELECT '=== HORAS PICO ===' as report_section;
SELECT 
    hour,
    FORMAT_NUMBER(total_requests, 0) as requests,
    FORMAT_NUMBER(unique_ips, 0) as unique_ips,
    CONCAT(error_rate_percent, '%') as error_rate
FROM hourly_stats
ORDER BY total_requests DESC
LIMIT 5;

-- Top errores más comunes
SELECT '=== ERRORES MÁS COMUNES ===' as report_section;
SELECT 
    response_code,
    status_category,
    FORMAT_NUMBER(SUM(error_count), 0) as total_occurrences,
    COUNT(DISTINCT uri) as unique_pages_affected
FROM error_analysis
GROUP BY response_code, status_category
ORDER BY SUM(error_count) DESC
LIMIT 10;