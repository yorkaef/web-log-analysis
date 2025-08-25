USE eclog_analysis;

-- Reporte Ejecutivo
SELECT '=== REPORTE EJECUTIVO EClog ===' as report_section;

SELECT 
    'Período de Análisis' as metric,
    CONCAT(
        CAST(MIN(log_date) AS STRING), 
        ' a ', 
        CAST(MAX(log_date) AS STRING)
    ) as value
FROM eclog_processed;

SELECT 
    'Total de Requests' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'IPs Únicas' as metric,
    CAST(COUNT(DISTINCT ip_id) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Usuarios Únicos' as metric,
    CAST(COUNT(DISTINCT user_id) AS STRING) as value  
FROM eclog_processed
UNION ALL
SELECT 
    'Datos Transferidos (GB)' as metric,
    CAST(ROUND(SUM(bytes) / (1024*1024*1024), 2) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Tasa de Error Global (%)' as metric,
    CAST(ROUND(
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) AS STRING) as value
FROM eclog_processed;

-- Top 5 países por tráfico
SELECT '=== TOP 5 PAÍSES ===' as report_section;
SELECT 
    country,
    CAST(total_requests AS STRING) as requests,
    CAST(unique_ips AS STRING) as unique_ips,
    CONCAT(CAST(error_rate_percent AS STRING), '%') as error_rate
FROM country_stats
ORDER BY total_requests DESC
LIMIT 5;

-- Horas pico de tráfico
SELECT '=== HORAS PICO ===' as report_section;
SELECT 
    hour,
    CAST(total_requests AS STRING) as requests,
    CAST(unique_ips AS STRING) as unique_ips,
    CONCAT(CAST(error_rate_percent AS STRING), '%') as error_rate
FROM hourly_stats
ORDER BY total_requests DESC
LIMIT 5;

-- Top errores más comunes
SELECT '=== ERRORES MÁS COMUNES ===' as report_section;
SELECT 
    response_code,
    status_category,
    CAST(SUM(error_count) AS STRING) as total_occurrences,
    CAST(COUNT(DISTINCT uri) AS STRING) as unique_pages_affected
FROM error_analysis
GROUP BY response_code, status_category
ORDER BY SUM(error_count) DESC
LIMIT 10;

-- Análisis de tráfico por hora del día
SELECT '=== DISTRIBUCIÓN HORARIA ===' as report_section;
SELECT 
    hour,
    CAST(total_requests AS STRING) as requests,
    CAST(ROUND(error_rate_percent, 1) AS STRING) as error_rate_pct,
    CASE 
        WHEN hour BETWEEN 0 AND 5 THEN 'Madrugada'
        WHEN hour BETWEEN 6 AND 11 THEN 'Mañana'
        WHEN hour BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN hour BETWEEN 18 AND 23 THEN 'Noche'
    END as period_name
FROM hourly_stats
ORDER BY total_requests DESC;

-- Resumen de tipos de recursos más solicitados
SELECT '=== TIPOS DE RECURSOS ===' as report_section;
SELECT 
    resource_type,
    CAST(COUNT(*) AS STRING) as total_requests,
    CAST(ROUND(COUNT(*) * 100.0 / 
        (SELECT COUNT(*) FROM eclog_processed), 1) AS STRING) as percentage,
    CAST(ROUND(AVG(bytes), 0) AS STRING) as avg_size_bytes
FROM eclog_processed
GROUP BY resource_type
ORDER BY COUNT(*) DESC;

-- Top IPs con más actividad
SELECT '=== TOP IPs ACTIVAS ===' as report_section;
SELECT 
    ip_id,
    country,
    CAST(total_requests AS STRING) as requests,
    user_classification,
    CAST(error_count AS STRING) as errors
FROM top_ips
ORDER BY total_requests DESC
LIMIT 10;

-- Estadísticas finales
SELECT '=== RESUMEN FINAL ===' as report_section;
SELECT 
    'Registros procesados exitosamente' as summary_item,
    CAST(COUNT(*) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Período de análisis (días)' as summary_item,
    CAST(DATEDIFF(MAX(log_date), MIN(log_date)) + 1 AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Promedio requests por día' as summary_item,
    CAST(ROUND(COUNT(*) / (DATEDIFF(MAX(log_date), MIN(log_date)) + 1), 0) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Países únicos detectados' as summary_item,
    CAST(COUNT(DISTINCT country) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Códigos de estado únicos' as summary_item,
    CAST(COUNT(DISTINCT response_code) AS STRING) as value
FROM eclog_processed;