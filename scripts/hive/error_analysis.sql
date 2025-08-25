USE eclog_analysis;

-- 4. Análisis Detallado de Errores HTTP
DROP TABLE IF EXISTS error_analysis;
CREATE TABLE error_analysis AS
SELECT 
    response_code,
    status_category,
    uri,
    COUNT(*) as error_count,
    COUNT(DISTINCT ip_id) as unique_ips_affected,
    COUNT(DISTINCT user_id) as unique_users_affected,
    MIN(request_timestamp) as first_occurrence,    -- Cambiado de 'timestamp' a 'request_timestamp'
    MAX(request_timestamp) as last_occurrence,     -- Cambiado de 'timestamp' a 'request_timestamp'
    COUNT(DISTINCT hour) as hours_with_errors,
    COUNT(DISTINCT http_method) as different_methods,
    COUNT(DISTINCT referrer) as different_referrers
FROM eclog_processed
WHERE response_code >= 400
GROUP BY response_code, status_category, uri;

-- Resumen de errores por código
SELECT 
    response_code,
    status_category,
    SUM(error_count) as total_errors,
    COUNT(DISTINCT uri) as unique_uris_affected,
    SUM(unique_ips_affected) as total_unique_ips_affected,  -- Cambiado para evitar doble COUNT DISTINCT
    ROUND(AVG(error_count), 2) as avg_errors_per_uri
FROM error_analysis
GROUP BY response_code, status_category
ORDER BY total_errors DESC;

-- Top 20 páginas con más errores
SELECT 
    uri,
    SUM(error_count) as total_errors,
    COUNT(DISTINCT response_code) as different_error_codes,
    -- Usar COLLECT_SET en lugar de COLLECT_LIST si está disponible
    CONCAT_WS(', ', COLLECT_SET(CAST(response_code AS STRING))) as error_codes
FROM error_analysis
GROUP BY uri
ORDER BY total_errors DESC
LIMIT 20;