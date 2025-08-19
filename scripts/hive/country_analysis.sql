USE eclog_analysis;

-- 1. Estadísticas por País
DROP TABLE IF EXISTS country_stats;
CREATE TABLE country_stats AS
SELECT 
    country,
    COUNT(*) as total_requests,
    COUNT(DISTINCT ip_id) as unique_ips,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(bytes) as avg_response_size,
    SUM(bytes) as total_bytes,
    -- Porcentaje de errores
    ROUND(
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as error_rate_percent,
    -- Distribución por método HTTP
    SUM(CASE WHEN http_method = 'GET' THEN 1 ELSE 0 END) as get_requests,
    SUM(CASE WHEN http_method = 'POST' THEN 1 ELSE 0 END) as post_requests,
    SUM(CASE WHEN http_method = 'HEAD' THEN 1 ELSE 0 END) as head_requests,
    -- Bots detectados
    SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) as bot_requests
FROM eclog_processed
GROUP BY country
ORDER BY total_requests DESC;

-- Ver resultados
SELECT * FROM country_stats;