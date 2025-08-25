USE eclog_analysis;

-- 2. Análisis Temporal por Hora
DROP TABLE IF EXISTS hourly_stats;
CREATE TABLE hourly_stats AS
SELECT 
    hour,
    COUNT(*) as total_requests,
    COUNT(DISTINCT ip_id) as unique_ips,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(bytes) as avg_response_size,
    -- Errores por hora
    SUM(CASE WHEN status_category = 'Server_Error' THEN 1 ELSE 0 END) as server_errors,
    SUM(CASE WHEN status_category = 'Client_Error' THEN 1 ELSE 0 END) as client_errors,
    -- Tasa de error
    ROUND(
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as error_rate_percent,
    -- Distribución por tipo de recurso
    SUM(CASE WHEN resource_type = 'HTML' THEN 1 ELSE 0 END) as html_requests,
    SUM(CASE WHEN resource_type = 'Image' THEN 1 ELSE 0 END) as image_requests,
    SUM(CASE WHEN resource_type = 'CSS' THEN 1 ELSE 0 END) as css_requests,
    SUM(CASE WHEN resource_type = 'JavaScript' THEN 1 ELSE 0 END) as js_requests
FROM eclog_processed
GROUP BY hour;

-- Ver patrones de tráfico por hora
SELECT * FROM hourly_stats
ORDER BY hour;

-- Análisis temporal más detallado
SELECT 
    hour,
    ROUND(AVG(total_requests), 0) as avg_requests_per_hour,
    MIN(total_requests) as min_requests,
    MAX(total_requests) as max_requests,
    CASE 
        WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END as time_period
FROM hourly_stats
GROUP BY hour
ORDER BY hour;