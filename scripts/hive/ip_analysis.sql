USE eclog_analysis;

-- 3. Análisis de IPs Más Frecuentes
DROP TABLE IF EXISTS top_ips;
CREATE TABLE top_ips AS
SELECT 
    ip_id,
    COUNT(*) as total_requests,
    COUNT(DISTINCT uri) as unique_pages_accessed,
    COUNT(DISTINCT HOUR(timestamp)) as active_hours,
    MIN(timestamp) as first_request,
    MAX(timestamp) as last_request,
    AVG(bytes) as avg_response_size,
    SUM(bytes) as total_bytes,
    -- Análisis de comportamiento
    CASE 
        WHEN is_bot = TRUE THEN 'Bot'
        WHEN COUNT(*) > 1000 AND COUNT(DISTINCT uri) < 10 THEN 'Suspicious_Bot'
        WHEN COUNT(*) > 500 AND COUNT(DISTINCT HOUR(timestamp)) > 20 THEN 'Heavy_User'
        WHEN COUNT(*) > 100 THEN 'Active_User'
        ELSE 'Normal_User'
    END as user_classification,
    -- Errores generados por esta IP
    SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) as error_count,
    -- País de la IP
    MAX(country) as country,
    -- Métodos HTTP utilizados
    COUNT(DISTINCT http_method) as different_methods_used
FROM eclog_processed
GROUP BY ip_id
ORDER BY total_requests DESC
LIMIT 100;

-- Ver top IPs sospechosas
SELECT 
    ip_id,
    total_requests,
    unique_pages_accessed,
    user_classification,
    error_count,
    country
FROM top_ips
WHERE user_classification IN ('Bot', 'Suspicious_Bot')
ORDER BY total_requests DESC;