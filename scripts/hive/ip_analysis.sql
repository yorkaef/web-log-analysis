USE eclog_analysis;

-- 3. Análisis de IPs Más Frecuentes
DROP TABLE IF EXISTS top_ips;
CREATE TABLE top_ips AS
SELECT 
    ip_id,
    total_requests,
    unique_pages_accessed,
    active_hours,
    first_request,
    last_request,
    avg_response_size,
    total_bytes,
    CASE 
        WHEN is_bot = TRUE THEN 'Bot'
        WHEN total_requests > 1000 AND unique_pages_accessed < 10 THEN 'Suspicious_Bot'
        WHEN total_requests > 500 AND active_hours > 20 THEN 'Heavy_User'
        WHEN total_requests > 100 THEN 'Active_User'
        ELSE 'Normal_User'
    END as user_classification,
    error_count,
    country,
    different_methods_used
FROM (
    SELECT 
        ip_id,
        COUNT(*) as total_requests,
        COUNT(DISTINCT uri) as unique_pages_accessed,
        COUNT(DISTINCT HOUR(request_timestamp)) as active_hours,  -- Corregido
        MIN(request_timestamp) as first_request,                   -- Corregido
        MAX(request_timestamp) as last_request,                    -- Corregido
        AVG(bytes) as avg_response_size,
        SUM(bytes) as total_bytes,
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) as error_count,
        MAX(country) as country,
        COUNT(DISTINCT http_method) as different_methods_used,
        MAX(is_bot) as is_bot
    FROM eclog_processed
    GROUP BY ip_id
) t;

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
ORDER BY total_requests DESC
LIMIT 100;