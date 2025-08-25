USE eclog_analysis;

-- Versión simple que replica exactamente el mapper de Python
DROP TABLE IF EXISTS country_stats;
CREATE TABLE country_stats AS
SELECT 
    -- Lógica simple que replica el mapper Python
    CASE 
        WHEN country != 'Unknown' THEN country
        WHEN ip_id LIKE '%PL' THEN 'Poland'
        WHEN ip_id LIKE '%US' THEN 'United States'
        WHEN ip_id LIKE '%DE' THEN 'Germany'
        WHEN ip_id LIKE '%CA' THEN 'Canada'
        WHEN ip_id LIKE '%NL' THEN 'Netherlands'
        WHEN ip_id LIKE '%GB' THEN 'United Kingdom'
        WHEN ip_id LIKE '%FR' THEN 'France'
        WHEN ip_id RLIKE '.*[A-Z]{2}$' THEN CONCAT('Other-', SUBSTR(ip_id, -2))
        ELSE 'Unknown'
    END as country,
    
    COUNT(*) as total_requests,
    COUNT(DISTINCT ip_id) as unique_ips,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(bytes) as avg_response_size,
    SUM(bytes) as total_bytes,
    ROUND(SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate_percent,
    SUM(CASE WHEN http_method = 'GET' THEN 1 ELSE 0 END) as get_requests,
    SUM(CASE WHEN http_method = 'POST' THEN 1 ELSE 0 END) as post_requests,
    SUM(CASE WHEN http_method = 'HEAD' THEN 1 ELSE 0 END) as head_requests,
    SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) as bot_requests
FROM eclog_processed
GROUP BY 
    CASE 
        WHEN country != 'Unknown' THEN country
        WHEN ip_id LIKE '%PL' THEN 'Poland'
        WHEN ip_id LIKE '%US' THEN 'United States'
        WHEN ip_id LIKE '%DE' THEN 'Germany'
        WHEN ip_id LIKE '%CA' THEN 'Canada'
        WHEN ip_id LIKE '%NL' THEN 'Netherlands'
        WHEN ip_id LIKE '%GB' THEN 'United Kingdom'
        WHEN ip_id LIKE '%FR' THEN 'France'
        WHEN ip_id RLIKE '.*[A-Z]{2}$' THEN CONCAT('Other-', SUBSTR(ip_id, -2))
        ELSE 'Unknown'
    END;

-- Ver resultados
SELECT * FROM country_stats ORDER BY total_requests DESC;