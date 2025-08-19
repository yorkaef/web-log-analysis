-- ~/web-log-analysis/scripts/hive/advanced_queries.sql

-- 1. Análisis de tráfico por país y hora
SELECT 
    country,
    hour,
    COUNT(*) as requests,
    COUNT(DISTINCT ip) as unique_users,
    AVG(size) as avg_response_size
FROM web_logs
GROUP BY country, hour
ORDER BY country, hour;

-- 2. Detección de posibles bots (muchas requests desde una IP)
SELECT 
    ip,
    COUNT(*) as total_requests,
    COUNT(DISTINCT url) as unique_urls,
    COUNT(DISTINCT hour) as active_hours,
    AVG(size) as avg_response_size,
    CASE 
        WHEN COUNT(*) > 1000 AND COUNT(DISTINCT url) < 10 THEN 'Probable Bot'
        WHEN COUNT(*) > 500 AND COUNT(DISTINCT hour) > 20 THEN 'Possible Bot'
        ELSE 'Likely Human'
    END as classification
FROM web_logs
GROUP BY ip
HAVING COUNT(*) > 100
ORDER BY total_requests DESC;

-- 3. Análisis de páginas más populares
SELECT 
    url,
    COUNT(*) as page_views,
    COUNT(DISTINCT ip) as unique_visitors,
    AVG(size) as avg_size,
    SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) as error_count
FROM web_logs
GROUP BY url
ORDER BY page_views DESC
LIMIT 50;

-- 4. Análisis temporal de errores
SELECT 
    hour,
    DATE(timestamp) as date,
    COUNT(CASE WHEN status >= 500 THEN 1 END) as server_errors,
    COUNT(CASE WHEN status >= 400 AND status < 500 THEN 1 END) as client_errors,
    COUNT(*) as total_requests,
    ROUND(COUNT(CASE WHEN status >= 400 THEN 1 END) * 100.0 / COUNT(*), 2) as error_rate
FROM web_logs
GROUP BY hour, DATE(timestamp)
ORDER BY date, hour;