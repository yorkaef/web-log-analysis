-- ~/web-log-analysis/scripts/hive/create_tables.sql
CREATE DATABASE IF NOT EXISTS weblog_analysis;
USE weblog_analysis;

-- Tabla externa para logs limpios
CREATE EXTERNAL TABLE IF NOT EXISTS web_logs (
    ip STRING,
    `timestamp` STRING,
    method STRING,
    url STRING,
    status INT,
    size BIGINT,
    referer STRING,
    user_agent STRING,
    hour INT,
    `date` STRING,
    status_category STRING,
    country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/input/';

-- Tabla para estadísticas por país
CREATE TABLE IF NOT EXISTS country_stats AS
SELECT 
    country,
    COUNT(*) as total_visits,
    COUNT(DISTINCT ip) as unique_visitors,
    AVG(size) as avg_response_size
FROM web_logs
GROUP BY country;

-- Tabla para estadísticas por hora
CREATE TABLE IF NOT EXISTS hourly_stats AS
SELECT 
    hour,
    COUNT(*) as total_requests,
    COUNT(DISTINCT ip) as unique_visitors,
    SUM(CASE WHEN status_category = 'Server_Error' THEN 1 ELSE 0 END) as server_errors,
    SUM(CASE WHEN status_category = 'Client_Error' THEN 1 ELSE 0 END) as client_errors
FROM web_logs
GROUP BY hour
ORDER BY hour;

-- Top IPs más frecuentes
CREATE TABLE IF NOT EXISTS top_ips AS
SELECT 
    ip,
    COUNT(*) as request_count,
    COUNT(DISTINCT url) as unique_pages,
    MAX(timestamp) as last_visit
FROM web_logs
GROUP BY ip
ORDER BY request_count DESC
LIMIT 100;

-- Análisis de errores HTTP
CREATE TABLE IF NOT EXISTS error_analysis AS
SELECT 
    status,
    status_category,
    COUNT(*) as error_count,
    url,
    COUNT(DISTINCT ip) as affected_users
FROM web_logs
WHERE status >= 400
GROUP BY status, status_category, url
ORDER BY error_count DESC;