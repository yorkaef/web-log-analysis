-- Crear base de datos
CREATE DATABASE IF NOT EXISTS eclog_analysis;
USE eclog_analysis;

-- Tabla externa para los logs limpios de EClog
DROP TABLE IF EXISTS eclog_raw;
CREATE EXTERNAL TABLE eclog_raw (
    ip_id STRING,
    user_id STRING,
    timestamp STRING,
    http_method STRING,
    uri STRING,
    http_version STRING,
    response_code INT,
    bytes BIGINT,
    referrer STRING,
    user_agent STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/input/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Tabla procesada con campos adicionales
DROP TABLE IF EXISTS eclog_processed;
CREATE TABLE eclog_processed AS
SELECT 
    ip_id,
    user_id,
    CAST(timestamp AS TIMESTAMP) as timestamp,
    http_method,
    uri,
    http_version,
    response_code,
    bytes,
    referrer,
    user_agent,
    -- Campos derivados
    HOUR(CAST(timestamp AS TIMESTAMP)) as hour,
    DAYOFWEEK(CAST(timestamp AS TIMESTAMP)) as day_of_week,
    DATE(CAST(timestamp AS TIMESTAMP)) as date,
    -- Categoría de código de respuesta
    CASE 
        WHEN response_code >= 200 AND response_code < 300 THEN 'Success'
        WHEN response_code >= 300 AND response_code < 400 THEN 'Redirect'
        WHEN response_code >= 400 AND response_code < 500 THEN 'Client_Error'
        WHEN response_code >= 500 THEN 'Server_Error'
        ELSE 'Other'
    END as status_category,
    -- Identificar posibles bots
    CASE 
        WHEN LOWER(user_agent) RLIKE '.*(bot|crawler|spider|scraper).*' THEN TRUE
        ELSE FALSE
    END as is_bot,
    -- País simplificado por IP
    CASE 
        WHEN ip_id RLIKE '^192\\.168\\.|^10\\.|^172\\.' THEN 'Local'
        WHEN ip_id RLIKE '^203\\.|^210\\.|^202\\.|^124\\.' THEN 'Asia-Pacific'
        WHEN ip_id RLIKE '^91\\.|^85\\.|^94\\.|^88\\.' THEN 'Europe'
        WHEN ip_id RLIKE '^198\\.|^199\\.|^208\\.|^204\\.' THEN 'North-America'
        WHEN ip_id RLIKE '^200\\.|^201\\.|^186\\.' THEN 'South-America'
        WHEN ip_id RLIKE '^196\\.|^197\\.|^41\\.' THEN 'Africa'
        ELSE 'Unknown'
    END as country,
    -- Extensión de archivo
    CASE 
        WHEN uri RLIKE '.*\\.(html|htm) THEN 'HTML'
        WHEN uri RLIKE '.*\\.(css) THEN 'CSS'
        WHEN uri RLIKE '.*\\.(js) THEN 'JavaScript'
        WHEN uri RLIKE '.*\\.(jpg|jpeg|png|gif|ico) THEN 'Image'
        WHEN uri RLIKE '.*\\.(pdf|doc|docx) THEN 'Document'
        WHEN uri RLIKE '.*\\.(xml|json) THEN 'Data'
        ELSE 'Other'
    END as resource_type,
    -- Tiene referrer
    CASE 
        WHEN referrer IS NOT NULL AND referrer != '-' THEN TRUE
        ELSE FALSE
    END as has_referrer
FROM eclog_raw;

-- Verificar datos procesados
SELECT 'Total records' as metric, COUNT(*) as value FROM eclog_processed
UNION ALL
SELECT 'Unique IPs' as metric, COUNT(DISTINCT ip_id) as value FROM eclog_processed  
UNION ALL
SELECT 'Unique Users' as metric, COUNT(DISTINCT user_id) as value FROM eclog_processed
UNION ALL
SELECT 'Date Range Start' as metric, CAST(MIN(timestamp) AS STRING) as value FROM eclog_processed
UNION ALL
SELECT 'Date Range End' as metric, CAST(MAX(timestamp) AS STRING) as value FROM eclog_processed;