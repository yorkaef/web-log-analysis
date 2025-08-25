-- ============================================================================
-- CREACIÓN DE TABLAS ECLOG - VERSIÓN CORREGIDA
-- ============================================================================

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS eclog_analysis;
USE eclog_analysis;

-- Eliminar tablas existentes si hay problemas
DROP TABLE IF EXISTS eclog_processed;
DROP TABLE IF EXISTS eclog_raw;

-- ============================================================================
-- PASO 1: Tabla externa para los logs RAW
-- ============================================================================
CREATE EXTERNAL TABLE eclog_raw (
    ip_id STRING,
    user_id STRING,
    request_timestamp STRING,  -- Cambié 'timestamp' por ser palabra reservada
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

-- Verificar que la tabla RAW se creó
SELECT 'Tabla RAW creada' as status;
DESCRIBE eclog_raw;

-- ============================================================================
-- PASO 2: Tabla procesada con campos derivados (VERSIÓN CORREGIDA)
-- ============================================================================
CREATE TABLE eclog_processed AS
SELECT 
    ip_id,
    user_id,
    CAST(request_timestamp AS TIMESTAMP) as request_timestamp,
    http_method,
    uri,
    http_version,
    response_code,
    bytes,
    referrer,
    user_agent,
    
    -- ========================================================================
    -- CAMPOS DERIVADOS DE TIEMPO
    -- ========================================================================
    HOUR(CAST(request_timestamp AS TIMESTAMP)) as hour,
    DAYOFWEEK(CAST(request_timestamp AS TIMESTAMP)) as day_of_week,
    DATE(CAST(request_timestamp AS TIMESTAMP)) as log_date,
    
    -- ========================================================================
    -- CATEGORÍA DE CÓDIGO DE RESPUESTA
    -- ========================================================================
    CASE 
        WHEN response_code >= 200 AND response_code < 300 THEN 'Success'
        WHEN response_code >= 300 AND response_code < 400 THEN 'Redirect'
        WHEN response_code >= 400 AND response_code < 500 THEN 'Client_Error'
        WHEN response_code >= 500 THEN 'Server_Error'
        ELSE 'Other'
    END as status_category,
    
    -- ========================================================================
    -- DETECCIÓN DE BOTS (SINTAXIS CORREGIDA)
    -- ========================================================================
    CASE 
        WHEN LOWER(user_agent) RLIKE '.*bot.*' THEN TRUE
        WHEN LOWER(user_agent) RLIKE '.*crawler.*' THEN TRUE
        WHEN LOWER(user_agent) RLIKE '.*spider.*' THEN TRUE
        WHEN LOWER(user_agent) RLIKE '.*scraper.*' THEN TRUE
        ELSE FALSE
    END as is_bot,
    
    -- ========================================================================
    -- CLASIFICACIÓN POR PAÍS (SINTAXIS SIMPLIFICADA)
    -- ========================================================================
    CASE 
        WHEN ip_id RLIKE '^192\\.168\\.' THEN 'Local'
        WHEN ip_id RLIKE '^10\\.' THEN 'Local'
        WHEN ip_id RLIKE '^172\\.' THEN 'Local'
        WHEN ip_id RLIKE '^203\\.' THEN 'Asia-Pacific'
        WHEN ip_id RLIKE '^210\\.' THEN 'Asia-Pacific'
        WHEN ip_id RLIKE '^202\\.' THEN 'Asia-Pacific'
        WHEN ip_id RLIKE '^124\\.' THEN 'Asia-Pacific'
        WHEN ip_id RLIKE '^91\\.' THEN 'Europe'
        WHEN ip_id RLIKE '^85\\.' THEN 'Europe'
        WHEN ip_id RLIKE '^94\\.' THEN 'Europe'
        WHEN ip_id RLIKE '^88\\.' THEN 'Europe'
        WHEN ip_id RLIKE '^198\\.' THEN 'North-America'
        WHEN ip_id RLIKE '^199\\.' THEN 'North-America'
        WHEN ip_id RLIKE '^208\\.' THEN 'North-America'
        WHEN ip_id RLIKE '^204\\.' THEN 'North-America'
        WHEN ip_id RLIKE '^200\\.' THEN 'South-America'
        WHEN ip_id RLIKE '^201\\.' THEN 'South-America'
        WHEN ip_id RLIKE '^186\\.' THEN 'South-America'
        WHEN ip_id RLIKE '^196\\.' THEN 'Africa'
        WHEN ip_id RLIKE '^197\\.' THEN 'Africa'
        WHEN ip_id RLIKE '^41\\.' THEN 'Africa'
        ELSE 'Unknown'
    END as country,
    
    -- ========================================================================
    -- TIPO DE RECURSO (SINTAXIS CORREGIDA)
    -- ========================================================================
    CASE 
        WHEN uri RLIKE '.*\\.html?$' THEN 'HTML'
        WHEN uri RLIKE '.*\\.css$' THEN 'CSS'
        WHEN uri RLIKE '.*\\.js$' THEN 'JavaScript'
        WHEN uri RLIKE '.*\\.(jpg|jpeg|png|gif|ico)$' THEN 'Image'
        WHEN uri RLIKE '.*\\.(pdf|doc|docx)$' THEN 'Document'
        WHEN uri RLIKE '.*\\.(xml|json)$' THEN 'Data'
        WHEN uri = '/' THEN 'HTML'
        ELSE 'Other'
    END as resource_type,
    
    -- ========================================================================
    -- INDICADOR DE REFERRER
    -- ========================================================================
    CASE 
        WHEN referrer IS NOT NULL AND referrer != '-' AND referrer != '' THEN TRUE
        ELSE FALSE
    END as has_referrer

FROM eclog_raw
WHERE request_timestamp IS NOT NULL 
  AND request_timestamp != ''
  AND response_code IS NOT NULL;

-- ============================================================================
-- PASO 3: VERIFICACIONES Y ESTADÍSTICAS BÁSICAS
-- ============================================================================

-- Verificar que la tabla se creó correctamente
SELECT 'Tabla PROCESADA creada exitosamente' as status;

-- Mostrar estructura de la tabla
DESCRIBE eclog_processed;

-- Estadísticas básicas
SELECT 'ESTADÍSTICAS BÁSICAS' as section;

SELECT 
    'Total de registros' as metric, 
    CAST(COUNT(*) AS STRING) as value 
FROM eclog_processed
UNION ALL
SELECT 
    'IPs únicas' as metric, 
    CAST(COUNT(DISTINCT ip_id) AS STRING) as value 
FROM eclog_processed  
UNION ALL
SELECT 
    'Usuarios únicos' as metric, 
    CAST(COUNT(DISTINCT user_id) AS STRING) as value 
FROM eclog_processed
UNION ALL
SELECT 
    'Fecha más antigua' as metric, 
    CAST(MIN(log_date) AS STRING) as value 
FROM eclog_processed
UNION ALL
SELECT 
    'Fecha más reciente' as metric, 
    CAST(MAX(log_date) AS STRING) as value 
FROM eclog_processed
UNION ALL
SELECT 
    'Códigos de respuesta únicos' as metric,
    CAST(COUNT(DISTINCT response_code) AS STRING) as value
FROM eclog_processed
UNION ALL
SELECT 
    'Países detectados' as metric,
    CAST(COUNT(DISTINCT country) AS STRING) as value
FROM eclog_processed;

-- Verificar distribución por status_category
SELECT 
    'DISTRIBUCIÓN POR ESTADO' as section,
    status_category,
    CAST(COUNT(*) AS STRING) as count_records
FROM eclog_processed
GROUP BY status_category
ORDER BY COUNT(*) DESC;

-- Verificar que no hay datos nulos críticos
SELECT 
    'VERIFICACIÓN DE DATOS NULOS' as section,
    'ip_id nulos' as check_type,
    CAST(SUM(CASE WHEN ip_id IS NULL THEN 1 ELSE 0 END) AS STRING) as null_count
FROM eclog_processed
UNION ALL
SELECT 
    'VERIFICACIÓN DE DATOS NULOS' as section,
    'response_code nulos' as check_type,
    CAST(SUM(CASE WHEN response_code IS NULL THEN 1 ELSE 0 END) AS STRING) as null_count
FROM eclog_processed
UNION ALL
SELECT 
    'VERIFICACIÓN DE DATOS NULOS' as section,
    'request_timestamp nulos' as check_type,
    CAST(SUM(CASE WHEN request_timestamp IS NULL THEN 1 ELSE 0 END) AS STRING) as null_count
FROM eclog_processed;

-- Muestra de datos para verificar
SELECT 'MUESTRA DE DATOS (5 registros)' as section;
SELECT 
    ip_id,
    user_id,
    request_timestamp,
    http_method,
    response_code,
    status_category,
    country,
    resource_type
FROM eclog_processed
LIMIT 5;

SELECT 'CREACIÓN DE TABLAS COMPLETADA EXITOSAMENTE' as final_status;