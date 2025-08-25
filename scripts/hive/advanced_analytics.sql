USE eclog_analysis;

-- 5. Análisis Avanzados y Detección de Patrones

-- 5.1 Análisis de Sesiones de Usuario
DROP TABLE IF EXISTS user_sessions;
CREATE TABLE user_sessions AS
SELECT 
    user_id,
    ip_id,
    requests_in_session,
    pages_visited,
    session_start,
    session_end,
    (UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60 as session_duration_minutes,
    requests_with_referrer,
    total_data_transferred,
    errors_in_session
FROM (
    SELECT 
        user_id,
        ip_id,
        COUNT(*) as requests_in_session,
        COUNT(DISTINCT uri) as pages_visited,
        MIN(request_timestamp) as session_start,
        MAX(request_timestamp) as session_end,
        SUM(CASE WHEN has_referrer = TRUE THEN 1 ELSE 0 END) as requests_with_referrer,
        SUM(bytes) as total_data_transferred,
        SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) as errors_in_session
    FROM eclog_processed
    WHERE user_id IS NOT NULL AND user_id != '-'
    GROUP BY user_id, ip_id
    HAVING COUNT(*) > 1
) t;

-- 5.2 Análisis de Páginas Más Populares
DROP TABLE IF EXISTS popular_pages;
CREATE TABLE popular_pages AS
SELECT 
    uri,
    COUNT(*) as page_views,
    COUNT(DISTINCT ip_id) as unique_visitors,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(bytes) as avg_page_size,
    -- Análisis de entrada y salida
    SUM(CASE WHEN has_referrer = FALSE THEN 1 ELSE 0 END) as entry_page_count,
    -- Errores en esta página
    SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) as error_count,
    -- Distribución temporal
    COUNT(DISTINCT hour) as active_hours,
    -- Países que acceden
    COUNT(DISTINCT country) as countries_accessing,
    -- Bots vs humanos
    SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) as bot_visits,
    SUM(CASE WHEN is_bot = FALSE THEN 1 ELSE 0 END) as human_visits
FROM eclog_processed
WHERE http_method = 'GET'
GROUP BY uri
ORDER BY page_views DESC
LIMIT 50;

-- 5.3 Detección de Ataques o Comportamiento Anómalo
DROP TABLE IF EXISTS anomaly_detection;
CREATE TABLE anomaly_detection AS
SELECT 
    ip_id,
    country,
    -- Métricas de actividad
    COUNT(*) as total_requests,
    COUNT(DISTINCT uri) as unique_uris,
    COUNT(DISTINCT hour) as active_hours,
    -- Métricas de error
    SUM(CASE WHEN response_code = 404 THEN 1 ELSE 0 END) as not_found_errors,
    SUM(CASE WHEN response_code = 403 THEN 1 ELSE 0 END) as forbidden_errors,
    SUM(CASE WHEN response_code >= 500 THEN 1 ELSE 0 END) as server_errors,
    -- Patrones sospechosos
    SUM(CASE WHEN uri RLIKE '.*(admin|login|wp-admin|config).*' THEN 1 ELSE 0 END) as admin_attempts,
    SUM(CASE WHEN uri RLIKE '.*\\.(php|asp|jsp).*' THEN 1 ELSE 0 END) as script_requests,
    -- Clasificación de anomalía
    CASE 
        WHEN COUNT(*) > 2000 THEN 'High_Volume_Suspicious'
        WHEN SUM(CASE WHEN response_code = 404 THEN 1 ELSE 0 END) > 100 THEN 'Scanner_Behavior'
        WHEN SUM(CASE WHEN uri RLIKE '.*(admin|login).*' THEN 1 ELSE 0 END) > 20 THEN 'Brute_Force_Attempt'
        WHEN COUNT(DISTINCT uri) < 5 AND COUNT(*) > 500 THEN 'Bot_Like_Behavior'
        ELSE 'Normal'
    END as anomaly_type
FROM eclog_processed
GROUP BY ip_id, country
HAVING COUNT(*) > 10
ORDER BY total_requests DESC;

-- 5.4 Análisis de Rendimiento del Servidor
DROP TABLE IF EXISTS server_performance;
CREATE TABLE server_performance AS
SELECT 
    hour,
    logdate,
    COUNT(*) as total_requests,
    AVG(bytes) as avg_response_size,
    -- Percentiles usando función compatible con Hive
    percentile_approx(bytes, 0.5) as median_response_size,
    percentile_approx(bytes, 0.95) as p95_response_size,
    -- Distribución de códigos de estado
    SUM(CASE WHEN response_code = 200 THEN 1 ELSE 0 END) as successful_requests,
    SUM(CASE WHEN response_code >= 400 AND response_code < 500 THEN 1 ELSE 0 END) as client_errors,
    SUM(CASE WHEN response_code >= 500 THEN 1 ELSE 0 END) as server_errors,
    -- Carga por tipo de recurso
    SUM(CASE WHEN resource_type = 'HTML' THEN bytes ELSE 0 END) as html_bytes,
    SUM(CASE WHEN resource_type = 'Image' THEN bytes ELSE 0 END) as image_bytes,
    SUM(CASE WHEN resource_type = 'CSS' THEN bytes ELSE 0 END) as css_bytes,
    SUM(CASE WHEN resource_type = 'JavaScript' THEN bytes ELSE 0 END) as js_bytes
FROM eclog_processed
GROUP BY hour, logdate
ORDER BY logdate, hour;