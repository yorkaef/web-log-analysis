#!/bin/bash

echo "=== Ejecutando Análisis Completo con Hive ==="

# Configuración
HIVE_SCRIPTS_DIR="scripts/hive"
RESULTS_DIR="results/hive"

# Crear directorio de resultados
mkdir -p $RESULTS_DIR

# Verificar que Hive esté disponible
if ! command -v hive &> /dev/null; then
    echo "Error: Hive no está instalado o no está en PATH"
    exit 1
fi

# 1. Crear tablas
echo "1. Creando tablas en Hive..."
hive -f $HIVE_SCRIPTS_DIR/create_eclog_tables.sql > $RESULTS_DIR/table_creation.log 2>&1

# 2. Análisis por país
echo "2. Ejecutando análisis por país..."
hive -f $HIVE_SCRIPTS_DIR/country_analysis.sql > $RESULTS_DIR/country_analysis.txt 2>&1

# 3. Análisis temporal
echo "3. Ejecutando análisis temporal..."
hive -f $HIVE_SCRIPTS_DIR/hourly_analysis.sql > $RESULTS_DIR/hourly_analysis.txt 2>&1

# 4. Análisis de IPs
echo "4. Ejecutando análisis de IPs..."
hive -f $HIVE_SCRIPTS_DIR/ip_analysis.sql > $RESULTS_DIR/ip_analysis.txt 2>&1

# 5. Análisis de errores
echo "5. Ejecutando análisis de errores..."
hive -f $HIVE_SCRIPTS_DIR/error_analysis.sql > $RESULTS_DIR/error_analysis.txt 2>&1

# 6. Análisis avanzados
echo "6. Ejecutando análisis avanzados..."
hive -f $HIVE_SCRIPTS_DIR/advanced_analytics.sql > $RESULTS_DIR/advanced_analytics.txt 2>&1

# 7. Generar reporte ejecutivo
echo "7. Generando reporte ejecutivo..."
hive -f $HIVE_SCRIPTS_DIR/generate_reports.sql > $RESULTS_DIR/executive_report.txt 2>&1

echo "=== Análisis Hive Completado ==="
echo "Resultados guardados en: $RESULTS_DIR/"
ls -la $RESULTS_DIR/

# Mostrar resumen
echo ""
echo "=== RESUMEN DE ARCHIVOS GENERADOS ==="
for file in $RESULTS_DIR/*.txt; do
    if [ -f "$file" ]; then
        echo "$(basename "$file"): $(wc -l < "$file") líneas"
    fi
done