#!/bin/bash
"""
Script principal para ejecutar análisis completo del dataset EClog
Integra limpieza de datos, MapReduce, Hive y generación de reportes
"""

set -e  # Salir si hay algún error

# Configuración
PROJECT_DIR="/home/$USER/web-log-analysis"
DATA_RAW="$PROJECT_DIR/data/raw/eclog_1day.csv"
DATA_PROCESSED="$PROJECT_DIR/data/processed/clean_logs.tsv"
RESULTS_DIR="$PROJECT_DIR/results"
SCRIPTS_DIR="$PROJECT_DIR/scripts"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Función para verificar prerequisitos
check_prerequisites() {
    log_info "Verificando prerequisitos..."
    
    # Verificar que existe el archivo de datos
    if [ ! -f "$DATA_RAW" ]; then
        log_error "No se encuentra el archivo $DATA_RAW"
        log_info "Por favor, descarga eclog_1day.csv desde Harvard Dataverse y ponlo en data/raw/"
        exit 1
    fi
    
    # Verificar Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 no está instalado"
        exit 1
    fi
    
    # Verificar pandas
    if ! python3 -c "import pandas" &> /dev/null; then
        log_warning "Pandas no está instalado. Instalando..."
        pip3 install pandas matplotlib seaborn requests
    fi
    
    # Verificar Hadoop
    if ! command -v hadoop &> /dev/null; then
        log_warning "Hadoop no está disponible. Solo se ejecutarán análisis locales y Hive"
    fi
    
    # Verificar Hive
    if ! command -v hive &> /dev/null; then
        log_warning "Hive no está disponible. Solo se ejecutarán análisis locales"
    fi
    
    log_info "Prerequisitos verificados"
}

# Función para inicializar servicios
initialize_services() {
    log_info "Inicializando servicios Hadoop..."
    
    if command -v start-dfs.sh &> /dev/null; then
        # Verificar si NameNode ya está corriendo
        if ! jps | grep -q "NameNode"; then
            log_info "Iniciando HDFS..."
            start-dfs.sh
            sleep 10
        fi
        
        # Verificar si ResourceManager ya está corriendo  
        if ! jps | grep -q "ResourceManager"; then
            log_info "Iniciando YARN..."
            start-yarn.sh
            sleep 5
        fi
        
        # Crear directorios en HDFS si no existen
        hdfs dfs -mkdir -p /user/input /user/output 2>/dev/null || true
        
        log_info "Servicios Hadoop iniciados"
    else
        log_warning "Servicios Hadoop no disponibles, continuando sin HDFS"
    fi
}

# Función principal
main() {
    echo "=============================================="
    echo "    ANÁLISIS COMPLETO DE LOGS WEB ECLOG"
    echo "=============================================="
    echo "Dataset: EClog E-commerce Web Logs"
    echo "Fecha: $(date)"
    echo "=============================================="

    # Paso 0: Limpiar resultados previos
    log_info "LIMPIANDO RESULTADOS ANTERIORES..."
    rm -rf "$RESULTS_DIR"/* 2>/dev/null || true
    rm -rf "$DATA_PROCESSED" 2>/dev/null || true
    hdfs dfs -rm -r -f /user/input/* 2>/dev/null || true
    log_info "Resultados anteriores eliminados."
    
    # Paso 1: Verificar prerequisitos
    check_prerequisites
    
    # Paso 2: Limpiar datos
    log_info "PASO 1/6: Limpiando y procesando datos..."
    cd "$PROJECT_DIR"
    python3 scripts/clean_logs.py
    
    if [ $? -ne 0 ]; then
        log_error "Error en la limpieza de datos"
        exit 1
    fi
    
    # Paso 3: Inicializar servicios
    initialize_services
    
    # Paso 4: Subir datos a HDFS (si está disponible)
    if command -v hdfs &> /dev/null; then
        log_info "PASO 2/6: Subiendo datos a HDFS..."
        hdfs dfs -rm -f /user/input/clean_logs.tsv 2>/dev/null || true
        hdfs dfs -put "$DATA_PROCESSED" /user/input/ 2>/dev/null || true
        log_info "Datos subidos a HDFS"
    fi
    
    # Paso 5: Ejecutar análisis MapReduce (si está disponible)
    if command -v hadoop &> /dev/null && [ -f "$SCRIPTS_DIR/run_all_mapreduce.sh" ]; then
        log_info "PASO 3/6: Ejecutando análisis MapReduce..."
        bash "$SCRIPTS_DIR/run_all_mapreduce.sh"
    else
        log_info "PASO 3/6: Ejecutando análisis MapReduce local..."
        python3 "$SCRIPTS_DIR/test_mapreduce_local.py"
    fi
    
    # Paso 6: Ejecutar análisis Hive (si está disponible)
    if command -v hive &> /dev/null; then
        log_info "PASO 4/6: Ejecutando análisis con Hive..."
        bash "$SCRIPTS_DIR/hive/run_hive_analysis.sh"
    else
        log_warning "Hive no disponible, saltando análisis con Hive"
    fi
    
    # Paso 7: Generar reportes finales
    log_info "PASO 5/6: Generando reportes finales..."
    python3 "$SCRIPTS_DIR/generate_final_report.py"
    
    # Paso 8: Limpiar recursos
    log_info "PASO 6/6: Finalizando análisis..."
    
    # Mostrar resumen final
    echo ""
    echo "=============================================="
    echo "           ANÁLISIS COMPLETADO"
    echo "=============================================="
    echo "Resultados disponibles en: $RESULTS_DIR"
    echo ""
    echo "Archivos generados:"
    find "$RESULTS_DIR" -name "*.txt" -o -name "*.html" -o -name "*.csv" | while read file; do
        echo "  - $(basename "$file") ($(wc -l < "$file" 2>/dev/null || echo '?') líneas)"
    done
    echo ""
    echo "Para ver el reporte principal: firefox $RESULTS_DIR/final_report.html"
    echo "=============================================="
}

# Función para limpiar en caso de error
cleanup() {
    log_warning "Limpiando recursos..."
    # Aquí puedes agregar limpieza específica si es necesario
    exit 1
}

# Configurar trap para cleanup
trap cleanup ERR

# Ejecutar función principal
main "$@"