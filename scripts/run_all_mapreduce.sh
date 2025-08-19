#!/bin/bash
# Script para ejecutar todos los análisis MapReduce

echo "=== Iniciando Análisis MapReduce del Dataset EClog ==="

# Configuración
INPUT_DIR="/user/input"
OUTPUT_BASE="/user/output"
JAR_PATH="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
LOCAL_INPUT_FILE="/home/kiper/web-log-analysis/data/processed/clean_logs.tsv"


# Verificar que Hadoop esté corriendo
if ! jps | grep -q "NameNode"; then
    echo "Iniciando servicios Hadoop..."
    start-dfs.sh
    start-yarn.sh
    sleep 10
fi

# Subir archivo a HDFS si no existe
echo "Verificando archivo en HDFS..."
hdfs dfs -test -e $INPUT_DIR/clean_logs.tsv
if [ $? -ne 0 ]; then
    echo "Archivo no encontrado en HDFS. Subiendo $LOCAL_INPUT_FILE a $INPUT_DIR..."
    hdfs dfs -mkdir -p $INPUT_DIR
    hdfs dfs -put -f $LOCAL_INPUT_FILE $INPUT_DIR/
else
    echo "Archivo ya existe en HDFS."
fi

# Limpiar outputs anteriores
echo "Limpiando outputs anteriores..."
hdfs dfs -rm -r $OUTPUT_BASE/* 2>/dev/null || true

# 1. Análisis por País
echo "1. Ejecutando análisis por país..."
hadoop jar $JAR_PATH \
    -files scripts/mapreduce/country_mapper.py,scripts/mapreduce/country_reducer.py \
    -mapper "python3 country_mapper.py" \
    -reducer "python3 country_reducer.py" \
    -input $INPUT_DIR/clean_logs.tsv \
    -output $OUTPUT_BASE/country_analysis

# 2. Análisis por Hora
echo "2. Ejecutando análisis temporal..."
hadoop jar $JAR_PATH \
    -files scripts/mapreduce/hourly_mapper.py,scripts/mapreduce/hourly_reducer.py \
    -mapper "python3 hourly_mapper.py" \
    -reducer "python3 hourly_reducer.py" \
    -input $INPUT_DIR/clean_logs.tsv \
    -output $OUTPUT_BASE/hourly_analysis

# 3. IPs Frecuentes
echo "3. Ejecutando análisis de IPs frecuentes..."
hadoop jar $JAR_PATH \
    -files scripts/mapreduce/frequent_ips_mapper.py,scripts/mapreduce/frequent_ips_reducer.py \
    -mapper "python3 frequent_ips_mapper.py" \
    -reducer "python3 frequent_ips_reducer.py" \
    -input $INPUT_DIR/clean_logs.tsv \
    -output $OUTPUT_BASE/frequent_ips

# 4. Análisis de Errores
echo "4. Ejecutando análisis de errores HTTP..."
hadoop jar $JAR_PATH \
    -files scripts/mapreduce/errors_mapper.py,scripts/mapreduce/errors_reducer.py \
    -mapper "python3 errors_mapper.py" \
    -reducer "python3 errors_reducer.py" \
    -input $INPUT_DIR/clean_logs.tsv \
    -output $OUTPUT_BASE/error_analysis

# Descargar resultados
echo "5. Descargando resultados..."
mkdir -p results/mapreduce

hdfs dfs -get $OUTPUT_BASE/country_analysis/part-00000 results/mapreduce/country_results.txt
hdfs dfs -get $OUTPUT_BASE/hourly_analysis/part-00000 results/mapreduce/hourly_results.txt  
hdfs dfs -get $OUTPUT_BASE/frequent_ips/part-00000 results/mapreduce/frequent_ips_results.txt
hdfs dfs -get $OUTPUT_BASE/error_analysis/part-00000 results/mapreduce/error_results.txt

echo "=== MapReduce completado ==="
echo "Resultados guardados en results/mapreduce/"
ls -la results/mapreduce/
