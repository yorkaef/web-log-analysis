#!/usr/bin/env python3
"""
Script de validación para verificar la integridad de los resultados
"""

import os
import pandas as pd
from datetime import datetime

def validate_data_quality():
    """Valida la calidad de los datos procesados"""
    print("=== Validación de Calidad de Datos ===")
    
    data_file = "data/processed/clean_logs.tsv"
    
    if not os.path.exists(data_file):
        print("❌ Error: No se encuentra el archivo de datos procesados")
        return False
    
    try:
        df = pd.read_csv(data_file, sep='\t')
        
        # Verificar columnas esperadas
        expected_columns = ['IpId', 'UserId', 'TimeStamp', 'HttpMethod', 'Uri', 
                          'ResponseCode', 'Bytes', 'Referrer', 'UserAgent']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            print(f"⚠️  Advertencia: Columnas faltantes: {missing_columns}")
        
        # Estadísticas básicas
        print(f"✅ Total de registros: {len(df):,}")
        print(f"✅ IPs únicas: {df['IpId'].nunique():,}")
        print(f"✅ Rango de fechas: {df['TimeStamp'].min()} a {df['TimeStamp'].max()}")
        
        # Verificar integridad
        null_counts = df.isnull().sum()
        critical_nulls = null_counts[['IpId', 'TimeStamp', 'ResponseCode']].sum()
        
        if critical_nulls > 0:
            print(f"⚠️  Advertencia: {critical_nulls} valores nulos en columnas críticas")
        else:
            print("✅ Integridad de datos: OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Error validando datos: {e}")
        return False

def validate_results():
    """Valida que se hayan generado todos los resultados esperados"""
    print("\n=== Validación de Resultados ===")
    
    expected_files = [
        "results/final_report.html",
        "results/analysis_summary.json",
        "data/processed/clean_logs.tsv"
    ]
    
    optional_files = [
        "results/mapreduce/country_results.txt",
        "results/hive/country_analysis.txt",
        "results/visualizations/hourly_traffic.png"
    ]
    
    all_good = True
    
    # Verificar archivos esenciales
    for file_path in expected_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✅ {file_path} ({size:,} bytes)")
        else:
            print(f"❌ Falta: {file_path}")
            all_good = False
    
    # Verificar archivos opcionales
    print("\nArchivos opcionales:")
    for file_path in optional_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✅ {file_path} ({size:,} bytes)")
        else:
            print(f"⚠️  No disponible: {file_path}")
    
    return all_good

if __name__ == "__main__":
    print("🔍 Validando resultados del análisis...")
    
    data_ok = validate_data_quality()
    results_ok = validate_results()
    
    if data_ok and results_ok:
        print("\n🎉 Validación exitosa: Todos los componentes esenciales están presentes")
    else:
        print("\n⚠️  Validación con advertencias: Revisar los mensajes arriba")