#!/usr/bin/env python3
"""
Script para limpiar logs web
Proyecto: Análisis de Logs Web
Dataset: EClog con columnas: IpId,UserId,TimeStamp,HttpMethod,Uri,HttpVersion,ResponseCode,Bytes,Referrer,UserAgent
"""

import pandas as pd
import re
from datetime import datetime
import os
import requests

def get_country_from_ip(ip):
    """
    Obtiene el país de una IP usando un servicio gratuito
    """
    try:
        # Usar ipapi.co para obtener país (máximo 1000 requests/día gratis)
        response = requests.get(f'http://ip-api.com/json/{ip}', timeout=1)
        if response.status_code == 200:
            data = response.json()
            return data.get('country', 'Unknown')
    except:
        pass
    
    # Fallback: clasificación simple por rango IP
    if ip.startswith('192.168') or ip.startswith('10.') or ip.startswith('172.'):
        return 'Local'
    elif ip.startswith(('203.', '210.')):
        return 'Asia-Pacific'
    elif ip.startswith(('91.', '85.', '94.')):
        return 'Europe'
    elif ip.startswith(('198.', '199.', '208.')):
        return 'North America'
    else:
        return 'Unknown'

def categorize_status_code(code):
    """Categoriza los códigos de respuesta HTTP"""
    try:
        code = int(code)
        if 200 <= code < 300:
            return 'Success'
        elif 300 <= code < 400:
            return 'Redirect'
        elif 400 <= code < 500:
            return 'Client_Error'
        elif 500 <= code < 600:
            return 'Server_Error'
        else:
            return 'Other'
    except:
        return 'Invalid'

def extract_file_extension(uri):
    """Extrae la extensión del archivo de la URI"""
    try:
        # Obtener solo el path sin query parameters
        path = uri.split('?')[0]
        if '.' in path:
            return path.split('.')[-1].lower()
        else:
            return 'no_extension'
    except:
        return 'unknown'

def clean_web_logs(input_file, output_file):
    """
    Limpia y prepara los logs web para el análisis
    Columnas esperadas: IpId,UserId,TimeStamp,HttpMethod,Uri,HttpVersion,ResponseCode,Bytes,Referrer,UserAgent
    """
    print("Iniciando limpieza de logs EClog...")
    
    # Verificar que el archivo existe
    if not os.path.exists(input_file):
        print(f"Error: No se encuentra el archivo {input_file}")
        return
    
    try:
        # Leer el archivo CSV con separador de tabulaciones
        print("Leyendo archivo EClog...")
        df = pd.read_csv(input_file, sep=',')
        
        print(f"Registros originales: {len(df)}")
        print(f"Columnas encontradas: {df.columns.tolist()}")
        
        # Verificar que tenemos las columnas esperadas
        expected_columns = ['IpId', 'UserId', 'TimeStamp', 'HttpMethod', 'Uri', 
                          'HttpVersion', 'ResponseCode', 'Bytes', 'Referrer', 'UserAgent']
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"Advertencia: Columnas faltantes: {missing_columns}")
        
        # Mostrar primeras filas para inspección
        print("\nPrimeras 5 filas:")
        print(df.head())
        print(f"\nTipos de datos:")
        print(df.dtypes)
        
        # Limpiar datos faltantes en columnas críticas
        initial_count = len(df)
        df = df.dropna(subset=['IpId', 'TimeStamp', 'HttpMethod', 'Uri', 'ResponseCode'])
        print(f"Registros después de limpiar NaN: {len(df)} (eliminados: {initial_count - len(df)})")
        
        # Procesar TimeStamp
        print("Procesando timestamps...")
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
        df['Hour'] = df['TimeStamp'].dt.hour
        df['DayOfWeek'] = df['TimeStamp'].dt.dayofweek
        df['Date'] = df['TimeStamp'].dt.date
        
        # Limpiar y procesar IPs
        df['IpId'] = df['IpId'].astype(str).str.strip()
        
        # Clasificar códigos de respuesta
        df['StatusCategory'] = df['ResponseCode'].apply(categorize_status_code)
        
        # Procesar bytes (convertir a numérico)
        df['Bytes'] = pd.to_numeric(df['Bytes'], errors='coerce').fillna(0)
        
        # Extraer extensión de archivos
        df['FileExtension'] = df['Uri'].apply(extract_file_extension)
        
        # Limpiar método HTTP
        df['HttpMethod'] = df['HttpMethod'].str.upper().str.strip()
        
        # Procesar User Agent para identificar bots
        df['IsBot'] = df['UserAgent'].str.contains(
            'bot|crawler|spider|scraper', case=False, na=False
        )
        
        # Procesar Referrer
        df['HasReferrer'] = df['Referrer'].notna() & (df['Referrer'] != '-')
        
        # Añadir país (solo para una muestra para no exceder límites de API)
        print("Procesando países de IPs (muestra)...")
        unique_ips = df['IpId'].unique()[:100]  # Solo primeras 100 IPs únicas
        ip_country_map = {}
        
        for ip in unique_ips[:50]:  # Limitar para evitar rate limits
            ip_country_map[ip] = get_country_from_ip(ip)
        
        df['Country'] = df['IpId'].map(ip_country_map).fillna('Unknown')
        
        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Guardar datos limpios
        df.to_csv(output_file, index=False, sep='\t')
        # Mostrar estadísticas finales
        print(f"\n=== ESTADÍSTICAS FINALES ===")
        print(f"Registros procesados: {len(df)}")
        print(f"IPs únicas: {df['IpId'].nunique()}")
        print(f"Usuarios únicos: {df['UserId'].nunique()}")
        print(f"URIs únicas: {df['Uri'].nunique()}")
        print(f"Códigos de respuesta únicos: {df['ResponseCode'].unique()}")
        print(f"Métodos HTTP: {df['HttpMethod'].value_counts().to_dict()}")
        print(f"Distribución de status: \n{df['StatusCategory'].value_counts()}")
        print(f"\nArchivo limpio guardado en: {output_file}")
        
        # Guardar estadísticas
        stats_file = output_file.replace('.tsv', '_stats.txt')
        with open(stats_file, 'w') as f:
            f.write(f"Estadísticas del dataset EClog\n")
            f.write(f"Generado: {datetime.now()}\n")
            f.write(f"Registros totales: {len(df)}\n")
            f.write(f"IPs únicas: {df['IpId'].nunique()}\n")
            f.write(f"Usuarios únicos: {df['UserId'].nunique()}\n")
            f.write(f"Período: {df['TimeStamp'].min()} a {df['TimeStamp'].max()}\n")
            f.write(f"Métodos HTTP:\n{df['HttpMethod'].value_counts().to_string()}\n")
            f.write(f"Códigos de respuesta:\n{df['ResponseCode'].value_counts().to_string()}\n")
        
        return True
        
    except Exception as e:
        print(f"Error al procesar archivo: {e}")
        import traceback
        traceback.print_exc()
        return False

def analyze_data_quality(file_path):
    """
    Analiza la calidad de los datos procesados
    """
    print("\n=== ANÁLISIS DE CALIDAD DE DATOS ===")
    
    df = pd.read_csv(file_path, sep='\t')
    
    print(f"Total de registros: {len(df)}")
    print(f"Columnas: {list(df.columns)}")
    
    # Análisis de valores faltantes
    print("\nValores faltantes por columna:")
    missing = df.isnull().sum()
    for col, count in missing.items():
        if count > 0:
            percentage = (count / len(df)) * 100
            print(f"  {col}: {count} ({percentage:.2f}%)")
    
    # Análisis temporal
    if 'TimeStamp' in df.columns:
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        print(f"\nRango temporal: {df['TimeStamp'].min()} a {df['TimeStamp'].max()}")
    
    # Top 10 IPs
    print(f"\nTop 10 IPs más activas:")
    top_ips = df['IpId'].value_counts().head(10)
    for ip, count in top_ips.items():
        print(f"  {ip}: {count} requests")

if __name__ == "__main__":
    # Rutas relativas al proyecto
    input_path = "data/raw/eclog_1day.csv"
    output_path = "data/processed/clean_logs.tsv"
    
    # Ejecutar limpieza
    success = clean_web_logs(input_path, output_path)
    
    if success:
        # Analizar calidad de datos
        analyze_data_quality(output_path)
    else:
        print("Error en el procesamiento. Revisa el archivo de entrada.")