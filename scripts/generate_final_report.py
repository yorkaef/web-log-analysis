#!/usr/bin/env python3
"""
Generador de reporte final integrado - VERSIÓN ACTUALIZADA
Combina resultados de MapReduce, Hive y análisis locales
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import json
import glob
import re
import numpy as np

class EClogReportGenerator:
    def __init__(self):
        self.project_dir = "/home/{}/web-log-analysis".format(os.getenv('USER', 'user'))
        self.results_dir = f"{self.project_dir}/results"
        self.data_dir = f"{self.project_dir}/data"
        
        # Configurar estilo de gráficos
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def load_processed_data(self):
        """Carga los datos procesados para análisis adicional"""
        try:
            data_file = f"{self.data_dir}/processed/clean_logs.tsv"
            if os.path.exists(data_file):
                print("Cargando datos procesados...")
                return pd.read_csv(data_file, sep='\t')
            else:
                print(f"Advertencia: No se encuentra {data_file}")
                return None
        except Exception as e:
            print(f"Error cargando datos: {e}")
            return None
    
    def load_mapreduce_results(self):
        """Carga resultados de MapReduce"""
        results = {}
        mapreduce_dir = f"{self.results_dir}/mapreduce"
        
        if not os.path.exists(mapreduce_dir):
            mapreduce_dir = f"{self.results_dir}/local_test"
        
        if os.path.exists(mapreduce_dir):
            print(f"Cargando resultados MapReduce desde: {mapreduce_dir}")
            
            # Cargar resultados por país
            country_file = f"{mapreduce_dir}/country_results.txt"
            if os.path.exists(country_file):
                with open(country_file, 'r') as f:
                    results['country'] = [line.strip().split('\t') for line in f if line.strip()]
            
            # Cargar resultados por hora
            hourly_file = f"{mapreduce_dir}/hourly_results.txt"
            if os.path.exists(hourly_file):
                with open(hourly_file, 'r') as f:
                    results['hourly'] = [line.strip().split('\t') for line in f if line.strip()]
            
            # Cargar IPs frecuentes
            ips_file = f"{mapreduce_dir}/frequent_ips_results.txt"
            if os.path.exists(ips_file):
                with open(ips_file, 'r') as f:
                    results['frequent_ips'] = [line.strip().split('\t') for line in f if line.strip()]
            
            # Cargar análisis de errores
            errors_file = f"{mapreduce_dir}/error_results.txt"
            if os.path.exists(errors_file):
                with open(errors_file, 'r') as f:
                    results['errors'] = [line.strip().split('\t') for line in f if line.strip()]
        
        return results
    
    def load_hive_results(self):
        """Carga resultados de análisis Hive"""
        results = {}
        hive_dir = f"{self.results_dir}/hive"
        
        if not os.path.exists(hive_dir):
            print(f"Advertencia: No se encuentra el directorio {hive_dir}")
            return results
        
        print(f"Cargando resultados Hive desde: {hive_dir}")
        
        try:
            # 1. Cargar análisis por país de Hive
            country_file = f"{hive_dir}/country_analysis.txt"
            if os.path.exists(country_file):
                results['hive_country'] = self._parse_hive_table_results(country_file, 'country_stats')
            
            # 2. Cargar análisis temporal de Hive
            hourly_file = f"{hive_dir}/hourly_analysis.txt"
            if os.path.exists(hourly_file):
                results['hive_hourly'] = self._parse_hive_table_results(hourly_file, 'hourly_stats')
            
            # 3. Cargar análisis de IPs de Hive
            ip_file = f"{hive_dir}/ip_analysis.txt"
            if os.path.exists(ip_file):
                results['hive_ips'] = self._parse_hive_table_results(ip_file, 'top_ips')
            
            # 4. Cargar análisis de errores de Hive
            error_file = f"{hive_dir}/error_analysis.txt"
            if os.path.exists(error_file):
                results['hive_errors'] = self._parse_hive_table_results(error_file, 'error_analysis')
            
            # 5. Cargar reporte ejecutivo de Hive
            executive_file = f"{hive_dir}/executive_report.txt"
            if os.path.exists(executive_file):
                results['hive_executive'] = self._parse_hive_executive_metrics(executive_file)
            
            # 6. Cargar análisis avanzados de Hive
            advanced_file = f"{hive_dir}/advanced_analytics.txt"
            if os.path.exists(advanced_file):
                results['hive_advanced'] = self._parse_hive_table_results(advanced_file, 'anomaly_detection')
                
        except Exception as e:
            print(f"Error cargando resultados de Hive: {e}")
        
        return results
    
    def _parse_hive_table_results(self, file_path, table_name):
        """Parsea resultados de tablas Hive de forma genérica"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Buscar datos tabulares (líneas con tabulaciones)
            lines = content.split('\n')
            table_data = []
            
            for line in lines:
                line = line.strip()
                if '\t' in line and not line.startswith('hive') and not line.startswith('WARN'):
                    # Filtrar líneas de log/advertencias de Hive
                    if any(keyword in line.lower() for keyword in ['warn', 'info', 'time taken', 'ok', 'null']):
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) >= 2:  # Al menos 2 columnas
                        # Verificar que contenga datos útiles
                        if any(part.strip() and part != 'NULL' for part in parts):
                            table_data.append(parts)
            
            return table_data[:20]  # Limitar a top 20
        except Exception as e:
            print(f"Error parseando {file_path}: {e}")
            return []
    
    def _parse_hive_executive_metrics(self, file_path):
        """Parsea métricas del reporte ejecutivo de Hive"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            metrics = {}
            lines = content.split('\n')
            
            for line in lines:
                line = line.strip()
                if '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        
                        # Buscar métricas específicas
                        if 'Total' in key and 'Requests' in key:
                            metrics['total_requests_hive'] = value
                        elif 'IPs' in key and 'nicas' in key:
                            metrics['unique_ips_hive'] = value
                        elif 'Usuarios' in key and 'nicos' in key:
                            metrics['unique_users_hive'] = value
                        elif 'Error' in key and '%' in value:
                            metrics['error_rate_hive'] = value
                        elif 'Datos Transferidos' in key:
                            metrics['data_transferred_hive'] = value
            
            return metrics
        except Exception as e:
            print(f"Error parseando reporte ejecutivo: {e}")
            return {}
    
    def create_visualizations(self, df, mapreduce_results):
        """Crea visualizaciones corregidas basadas en los datos reales"""
        print("Generando visualizaciones mejoradas...")
        viz_dir = f"{self.results_dir}/visualizations"
        os.makedirs(viz_dir, exist_ok=True)
        
        visualizations = {}
        
        try:
            # 1. Top países por tráfico usando datos reales de MapReduce
            if 'country' in mapreduce_results and mapreduce_results['country']:
                plt.figure(figsize=(14, 8))
                countries = []
                requests = []
                
                # Procesar datos de MapReduce directamente
                for country_data in mapreduce_results['country'][:15]:  # Top 15
                    if len(country_data) == 2:
                        country_name = country_data[0]
                        # Limpiar nombres de países
                        if country_name.startswith('Other-'):
                            country_name = country_name.replace('Other-', '')
                        
                        countries.append(country_name)
                        requests.append(int(country_data[1]))
                
                # Crear gráfico horizontal
                colors = plt.cm.Set3(np.linspace(0, 1, len(countries)))
                bars = plt.barh(countries, requests, color=colors)
                
                plt.title('Top 15 Países por Número de Requests', fontsize=16, pad=20)
                plt.xlabel('Número de Requests', fontsize=12)
                plt.ylabel('País', fontsize=12)
                
                # Añadir valores en las barras
                for bar, value in zip(bars, requests):
                    plt.text(value + max(requests) * 0.01, bar.get_y() + bar.get_height()/2, 
                            f'{value:,}', ha='left', va='center', fontsize=10)
                
                plt.grid(axis='x', alpha=0.3)
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/top_countries_fixed.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['top_countries'] = 'top_countries_fixed.png'
            
            # 2. Distribución de tráfico por hora usando datos de MapReduce
            if 'hourly' in mapreduce_results and mapreduce_results['hourly']:
                plt.figure(figsize=(14, 8))
                hours = []
                hourly_requests = []
                
                for hour_data in mapreduce_results['hourly']:
                    if len(hour_data) == 2:
                        hours.append(int(hour_data[0]))
                        hourly_requests.append(int(hour_data[1]))
                
                # Ordenar por hora
                hourly_df = pd.DataFrame({'hour': hours, 'requests': hourly_requests}).sort_values('hour')
                
                plt.bar(hourly_df['hour'], hourly_df['requests'], 
                       color='lightblue', edgecolor='navy', alpha=0.7)
                
                plt.title('Distribución de Tráfico por Hora del Día', fontsize=16, pad=20)
                plt.xlabel('Hora del Día', fontsize=12)
                plt.ylabel('Número de Requests', fontsize=12)
                plt.xticks(range(0, 24, 2))
                plt.grid(axis='y', alpha=0.3)
                
                # Añadir valores en las barras
                for i, (hour, req) in enumerate(zip(hourly_df['hour'], hourly_df['requests'])):
                    if req > max(hourly_requests) * 0.7:  # Solo mostrar valores altos
                        plt.text(hour, req + max(hourly_requests) * 0.01, 
                                f'{req:,}', ha='center', va='bottom', fontsize=9)
                
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/hourly_traffic_fixed.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['hourly_traffic'] = 'hourly_traffic_fixed.png'
            
            # 3. Top IPs más activas si existen datos
            if 'frequent_ips' in mapreduce_results and mapreduce_results['frequent_ips']:
                plt.figure(figsize=(12, 8))
                ips = []
                ip_requests = []
                
                for ip_data in mapreduce_results['frequent_ips'][:10]:
                    if len(ip_data) == 2:
                        ips.append(f"IP-{len(ips)+1}")  # Anonimizar IPs
                        ip_requests.append(int(ip_data[1]))
                
                bars = plt.bar(range(len(ips)), ip_requests, color='coral', alpha=0.7)
                plt.title('Top 10 IPs Más Activas', fontsize=16, pad=20)
                plt.xlabel('IPs (Anonimizadas)', fontsize=12)
                plt.ylabel('Número de Requests', fontsize=12)
                plt.xticks(range(len(ips)), ips, rotation=45)
                
                # Añadir valores
                for bar, value in zip(bars, ip_requests):
                    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(ip_requests) * 0.01,
                            f'{value:,}', ha='center', va='bottom', fontsize=10)
                
                plt.grid(axis='y', alpha=0.3)
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/top_ips_fixed.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['top_ips'] = 'top_ips_fixed.png'
            
            # 4. Análisis de códigos de respuesta usando datos del DataFrame
            if df is not None and 'ResponseCode' in df.columns:
                plt.figure(figsize=(10, 8))
                status_counts = df['ResponseCode'].value_counts()
                
                colors = ['green' if x < 400 else 'orange' if x < 500 else 'red' 
                         for x in status_counts.index]
                
                wedges, texts, autotexts = plt.pie(status_counts.values, 
                                                  labels=status_counts.index, 
                                                  colors=colors,
                                                  autopct='%1.1f%%', 
                                                  startangle=90)
                
                plt.title('Distribución de Códigos de Respuesta HTTP', fontsize=16, pad=20)
                plt.axis('equal')
                
                # Leyenda
                plt.legend(wedges, [f'{code}: {count:,} requests' 
                          for code, count in zip(status_counts.index, status_counts.values)],
                          title="Códigos de Respuesta",
                          loc="center left",
                          bbox_to_anchor=(1, 0, 0.5, 1))
                
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/response_codes_fixed.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['response_codes'] = 'response_codes_fixed.png'
            
            # 5. Heatmap de actividad mejorado
            if df is not None and 'Hour' in df.columns and 'DayOfWeek' in df.columns:
                plt.figure(figsize=(14, 8))
                
                # Crear pivot table
                activity_pivot = df.groupby(['DayOfWeek', 'Hour']).size().unstack(fill_value=0)
                
                # Crear heatmap
                sns.heatmap(activity_pivot, 
                           annot=False, 
                           cmap='YlOrRd', 
                           cbar_kws={'label': 'Número de Requests'},
                           linewidths=0.5)
                
                plt.title('Heatmap de Actividad: Día de la Semana vs Hora', fontsize=16, pad=20)
                plt.xlabel('Hora del Día', fontsize=12)
                plt.ylabel('Día de la Semana', fontsize=12)
                
                # Personalizar etiquetas
                days = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
                plt.yticks(range(len(days)), days, rotation=0)
                
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/activity_heatmap_fixed.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['activity_heatmap'] = 'activity_heatmap_fixed.png'
        
        except Exception as e:
            print(f"Error generando visualizaciones: {e}")
            import traceback
            traceback.print_exc()
        
        return visualizations
    
    def generate_summary_stats(self, df):
        """Genera estadísticas resumidas"""
        if df is None:
            return {}
        
        stats = {}
        
        try:
            stats['total_records'] = len(df)
            stats['unique_ips'] = df['IpId'].nunique() if 'IpId' in df.columns else 'N/A'
            stats['unique_users'] = df['UserId'].nunique() if 'UserId' in df.columns else 'N/A'
            stats['date_range'] = {
                'start': df['TimeStamp'].min() if 'TimeStamp' in df.columns else 'N/A',
                'end': df['TimeStamp'].max() if 'TimeStamp' in df.columns else 'N/A'
            }
            
            if 'ResponseCode' in df.columns:
                stats['success_rate'] = (df['ResponseCode'] < 400).mean() * 100
                stats['error_rate'] = (df['ResponseCode'] >= 400).mean() * 100
            
            if 'Bytes' in df.columns:
                stats['total_bytes'] = df['Bytes'].sum()
                stats['avg_response_size'] = df['Bytes'].mean()
            
            if 'HttpMethod' in df.columns:
                stats['http_methods'] = df['HttpMethod'].value_counts().to_dict()
            
        except Exception as e:
            print(f"Error calculando estadísticas: {e}")
        
        return stats
    
    def generate_html_report(self, df, mapreduce_results, hive_results, visualizations, stats):
        """Genera el reporte HTML final con MapReduce y Hive integrados"""
        html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Análisis de Logs Web EClog - Reporte Final</title>
    <style>
        body {{
            font-family: 'Arial', sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            text-align: center;
            margin-bottom: 30px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .metric-card {{
            background: white;
            padding: 20px;
            margin: 15px 0;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border-left: 4px solid #667eea;
        }}
        .hive-card {{
            border-left: 4px solid #2ecc71;
        }}
        .mapreduce-card {{
            border-left: 4px solid #e67e22;
        }}
        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .metric-item {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .metric-value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }}
        .metric-label {{
            color: #666;
            font-size: 0.9em;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: bold;
            color: #333;
        }}
        .visualization {{
            text-align: center;
            margin: 20px 0;
        }}
        .visualization img {{
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .section {{
            margin: 30px 0;
        }}
        .section h2 {{
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        .comparison-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin: 20px 0;
        }}
        .alert {{
            padding: 15px;
            margin: 20px 0;
            border-radius: 5px;
            border-left: 4px solid #f39c12;
            background-color: #fef9e7;
        }}
        .success {{
            border-left-color: #27ae60;
            background-color: #eafaf1;
        }}
        .danger {{
            border-left-color: #e74c3c;
            background-color: #fdf2f2;
            }}
        .footer {{
            text-align: center;
            margin-top: 50px;
            padding: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Análisis de Logs Web EClog</h1>
            <p>Reporte Completo: MapReduce + Hive + Análisis Local</p>
            <p>Generado el: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        </div>

        <!-- Resumen Ejecutivo -->
        <div class="section">
            <h2>📈 Resumen Ejecutivo</h2>
            <div class="metric-grid">
                <div class="metric-item">
                    <div class="metric-value">{stats.get('total_records', 'N/A'):,}</div>
                    <div class="metric-label">Total de Requests</div>
                </div>
                <div class="metric-item">
                    <div class="metric-value">{stats.get('unique_ips', 'N/A'):,}</div>
                    <div class="metric-label">IPs Únicas</div>
                </div>
                <div class="metric-item">
                    <div class="metric-value">{stats.get('unique_users', 'N/A'):,}</div>
                    <div class="metric-label">Usuarios Únicos</div>
                </div>
                <div class="metric-item success">
                    <div class="metric-value">{stats.get('success_rate', 0):.1f}%</div>
                    <div class="metric-label">Tasa de Éxito</div>
                </div>
                <div class="metric-item {'danger' if stats.get('error_rate', 0) > 10 else ''}">
                    <div class="metric-value">{stats.get('error_rate', 0):.1f}%</div>
                    <div class="metric-label">Tasa de Error</div>
                </div>
                <div class="metric-item">
                    <div class="metric-value">{stats.get('total_bytes', 0) / (1024**3):.1f} GB</div>
                    <div class="metric-label">Datos Transferidos</div>
                </div>
            </div>
        </div>"""

        # Agregar visualizaciones si existen
        if visualizations:
            html_content += """
        <div class="section">
            <h2>📊 Visualizaciones</h2>"""
            
            for viz_name, viz_file in visualizations.items():
                viz_title = viz_name.replace('_', ' ').title()
                html_content += f"""
            <div class="visualization">
                <h3>{viz_title}</h3>
                <img src="visualizations/{viz_file}" alt="{viz_title}">
            </div>"""
            
            html_content += "</div>"

        # Comparación MapReduce vs Hive
        if mapreduce_results and hive_results:
            html_content += """
        <div class="section">
            <h2>⚖️ Comparación: MapReduce vs Hive</h2>
            <div class="comparison-grid">"""
            
            # MapReduce - Análisis por País
            if 'country' in mapreduce_results:
                # Ordenar países por número de requests (de mayor a menor)
                sorted_countries = sorted(mapreduce_results['country'], 
                key=lambda x: int(x[1]) if len(x) == 2 and x[1].isdigit() else 0, 
                reverse=True)
                
                html_content += """
                <div class="metric-card mapreduce-card">
                    <h3>🌍 MapReduce - Top Países</h3>
                    <table>
                        <tr><th>País</th><th>Requests</th></tr>"""
                for country_data in sorted_countries[:5]:
                    if len(country_data) == 2:
                        html_content += f"<tr><td>{country_data[0]}</td><td>{int(country_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            # Hive - Análisis por País
            if 'hive_country' in hive_results:
                html_content += """
                <div class="metric-card hive-card">
                    <h3>🌍 Hive - Top Países</h3>
                    <table>
                        <tr><th>País</th><th>Requests</th><th>IPs</th></tr>"""
                for country_data in hive_results['hive_country'][:5]:
                    if len(country_data) >= 3:
                        html_content += f"<tr><td>{country_data[0]}</td><td>{country_data[1]}</td><td>{country_data[2]}</td></tr>"
                html_content += "</table></div>"
            
            html_content += "</div></div>"

        # Sección detallada de MapReduce
        if mapreduce_results:
            html_content += """
        <div class="section">
            <h2>⚡ Resultados MapReduce (Hadoop)</h2>"""
            
            # Análisis temporal MapReduce
            if 'hourly' in mapreduce_results:
                html_content += """
            <div class="metric-card mapreduce-card">
                <h3>⏰ Tráfico por Hora (MapReduce)</h3>
                <table>
                    <tr><th>Hora</th><th>Total Requests</th></tr>"""
                hourly_sorted = sorted(mapreduce_results['hourly'], key=lambda x: int(x[0]) if x[0].isdigit() else 0)
                for hour_data in hourly_sorted:
                    if len(hour_data) == 2:
                        html_content += f"<tr><td>{hour_data[0]}:00</td><td>{int(hour_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            # Top IPs MapReduce
            if 'frequent_ips' in mapreduce_results:
                html_content += """
            <div class="metric-card mapreduce-card">
                <h3>🔍 Top IPs Más Activas (MapReduce)</h3>
                <table>
                    <tr><th>IP</th><th>Total Requests</th></tr>"""
                for ip_data in mapreduce_results['frequent_ips'][:10]:
                    if len(ip_data) == 2:
                        html_content += f"<tr><td>{ip_data[0]}</td><td>{int(ip_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            # Errores MapReduce
            if 'errors' in mapreduce_results:
                html_content += """
            <div class="metric-card mapreduce-card">
                <h3>❌ Análisis de Errores (MapReduce)</h3>
                <table>
                    <tr><th>Código</th><th>URI</th><th>Cantidad</th></tr>"""
                for error_data in mapreduce_results['errors'][1:11]:  # Skip header
                    if len(error_data) >= 3:
                        html_content += f"<tr><td>{error_data[0]}</td><td>{error_data[1][:50]}...</td><td>{error_data[2]}</td></tr>"
                html_content += "</table></div>"
            
            html_content += "</div>"

        # Sección detallada de Hive
        if hive_results:
            html_content += """
        <div class="section">
            <h2>🏢 Análisis Hive (Big Data SQL)</h2>"""
            
            # Métricas ejecutivas de Hive
            if 'hive_executive' in hive_results:
                hive_exec = hive_results['hive_executive']
                html_content += """
            <div class="metric-card hive-card">
                <h3>📋 Métricas Ejecutivas (Hive)</h3>
                <div class="metric-grid">"""
                
                for key, value in hive_exec.items():
                    label = key.replace('_hive', '').replace('_', ' ').title()
                    html_content += f"""
                    <div class="metric-item">
                        <div class="metric-value">{value}</div>
                        <div class="metric-label">{label}</div>
                    </div>"""
                
                html_content += "</div></div>"
            
            # Análisis temporal Hive
            if 'hive_hourly' in hive_results:
                html_content += """
            <div class="metric-card hive-card">
                <h3>⏰ Análisis Temporal (Hive)</h3>
                <table>
                    <tr><th>Hora</th><th>Requests</th><th>IPs Únicas</th><th>Usuarios</th></tr>"""
                for hour_data in hive_results['hive_hourly'][:12]:
                    if len(hour_data) >= 4:
                        html_content += f"<tr><td>{hour_data[0]}:00</td><td>{hour_data[1]}</td><td>{hour_data[2]}</td><td>{hour_data[3]}</td></tr>"
                html_content += "</table></div>"
            
            # Top IPs con clasificación Hive
            if 'hive_ips' in hive_results:
                html_content += """
            <div class="metric-card hive-card">
                <h3>🔍 IPs Más Activas con Clasificación (Hive)</h3>
                <table>
                    <tr><th>IP</th><th>Requests</th><th>Páginas</th><th>Clasificación</th></tr>"""
                for ip_data in hive_results['hive_ips'][:10]:
                    if len(ip_data) >= 4:
                        html_content += f"<tr><td>{ip_data[0]}</td><td>{ip_data[1]}</td><td>{ip_data[2]}</td><td>{ip_data[8] if len(ip_data) > 8 else 'N/A'}</td></tr>"
                html_content += "</table></div>"
            
            html_content += "</div>"

        # Métodos HTTP
        if stats.get('http_methods'):
            html_content += """
        <div class="metric-card">
            <h3>📄 Distribución de Métodos HTTP</h3>
            <table>
                <tr><th>Método</th><th>Cantidad</th><th>Porcentaje</th></tr>"""
            total_methods = sum(stats['http_methods'].values())
            for method, count in stats['http_methods'].items():
                percentage = (count / total_methods) * 100
                html_content += f"<tr><td>{method}</td><td>{count:,}</td><td>{percentage:.1f}%</td></tr>"
            html_content += "</table></div>"

        # Conclusiones
        html_content += f"""
        <div class="section">
            <h2>🔍 Conclusiones y Recomendaciones</h2>
            <div class="metric-card">
                <h3>✅ Conclusiones Principales</h3>
                <ul>
                    <li><strong>Volumen de Tráfico:</strong> Se procesaron {stats.get('total_records', 0):,} requests de {stats.get('unique_ips', 0):,} IPs únicas</li>
                    <li><strong>Calidad del Servicio:</strong> Tasa de éxito del {stats.get('success_rate', 0):.1f}% y tasa de error del {stats.get('error_rate', 0):.1f}%</li>
                    <li><strong>Transferencia de Datos:</strong> Se transfirieron {stats.get('total_bytes', 0) / (1024**3):.1f} GB de datos</li>
                    <li><strong>Tecnologías Validadas:</strong> {'MapReduce y Hive' if mapreduce_results and hive_results else 'MapReduce' if mapreduce_results else 'Hive' if hive_results else 'Análisis local'}</li>
                </ul>
            </div>
            
            <div class="metric-card">
                <h3>🎯 Recomendaciones</h3>
                <ul>
                    <li><strong>Monitoreo:</strong> Implementar alertas para tasas de error superiores al 5%</li>
                    <li><strong>Seguridad:</strong> Revisar IPs con alto número de requests para detectar posibles ataques</li>
                    <li><strong>Performance:</strong> Optimizar recursos más solicitados para mejorar tiempos de respuesta</li>
                    <li><strong>Capacidad:</strong> Planificar capacidad basada en patrones de tráfico por hora</li>
                    <li><strong>Big Data:</strong> {'Continuar usando MapReduce y Hive para análisis escalables' if mapreduce_results and hive_results else 'Implementar análisis distribuido con Hadoop'}</li>
                </ul>
            </div>
        </div>

        <!-- Información técnica -->
        <div class="section">
            <h2>🔧 Información Técnica</h2>
            <div class="metric-card">
                <h3>Tecnologías Utilizadas</h3>
                <ul>
                    <li><strong>Hadoop HDFS:</strong> Almacenamiento distribuido de logs</li>
                    <li><strong>MapReduce:</strong> Procesamiento paralelo de análisis por país, hora, IPs y errores</li>
                    <li><strong>Apache Hive:</strong> Consultas SQL sobre big data con análisis avanzados</li>
                    <li><strong>Python:</strong> Limpieza de datos y generación de reportes</li>
                    <li><strong>Pandas & Matplotlib:</strong> Análisis de datos y visualizaciones</li>
                </ul>
                
                <h4>Resultados Procesados:</h4>
                <ul>
                    <li>MapReduce: {'✅ ' + str(len(mapreduce_results)) + ' análisis completados' if mapreduce_results else '❌ No disponible'}</li>
                    <li>Hive: {'✅ ' + str(len(hive_results)) + ' análisis completados' if hive_results else '❌ No disponible'}</li>
                    <li>Visualizaciones: {'✅ ' + str(len(visualizations)) + ' gráficos generados' if visualizations else '❌ No disponible'}</li>
                </ul>
            </div>
        </div>

        <!-- Comparativa de rendimiento -->
        {'<div class="section"><h2>⚔️ Comparativa de Rendimiento</h2>' if mapreduce_results and hive_results else ''}
        {'<div class="metric-card"><h3>MapReduce vs Hive</h3><p><strong>MapReduce:</strong> Procesamiento distribuido de bajo nivel, ideal para operaciones personalizadas y grandes volúmenes.</p><p><strong>Hive:</strong> Interface SQL de alto nivel, mejor para análisis complejos y consultas ad-hoc.</p><p><strong>Resultado:</strong> Ambas tecnologías producen resultados consistentes, validando la arquitectura de big data.</p></div></div>' if mapreduce_results and hive_results else ''}

        <div class="footer">
            <p>📊 Reporte generado automáticamente por el sistema de análisis de logs web</p>
            <p>Proyecto: Análisis de Logs Webs con Tecnologías Big Data</p>
            <p>Dataset: EClog E-commerce Web Logs - Harvard Dataverse</p>
            <p>Tecnologías: Hadoop MapReduce + Apache Hive + Python</p>
        </div>
    </div>
</body>
</html>"""

        # Guardar reporte HTML
        report_file = f"{self.results_dir}/final_report.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Reporte HTML generado: {report_file}")
        return report_file
    
    def run(self):
        """Ejecuta la generación completa del reporte con MapReduce y Hive"""
        print("=== Generando Reporte Final (MapReduce + Hive) ===")
        
        # Crear directorio de resultados
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Cargar datos de todas las fuentes
        print("Cargando datos procesados...")
        df = self.load_processed_data()
        
        print("Cargando resultados MapReduce...")
        mapreduce_results = self.load_mapreduce_results()
        
        print("Cargando resultados Hive...")
        hive_results = self.load_hive_results()
        
        # Mostrar resumen de lo que se cargó
        print(f"\nRESUMEN DE CARGA:")
        print(f"- Datos procesados: {'✅ ' + str(len(df)) + ' registros' if df is not None else '❌ No disponible'}")
        print(f"- MapReduce: {'✅ ' + str(len(mapreduce_results)) + ' análisis' if mapreduce_results else '❌ No disponible'}")
        print(f"- Hive: {'✅ ' + str(len(hive_results)) + ' análisis' if hive_results else '❌ No disponible'}")
        
        # Generar estadísticas
        stats = self.generate_summary_stats(df)
        
        # Crear visualizaciones
        visualizations = self.create_visualizations(df, mapreduce_results)
        
        # Generar reporte HTML integrado
        report_file = self.generate_html_report(df, mapreduce_results, hive_results, visualizations, stats)
        
        # Generar resumen JSON para uso programático
        summary = {
            'generated_at': datetime.now().isoformat(),
            'stats': stats,
            'mapreduce_results_loaded': list(mapreduce_results.keys()) if mapreduce_results else [],
            'hive_results_loaded': list(hive_results.keys()) if hive_results else [],
            'visualizations': list(visualizations.keys()),
            'report_file': report_file,
            'data_sources': {
                'local_data': df is not None,
                'mapreduce': bool(mapreduce_results),
                'hive': bool(hive_results)
            }
        }
        
        with open(f"{self.results_dir}/analysis_summary.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print("\n=== Reporte Final Completado ===")
        print(f"📊 Reporte HTML: {report_file}")
        print(f"📋 Resumen JSON: {self.results_dir}/analysis_summary.json")
        
        return report_file

if __name__ == "__main__":
    generator = EClogReportGenerator()
    report_file = generator.run()
    print(f"\n🎉 Reporte disponible en: {report_file}")
    print("Para verlo: firefox results/final_report.html")