#!/usr/bin/env python3
"""
Generador de reporte final integrado
Combina resultados de MapReduce, Hive y análisis locales
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import json
import glob

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
    
    def create_visualizations(self, df):
        """Crea visualizaciones basadas en los datos"""
        if df is None:
            return {}
        
        print("Generando visualizaciones...")
        viz_dir = f"{self.results_dir}/visualizations"
        os.makedirs(viz_dir, exist_ok=True)
        
        visualizations = {}
        
        try:
            # 1. Distribución de tráfico por hora
            if 'Hour' in df.columns:
                plt.figure(figsize=(12, 6))
                hourly_counts = df['Hour'].value_counts().sort_index()
                hourly_counts.plot(kind='bar')
                plt.title('Distribución de Tráfico por Hora del Día')
                plt.xlabel('Hora')
                plt.ylabel('Número de Requests')
                plt.xticks(rotation=0)
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/hourly_traffic.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['hourly_traffic'] = 'hourly_traffic.png'
            
            # 2. Top países por tráfico
            if 'Country' in df.columns:
                plt.figure(figsize=(10, 6))
                country_counts = df['Country'].value_counts().head(10)
                country_counts.plot(kind='barh')
                plt.title('Top 10 Países por Tráfico')
                plt.xlabel('Número de Requests')
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/top_countries.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['top_countries'] = 'top_countries.png'
            
            # 3. Distribución de códigos de respuesta
            if 'ResponseCode' in df.columns:
                plt.figure(figsize=(10, 6))
                status_counts = df['ResponseCode'].value_counts()
                colors = ['green' if x < 400 else 'orange' if x < 500 else 'red' for x in status_counts.index]
                status_counts.plot(kind='bar', color=colors)
                plt.title('Distribución de Códigos de Respuesta HTTP')
                plt.xlabel('Código de Respuesta')
                plt.ylabel('Número de Requests')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/response_codes.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['response_codes'] = 'response_codes.png'
            
            # 4. Análisis de bots vs humanos
            if 'IsBot' in df.columns:
                plt.figure(figsize=(8, 6))
                bot_counts = df['IsBot'].value_counts()
                bot_counts.index = ['Humanos' if not x else 'Bots' for x in bot_counts.index]
                plt.pie(bot_counts.values, labels=bot_counts.index, autopct='%1.1f%%')
                plt.title('Distribución: Tráfico de Bots vs Humanos')
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/bots_vs_humans.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['bots_vs_humans'] = 'bots_vs_humans.png'
            
            # 5. Heatmap de actividad por hora y día de la semana
            if 'Hour' in df.columns and 'DayOfWeek' in df.columns:
                plt.figure(figsize=(12, 8))
                activity_pivot = df.groupby(['DayOfWeek', 'Hour']).size().unstack(fill_value=0)
                sns.heatmap(activity_pivot, annot=False, cmap='YlOrRd', cbar_kws={'label': 'Número de Requests'})
                plt.title('Heatmap de Actividad: Día de la Semana vs Hora')
                plt.xlabel('Hora del Día')
                plt.ylabel('Día de la Semana (0=Lunes)')
                plt.tight_layout()
                plt.savefig(f"{viz_dir}/activity_heatmap.png", dpi=300, bbox_inches='tight')
                plt.close()
                visualizations['activity_heatmap'] = 'activity_heatmap.png'
        
        except Exception as e:
            print(f"Error generando visualizaciones: {e}")
        
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
    
    def generate_html_report(self, df, mapreduce_results, visualizations, stats):
        """Genera el reporte HTML final"""
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
        .code-block {{
            background: #2d3748;
            color: #e2e8f0;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            font-family: 'Courier New', monospace;
            margin: 20px 0;
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
            <p>Reporte Completo de Análisis de Big Data</p>
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
        </div>

        <!-- Período de Análisis -->
        <div class="metric-card">
            <h3>📅 Período de Análisis</h3>
            <p><strong>Inicio:</strong> {stats.get('date_range', {}).get('start', 'N/A')}</p>
            <p><strong>Fin:</strong> {stats.get('date_range', {}).get('end', 'N/A')}</p>
        </div>"""

        # Agregar visualizaciones
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

        # Agregar resultados de MapReduce
        if mapreduce_results:
            html_content += """
        <div class="section">
            <h2>⚡ Resultados MapReduce</h2>"""
            
            # Resultados por país
            if 'country' in mapreduce_results:
                html_content += """
            <div class="metric-card">
                <h3>🌍 Análisis por País (Top 10)</h3>
                <table>
                    <tr><th>País</th><th>Total Requests</th></tr>"""
                for country_data in mapreduce_results['country'][:10]:
                    if len(country_data) == 2:
                        html_content += f"<tr><td>{country_data[0]}</td><td>{int(country_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            # Resultados por hora
            if 'hourly' in mapreduce_results:
                html_content += """
            <div class="metric-card">
                <h3>⏰ Tráfico por Hora</h3>
                <table>
                    <tr><th>Hora</th><th>Total Requests</th></tr>"""
                hourly_sorted = sorted(mapreduce_results['hourly'], key=lambda x: int(x[0]) if x[0].isdigit() else 0)
                for hour_data in hourly_sorted:
                    if len(hour_data) == 2:
                        html_content += f"<tr><td>{hour_data[0]}:00</td><td>{int(hour_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            # Top IPs
            if 'frequent_ips' in mapreduce_results:
                html_content += """
            <div class="metric-card">
                <h3>🔍 Top IPs Más Activas</h3>
                <table>
                    <tr><th>IP</th><th>Total Requests</th></tr>"""
                for ip_data in mapreduce_results['frequent_ips'][:10]:
                    if len(ip_data) == 2:
                        html_content += f"<tr><td>{ip_data[0]}</td><td>{int(ip_data[1]):,}</td></tr>"
                html_content += "</table></div>"
            
            html_content += "</div>"

        # Métodos HTTP
        if stats.get('http_methods'):
            html_content += """
        <div class="metric-card">
            <h3>🔄 Distribución de Métodos HTTP</h3>
            <table>
                <tr><th>Método</th><th>Cantidad</th><th>Porcentaje</th></tr>"""
            total_methods = sum(stats['http_methods'].values())
            for method, count in stats['http_methods'].items():
                percentage = (count / total_methods) * 100
                html_content += f"<tr><td>{method}</td><td>{count:,}</td><td>{percentage:.1f}%</td></tr>"
            html_content += "</table></div>"

        # Conclusiones y recomendaciones
        html_content += f"""
        <div class="section">
            <h2>📝 Conclusiones y Recomendaciones</h2>
            <div class="metric-card">
                <h3>✅ Conclusiones Principales</h3>
                <ul>
                    <li><strong>Volumen de Tráfico:</strong> Se procesaron {stats.get('total_records', 0):,} requests de {stats.get('unique_ips', 0):,} IPs únicas</li>
                    <li><strong>Calidad del Servicio:</strong> Tasa de éxito del {stats.get('success_rate', 0):.1f}% y tasa de error del {stats.get('error_rate', 0):.1f}%</li>
                    <li><strong>Transferencia de Datos:</strong> Se transfirieron {stats.get('total_bytes', 0) / (1024**3):.1f} GB de datos</li>
                </ul>
            </div>
            
            <div class="metric-card">
                <h3>🎯 Recomendaciones</h3>
                <ul>
                    <li><strong>Monitoreo:</strong> Implementar alertas para tasas de error superiores al 5%</li>
                    <li><strong>Seguridad:</strong> Revisar IPs con alto número de requests para detectar posibles ataques</li>
                    <li><strong>Performance:</strong> Optimizar recursos más solicitados para mejorar tiempos de respuesta</li>
                    <li><strong>Capacidad:</strong> Planificar capacidad basada en patrones de tráfico por hora</li>
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
                    <li><strong>Apache Hive:</strong> Consultas SQL sobre big data</li>
                    <li><strong>Python:</strong> Limpieza de datos y generación de reportes</li>
                    <li><strong>Pandas & Matplotlib:</strong> Análisis de datos y visualizaciones</li>
                </ul>
            </div>
        </div>

        <div class="footer">
            <p>📊 Reporte generado automáticamente por el sistema de análisis de logs web</p>
            <p>Proyecto: Análisis de Logs Web con Tecnologías Big Data</p>
            <p>Dataset: EClog E-commerce Web Logs - Harvard Dataverse</p>
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
        """Ejecuta la generación completa del reporte"""
        print("=== Generando Reporte Final ===")
        
        # Crear directorio de resultados
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Cargar datos
        df = self.load_processed_data()
        mapreduce_results = self.load_mapreduce_results()
        
        # Generar estadísticas
        stats = self.generate_summary_stats(df)
        
        # Crear visualizaciones
        visualizations = self.create_visualizations(df)
        
        # Generar reporte HTML
        report_file = self.generate_html_report(df, mapreduce_results, visualizations, stats)
        
        # Generar resumen JSON para uso programático
        summary = {
            'generated_at': datetime.now().isoformat(),
            'stats': stats,
            'visualizations': list(visualizations.keys()),
            'report_file': report_file
        }
        
        with open(f"{self.results_dir}/analysis_summary.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print("=== Reporte Final Completado ===")
        return report_file

if __name__ == "__main__":
    generator = EClogReportGenerator()
    report_file = generator.run()
    print(f"\n🎉 Reporte disponible en: {report_file}")
    print("Para verlo: firefox results/final_report.html")