#!/usr/bin/env python3
"""
Mapper simple para analizar tráfico por hora
Input: TSV EClog con columna Hour
Output: hora \t 1
"""
import sys

first_line = True

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    # Saltar header
    if first_line:
        first_line = False
        if line.startswith('IpId'):
            continue
    
    try:
        fields = line.split('\t')
        if len(fields) >= 18:  # Verificar que tenemos todas las columnas
            hour = fields[10].strip()  # Hour es la columna 10 (índice 10)
            
            # Validar que es un número válido
            try:
                hour_int = int(hour)
                if 0 <= hour_int <= 23:
                    print(f"{hour_int}\t1")
            except ValueError:
                # Si no es un número válido, intentar extraer del timestamp
                timestamp_str = fields[2].strip()  # TimeStamp
                try:
                    from datetime import datetime
                    # El formato parece ser: 1990-03-11 00:42:36.000
                    timestamp = datetime.strptime(timestamp_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    hour_from_timestamp = timestamp.hour
                    print(f"{hour_from_timestamp}\t1")
                except:
                    continue
                    
    except Exception:
        # Ignorar líneas malformadas
        continue