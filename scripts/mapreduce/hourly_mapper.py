#!/usr/bin/env python3
"""
Mapper para analizar tráfico por hora
Input: TSV con columnas EClog
Output: hora \t 1
"""
import sys
from datetime import datetime

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
        if len(fields) >= 10:
            timestamp_str = fields[2]  # TimeStamp es la tercera columna
            
            # Parsear timestamp (formato puede variar)
            try:
                # Intentar varios formatos comunes
                for fmt in ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S']:
                    try:
                        timestamp = datetime.strptime(timestamp_str, fmt)
                        hour = timestamp.hour
                        print(f"{hour}\t1")
                        break
                    except ValueError:
                        continue
            except:
                continue
    except Exception:
        continue