#!/usr/bin/env python3
"""
Mapper simple para analizar errores HTTP
Input: TSV EClog
Output: error_code \t uri (solo para códigos 4xx y 5xx)
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
        if len(fields) >= 18:
            response_code = fields[6].strip()  # ResponseCode es columna 6 (índice 6)
            uri = fields[4].strip()  # Uri es columna 4 (índice 4)
            
            try:
                code = int(response_code)
                if code >= 400:  # Solo errores 4xx y 5xx
                    print(f"{code}\t{uri}")
            except ValueError:
                continue
    except Exception:
        continue