#!/usr/bin/env python3
"""
Mapper simple para contar requests por IpId
Input: TSV EClog
Output: ip_id \t 1
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
            ip_id = fields[0].strip()  # IpId es la primera columna (índice 0)
            if ip_id and ip_id != '-':
                print(f"{ip_id}\t1")
    except Exception:
        continue