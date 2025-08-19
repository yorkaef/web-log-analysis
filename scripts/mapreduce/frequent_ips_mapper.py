#!/usr/bin/env python3
"""
Mapper para contar requests por IP
Input: TSV EClog
Output: ip \t 1
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
        if len(fields) >= 10:
            ip_id = fields[0].strip()  # IpId
            if ip_id and ip_id != '-':
                print(f"{ip_id}\t1")
    except Exception:
        continue