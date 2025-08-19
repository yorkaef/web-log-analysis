#!/usr/bin/env python3
"""
Reducer para contar y ordenar IPs por frecuencia
"""
import sys

ip_counts = {}

for line in sys.stdin:
    line = line.strip()
    
    try:
        ip, count = line.split('\t')
        count = int(count)
        
        if ip in ip_counts:
            ip_counts[ip] += count
        else:
            ip_counts[ip] = count
    except ValueError:
        continue

# Ordenar por frecuencia descendente
sorted_ips = sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)

# Imprimir top 100
for ip, count in sorted_ips[:100]:
    print(f"{ip}\t{count}")