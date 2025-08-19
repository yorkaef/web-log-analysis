#!/usr/bin/env python3
"""
Reducer para sumar visitas por país
Input: país \t 1
Output: país \t total_visitas
"""
import sys

current_country = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    
    try:
        country, count = line.split('\t')
        count = int(count)
        
        if current_country == country:
            current_count += count
        else:
            if current_country:
                print(f"{current_country}\t{current_count}")
            current_country = country
            current_count = count
    except ValueError:
        # Ignorar líneas malformadas
        continue

# Imprimir el último país
if current_country:
    print(f"{current_country}\t{current_count}")