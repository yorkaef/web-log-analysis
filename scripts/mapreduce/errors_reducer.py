#!/usr/bin/env python3
"""
Reducer para contar errores por código y URI
"""
import sys

error_counts = {}

for line in sys.stdin:
    line = line.strip()
    
    try:
        code, uri = line.split('\t', 1)  # Split solo en el primer tab
        key = f"{code}:{uri}"
        
        if key in error_counts:
            error_counts[key] += 1
        else:
            error_counts[key] = 1
    except ValueError:
        continue

# Ordenar por frecuencia descendente
sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)

# Imprimir resultados
print("ERROR_CODE\tURI\tCOUNT")
for error_key, count in sorted_errors:
    code, uri = error_key.split(':', 1)
    print(f"{code}\t{uri}\t{count}")