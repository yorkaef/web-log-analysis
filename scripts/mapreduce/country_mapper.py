#!/usr/bin/env python3
"""
Mapper para contar visitas por país
Input: TSV con columnas del dataset EClog
Output: país \t 1
"""
import sys

def get_country_from_ip(ip):
    """Clasificación simple de país por IP"""
    if ip.startswith('192.168') or ip.startswith('10.') or ip.startswith('172.'):
        return 'Local'
    elif ip.startswith(('203.', '210.', '202.', '124.')):
        return 'Asia-Pacific'
    elif ip.startswith(('91.', '85.', '94.', '88.')):
        return 'Europe'
    elif ip.startswith(('198.', '199.', '208.', '204.')):
        return 'North-America'
    elif ip.startswith(('200.', '201.', '186.')):
        return 'South-America'
    elif ip.startswith(('196.', '197.', '41.')):
        return 'Africa'
    else:
        return 'Unknown'

# Procesar header (primera línea)
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
        if len(fields) >= 10:  # Verificar que tenemos todas las columnas
            ip_id = fields[0]  # IpId es la primera columna
            country = get_country_from_ip(ip_id)
            print(f"{country}\t1")
    except Exception as e:
        # Ignorar líneas malformadas
        continue