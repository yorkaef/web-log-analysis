#!/usr/bin/env python3
"""
Mapper simple para contar visitas por país
Input: TSV EClog con columna Country
Output: país \t 1
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
            country = fields[17].strip()  # Country es la columna 17 (índice 17)
            
            # Si el país está como "Unknown", intentar inferir del IpId
            if country == "Unknown" or not country:
                ip_id = fields[0].strip()  # IpId es la primera columna
                
                # Extraer código de país del IpId (formato: número + código)
                import re
                match = re.search(r'([A-Z]{2})$', ip_id.upper())
                
                if match:
                    country_code = match.group(1)
                    country_mapping = {
                        'PL': 'Poland',
                        'US': 'United States', 
                        'DE': 'Germany',
                        'CA': 'Canada',
                        'NL': 'Netherlands',
                        'GB': 'United Kingdom',
                        'FR': 'France'
                    }
                    country = country_mapping.get(country_code, f'Other-{country_code}')
                else:
                    country = 'Unknown'
            
            print(f"{country}\t1")
            
    except Exception:
        # Ignorar líneas malformadas
        continue