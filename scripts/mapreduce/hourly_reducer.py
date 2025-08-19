#!/usr/bin/env python3
"""
Reducer para contar requests por hora
"""
import sys

current_hour = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    
    try:
        hour, count = line.split('\t')
        hour = int(hour)
        count = int(count)
        
        if current_hour == hour:
            current_count += count
        else:
            if current_hour is not None:
                print(f"{current_hour}\t{current_count}")
            current_hour = hour
            current_count = count
    except ValueError:
        continue

if current_hour is not None:
    print(f"{current_hour}\t{current_count}")