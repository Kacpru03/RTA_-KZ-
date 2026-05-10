from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': float('-inf')})
msg_count = 0

print("Zbieram statystyki per kategoria... Tabela pojawi się po 10 wiadomościach.")

for message in consumer:
    transaction = message.value
    category = transaction['category']
    amount = transaction['amount']
    
    stats[category]['count'] += 1
    stats[category]['total'] += amount
    
    if amount < stats[category]['min']:
        stats[category]['min'] = amount
        
    if amount > stats[category]['max']:
        stats[category]['max'] = amount
        
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "=" * 65)
        print(f"{'Kategoria':<14} | {'Liczba':<8} | {'Przychód':<12} | {'Min':<8} | {'Max':<8}")
        print("-" * 65)
        
        for cat, data in stats.items():
            print(f"{cat:<14} | {data['count']:<8} | {data['total']:<12.2f} | {data['min']:<8.2f} | {data['max']:<8.2f}")
            
        print("=" * 65)
