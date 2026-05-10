from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Nasłuchuję... Tabela zagregowanych danych pojawi się po zebraniu 10 transakcji.")

for message in consumer:
    transaction = message.value
    store = transaction['store']
    amount = transaction['amount']
    
    store_counts[store] += 1
    
    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "=" * 55)
        print(f"{'Sklep':<12} | {'Liczba':<8} | {'Suma (PLN)':<12} | {'Średnia (PLN)':<12}")
        print("-" * 55)
        
        for s, count in store_counts.items():
            suma = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} | {count:<8} | {suma:<12.2f} | {srednia:<12.2f}")
        
        print("=" * 55)
