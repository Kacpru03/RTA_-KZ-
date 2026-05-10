from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='risk_evaluation_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Rozpoczynam ocenę ryzyka transakcji w czasie rzeczywistym...")

for message in consumer:
    transaction = message.value
    amount = transaction['amount']
    
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
        
    transaction['risk_level'] = risk_level
    print(f"[{risk_level}] Zaktualizowano transakcję: {transaction['tx_id']} - Kwota: {amount}")
