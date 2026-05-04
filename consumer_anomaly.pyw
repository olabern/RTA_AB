from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_events = defaultdict(list)

print("Nasłuchuję na anomalie (>3 transakcje w 60s)...\n")

for message in consumer:
    tx = message.value
    print(tx)
    user = tx['user_id']
    now = datetime.fromisoformat(tx['timestamp'])

    user_events[user].append(now)

    # usuń stare zdarzenia (starsze niż 60 sekund)
    user_events[user] = [
        t for t in user_events[user]
        if now - t <= timedelta(seconds=60)
    ]

    if len(user_events[user]) > 3:
        print(f"ALERT: {user} zrobił {len(user_events[user])} transakcji w 60s")
