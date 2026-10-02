"""Sends fake events so the dashboard has live data. Usage: python demo_producer.py [topic] [msgs_per_sec]"""
import json, random, sys, time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

topic = sys.argv[1] if len(sys.argv) > 1 else "orders"
rate = float(sys.argv[2]) if len(sys.argv) > 2 else 5
cfg = {"bootstrap.servers": "localhost:9092"}

AdminClient(cfg).create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
p = Producer(cfg)
users = ["asha", "ravi", "mei", "tom", "lena"]
print(f"Producing to '{topic}' at ~{rate}/s. Ctrl+C to stop.")
i = 0
while True:
    evt = {"id": i, "user": random.choice(users), "amount": round(random.uniform(5, 500), 2)}
    p.produce(topic, key=evt["user"], value=json.dumps(evt))
    p.poll(0)
    i += 1
    time.sleep(1 / rate)