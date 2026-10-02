"""A slow consumer so you can see consumer-group lag. Usage: python demo_consumer.py [topic]"""
import sys, time
from confluent_kafka import Consumer

topic = sys.argv[1] if len(sys.argv) > 1 else "orders"
c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "demo-group",
              "auto.offset.reset": "earliest", "enable.auto.commit": True})
c.subscribe([topic])
print("Consuming slowly (1 msg/s) so lag builds up. Ctrl+C to stop.")
while True:
    m = c.poll(1.0)
    if m and not m.error():
        print(m.partition(), m.offset(), m.value()[:60])
        time.sleep(1)