"""Order service (producer).

Usage:
    python producer.py            # sends 3 orders
    python producer.py 5          # sends 5 orders
    python producer.py 1 --bad    # sends 1 order that consumers will reject
"""
import json
import sys
import time

import pika

from common import EXCHANGE, connect, setup

count = 3
bad = "--bad" in sys.argv
for arg in sys.argv[1:]:
    if arg.isdigit():
        count = int(arg)

connection = connect()
channel = connection.channel()
setup(channel)

base = int(time.time()) % 100000
for i in range(1, count + 1):
    order = {
        "event": "OrderPlaced",
        "order_id": f"ORD-{base}-{i}",
        "item": "adobo",
        "qty": 1,
        "fail": bad,
    }
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="",  # ignored by a fanout exchange
        body=json.dumps(order),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent message
    )
    print(f"[Order Service] published {order['order_id']}")

connection.close()
print("Done. The producer did not wait for any consumer.")