"""Consumer for one service.

Usage:
    python consumer.py payment
    python consumer.py inventory
    python consumer.py notification
Run it twice for the same service to show competing consumers.
"""
import json
import os
import sys
import time

from common import SERVICES, connect, setup

if len(sys.argv) < 2 or sys.argv[1] not in SERVICES:
    sys.exit(f"Usage: python consumer.py <{'|'.join(SERVICES)}>")

service = sys.argv[1]
queue = f"{service}.queue"
worker = f"{service}-{os.getpid()}"

ACTIONS = {
    "payment": "charged customer for",
    "inventory": "reserved stock for",
    "notification": "sent confirmation email for",
}


def on_message(channel, method, properties, body):
    order = json.loads(body)

    if order.get("fail"):
        print(f"[{worker}] REJECTED {order['order_id']} -> dead letter queue")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    time.sleep(2)  # simulate work
    print(f"[{worker}] {ACTIONS[service]} {order['order_id']}")
    channel.basic_ack(delivery_tag=method.delivery_tag)  # acknowledgement


connection = connect()
channel = connection.channel()
setup(channel)
channel.basic_qos(prefetch_count=1)  # fair sharing between consumers
channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)

print(f"[{worker}] waiting for messages on {queue}. Ctrl+C to stop.")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print(f"\n[{worker}] stopped.")
    connection.close()