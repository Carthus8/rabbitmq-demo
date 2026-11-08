import pika

EXCHANGE = "orders"
DLX = "orders.dlx"
DLQ = "orders.dead"
SERVICES = ["payment", "inventory", "notification"]


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters("localhost"))


def setup(channel):
    """Declare everything. Safe to call from the producer and every consumer."""
    # Main exchange: fanout copies each message to every bound queue
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)

    # Dead letter exchange and queue for rejected messages
    channel.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX)

    # One durable queue per service, bound to the exchange
    for service in SERVICES:
        queue = f"{service}.queue"
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX},
        )
        channel.queue_bind(queue=queue, exchange=EXCHANGE)