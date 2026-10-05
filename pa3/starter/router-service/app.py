"""
Router Service
==============
Your job: implement two EIP patterns on top of the connection handling and
consume loop already wired up below.
"""

import json
import pika
import os


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


ROUTES = {
    'physical': 'orders.physical',
    'digital': 'orders.digital',
    'subscription': 'orders.subscription',
}


def route_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['orderId']
    correlation_id = order['correlationId']

    print(f"[Router] Processing order {order_id}")

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.physical', durable=True)
    channel.queue_declare(queue='orders.digital', durable=True)
    channel.queue_declare(queue='orders.subscription', durable=True)

    items = order.get('items', [])
    item_count = len(items)

    for item_index, item in enumerate(items):
        message = {
            "orderId": order_id,
            "correlationId": correlation_id,
            "itemIndex": item_index,
            "totalItems": item_count,
            "item": item,
        }

        item_type = item.get('type')
        routing_key = ROUTES.get(item_type)

        if routing_key is None:
            print(f"[Router] WARNING: unknown item type '{item_type}' "
                  f"for order {order_id} item {item_index}; "
                  f"falling back to orders.physical")
            routing_key = 'orders.physical'

        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    connection.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"[Router] Order {order_id} split into {item_count} items and routed")


def main():
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.incoming', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='orders.incoming', on_message_callback=route_order)

    print('[Router] Waiting for orders...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
