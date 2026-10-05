"""
Aggregator Service
==================
Your job: implement the AGGREGATOR pattern on top of the connection
handling and consume loop already wired up below.
"""

import json
import pika
import os
import threading
import time


# orderId -> {
#     "results": {itemIndex: resultDict, ...},
#     "totalItems": int,
#     "lastActivity": float,
# }
in_flight = {}
lock = threading.Lock()

IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()


def _build_message(order_id, total_items, results_dict, status):
    received_indexes = set(results_dict.keys())
    missing = sorted(set(range(total_items)) - received_indexes)
    return {
        "orderId": order_id,
        "correlationId": order_id,
        "status": status,
        "totalItems": total_items,
        "receivedItems": len(results_dict),
        "itemResults": list(results_dict.values()),
        "missingItemIndexes": missing,
    }


def aggregate_result(ch, method, properties, body):
    to_publish = None

    try:
        result = json.loads(body)
        order_id = result['orderId']
        item_index = result['itemIndex']
        total_items = result['totalItems']
    except (json.JSONDecodeError, KeyError) as e:
        print(f'[Aggregator] Bad message, dropping: {e}')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    with lock:
        entry = in_flight.setdefault(order_id, {
            "results": {},
            "totalItems": total_items,
            "lastActivity": time.time(),
        })

        entry["results"][item_index] = result
        entry["lastActivity"] = time.time()

        if len(entry["results"]) >= entry["totalItems"]:
            to_publish = _build_message(order_id, entry["totalItems"], entry["results"], "complete")
            del in_flight[order_id]

    if to_publish is not None:
        publish_completion(to_publish)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        now = time.time()
        timed_out = []

        with lock:
            for order_id, entry in list(in_flight.items()):
                if now - entry["lastActivity"] > IDLE_TIMEOUT_SECONDS:
                    timed_out.append(
                        _build_message(order_id, entry["totalItems"], entry["results"], "partial")
                    )
                    del in_flight[order_id]

        for message in timed_out:
            publish_completion(message)


def main():
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    channel.basic_qos(prefetch_count=1)

    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    print('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
