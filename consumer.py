import json
import random
from confluent_kafka import Consumer, Producer, KafkaException
import avro.schema
import avro.io
import io


schema = avro.schema.parse(open("order.avsc", "r").read())


consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
}


producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe(["orders", "orders-retry"])

price_sum = 0.0
order_count = 0
retry_counts = {}
MAX_RETRIES = 3

def deserialize(binary_data):
    bytes_reader = io.BytesIO(binary_data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def process_order(order):
    if random.random() < 0.2:
        raise Exception("Temporary processing error")

    if random.random() < 0.05:
        raise ValueError("Permanent error: Invalid order data")

    print(f"✓ Processed: Order {order['orderId']} → {order['product']} → ${order['price']}")
    global price_sum, order_count
    price_sum += order['price']
    order_count += 1
    print(f"Running Average: ${price_sum/order_count:.2f}")
    return True

def send_retry(msg):
    producer.produce("orders-retry", key=msg.key(), value=msg.value())
    producer.flush()

def send_dlq(msg, err):
    print(f"✗ Sending to DLQ — {err}")
    producer.produce("orders-dlq", key=msg.key(), value=msg.value())
    producer.flush()

def main():
    print("Order Consumer Started...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg:
                continue
            
            if msg.error():
                raise KafkaException(msg.error())

            order = deserialize(msg.value())
            oid = order["orderId"]

            if oid not in retry_counts:
                retry_counts[oid] = 0

            try:
                process_order(order)
                del retry_counts[oid]

            except ValueError as e:
                send_dlq(msg, str(e))

            except Exception as e:
                retry_counts[oid] += 1
                if retry_counts[oid] <= MAX_RETRIES:
                    print(f"⚠ Retrying ({retry_counts[oid]}/{MAX_RETRIES})")
                    send_retry(msg)
                else:
                    send_dlq(msg, "Max retries exceeded")
                    del retry_counts[oid]

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
