import random
import time
from confluent_kafka import Producer
import avro.schema
import avro.io
import io

schema = avro.schema.parse(open("order.avsc", "r").read())

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-producer'
}

producer = Producer(conf)

def serialize(order):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(order, encoder)
    return bytes_writer.getvalue()

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}")

def main():
    print("Order Producer Started...")
    oid = 1000
    try:
        while True:
            products = {"I1": 999.50, "I2": 25.50, "I3": 75.50, "I4": 350.50, "I5": 150.50, "I6": 89.50}
            product = random.choice(list(products.keys()))
            order = {"orderId": str(oid), "product": product, "price": products[product]}

            print("Producing:", order)
            serialized = serialize(order)

            producer.produce("orders", key=str(oid), value=serialized, callback=delivery_report)
            producer.poll(0)
            
            oid += 1
            time.sleep(2)

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
