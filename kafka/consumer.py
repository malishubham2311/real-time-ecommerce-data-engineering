import json

from kafka import KafkaConsumer


# Kafka Configuration
KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ecommerce_transactions"


# Initialize Consumer
consumer = KafkaConsumer(

    TOPIC_NAME,

    bootstrap_servers=KAFKA_SERVER,

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    group_id="ecommerce_consumer_group",

    value_deserializer=lambda x: json.loads(x.decode("utf-8"))

)


print("Listening for E-Commerce Transactions...")


try:

    for message in consumer:

        transaction = message.value

        print("\nNew Transaction Received")

        print("-----------------------------")

        print("Transaction ID:", transaction["transaction_id"])

        print("Customer ID:", transaction["customer_id"])

        print("Product ID:", transaction["product_id"])

        print("Quantity:", transaction["quantity"])

        print("Price:", transaction["price"])

        print("Payment:", transaction["payment_method"])

        print("Timestamp:", transaction["timestamp"])

        print("Partition:", message.partition)

        print("-----------------------------")


except KeyboardInterrupt:

    print("\nStopping Consumer...")


finally:

    consumer.close()