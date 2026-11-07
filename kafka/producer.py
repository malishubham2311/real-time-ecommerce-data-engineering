import json
import time
import random
import uuid

from datetime import datetime
from kafka import KafkaProducer


# Kafka Configuration
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "ecommerce_transactions"


# Initialize Kafka Producer
producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

    key_serializer=lambda k: k.encode("utf-8"),

    acks="all",

    retries=5

)


# Sample Data
payment_methods = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "PayPal"
]

product_ids = [
    "PROD101",
    "PROD102",
    "PROD103",
    "PROD104",
    "PROD105"
]


print("Starting E-Commerce Transaction Producer...")


try:

    while True:

        transaction = {

            "transaction_id": str(uuid.uuid4()),

            "customer_id": f"CUST{random.randint(100, 999)}",

            "product_id": random.choice(product_ids),

            "quantity": random.randint(1, 5),

            "price": round(random.uniform(10, 500), 2),

            "payment_method": random.choice(payment_methods),

            "timestamp": datetime.utcnow().isoformat()

        }


        # Send transaction to Kafka

        producer.send(

            TOPIC_NAME,

            key=transaction["transaction_id"],

            value=transaction

        )


        producer.flush()


        print("Transaction sent:", transaction)


        time.sleep(2)


except KeyboardInterrupt:

    print("\nStopping Producer...")


finally:

    producer.close()