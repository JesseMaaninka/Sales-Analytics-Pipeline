import redis
import json
import random
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

salesmen = ["Jenny", "Mikael", "Sofia", "Juhani", "Emma"]
products = [
    {"Product": "Keyboard",   "UnitPrice": 60},
    {"Product": "Mouse",      "UnitPrice": 25},
    {"Product": "Monitor",    "UnitPrice": 300},
    {"Product": "Headphones", "UnitPrice": 120},
    {"Product": "Webcam",     "UnitPrice": 80},
    {"Product": "Laptop",     "UnitPrice": 999},
    {"Product": "USB Hub",    "UnitPrice": 35},
]

# Clear old data
r.delete("sales_stream")
print("=== Kafka Producer: Pushing 200 sales orders to Redis ===\n")

for i in range(200):
    salesman = random.choice(salesmen)
    product  = random.choice(products)
    quantity = random.randint(1, 20)
    age      = random.randint(22, 60)

    sale = {
        "order_id":   i + 1,
        "SalesMan":   salesman,
        "Product":    product["Product"],
        "UnitPrice":  product["UnitPrice"],
        "quantity":   quantity,
        "revenue":    round(quantity * product["UnitPrice"], 2),
        "Age":        age,
        "city":       random.choice(["Helsinki", "Espoo", "Tampere", "Turku", "Oulu"]),
    }

    # Push to Redis list (simulating Kafka topic)
    r.rpush("sales_stream", json.dumps(sale))

print(f"  Total messages in Redis: {r.llen('sales_stream')}")
print("\nProducer done. Run spark_etl.py next.")