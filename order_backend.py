from asyncio.log import logger
import json
import time
import logger
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details_1"
ORDER_LIMIT = 10

producer = KafkaProducer(bootstrap_servers="localhost:9092", retries=5)

print("Going to be generating order after 5 seconds")
print("Will generate one unique order every 5 seconds")
time.sleep(5)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    logger.error('I am an errback', exc_info=excp)

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": "ID {}".format(i),
        "user_id": f"tom_{i}",
        "total_cost": i * 5,
        "items": "burger,sandwich",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"), partition=1).add_callback(on_send_success).add_errback(on_send_error)

    # block until all async messages are sent
    producer.flush()
    print(f"Done Sending..{i}")
    # time.sleep(10)
