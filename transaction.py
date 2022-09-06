import imp
import json

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.structs import TopicPartition
import logger


ORDER_KAFKA_TOPIC = "order_details_1"
ORDER_KAFKA_GROUP = "order_details_1_group"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed_1"

consumer = KafkaConsumer(
    # ORDER_KAFKA_TOPIC, 
    bootstrap_servers="localhost:9092",
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    group_id=ORDER_KAFKA_GROUP
)

producer = KafkaProducer(bootstrap_servers="localhost:9092", retries=5)

# consumer.subscribe([ORDER_KAFKA_TOPIC])
# Read the specified partition
consumer.assign([TopicPartition(ORDER_KAFKA_TOPIC, 1)])
try:
    print("Gonna start listening")
    while True:
        msg = consumer.poll(timeout_ms=1.0)
        if msg is None:
            continue
        else:
            for message in consumer:
                print("Ongoing transaction..")
                consumed_message = json.loads(message.value.decode())
                print(consumed_message)
                user_id = consumed_message["user_id"]
                total_cost = consumed_message["total_cost"]
                data = {
                    "customer_id": user_id,
                    "customer_email": f"{user_id}@gmail.com",
                    "total_cost": total_cost
                }
                print("Successful transaction..")
                consumer.commit()
                producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"), partition=0)
                producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"), partition=1)
except Exception as e:
        # logger.error(str(e))
         print("error {}".format(e))
finally:
        consumer.close()
