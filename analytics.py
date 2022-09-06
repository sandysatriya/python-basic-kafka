import json
import logger
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed_1"
ORDER_CONFIRMED_KAFKA_TOPIC_GROUP = "order_confirmed_1_group"


consumer = KafkaConsumer(
    # ORDER_CONFIRMED_KAFKA_TOPIC, 
    bootstrap_servers="localhost:9092",
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    group_id=ORDER_CONFIRMED_KAFKA_TOPIC_GROUP
)

total_orders_count = 0
total_revenue = 0

# consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC])
consumer.assign([TopicPartition(ORDER_CONFIRMED_KAFKA_TOPIC, 0)])
print("Gonna start listening")
try:
    while True:
        msg = consumer.poll(timeout_ms=1.0)
        if msg is None:
            continue
        else:
            for message in consumer:
                print("Updating analytics..")
                consumed_message = json.loads(message.value.decode())
                total_cost = float(consumed_message["total_cost"])
                total_orders_count += 1
                total_revenue += total_cost
                print(f"Orders so far today: {total_orders_count}")
                print(f"Revenue so far today: {total_revenue}")
                consumer.commit()
except Exception as e:
        # logger.error(str(e))
        print("error {}".format(e))
finally:
        consumer.close()


