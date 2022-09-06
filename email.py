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

# consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC])
consumer.assign([TopicPartition(ORDER_CONFIRMED_KAFKA_TOPIC, 1)])
emails_sent_so_far = set()
print("Gonna start listening")
try:
    while True:
        msg = consumer.poll(timeout_ms=1.0)
        if msg is None:
            continue
        else:
            for message in consumer:
                consumed_message = json.loads(message.value.decode())
                customer_email = consumed_message["customer_email"]
                print(f"Sending email to {customer_email} ")
                emails_sent_so_far.add(customer_email)
                print(f"So far emails sent to {len(emails_sent_so_far)} unique emails")
                consumer.commit()
except Exception as e:
        # logger.error(str(e))
         print("error {}".format(e))
finally:
        consumer.close()
