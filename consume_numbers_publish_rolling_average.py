
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
import numpy as np


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message', value_bytes, 'published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def publish_rolling_average(consumer, producer, n):
    que = deque([0 for _  in range(n)])
    for message in consumer:
        que.popleft()
        que.append(float(message.value))
        rolling_average = np.mean(que)
        publish_message(producer, 'rolling_average', 'rolling_average', str(rolling_average))




if __name__ == '__main__':

    # Notify if a recipe has more than 200 calories
    numbers_consumer = KafkaConsumer('numbers',
                             auto_offset_reset='latest',
                             bootstrap_servers=['localhost:9092'],
                             api_version=(0, 10),
                             consumer_timeout_ms=30000)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))

    publish_rolling_average(numbers_consumer, producer, 5)

    if numbers_consumer is not None:
        numbers_consumer.close()