from kafka import KafkaConsumer, KafkaProducer
import random


def return_a_number():
    return random.random()


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':
    all_numbers = [return_a_number() for _ in range(1000)]
    if len(all_numbers) > 0:
        kafka_producer = connect_kafka_producer()
        for num in all_numbers:
            publish_message(producer_instance=kafka_producer,
                            topic_name='numbers',
                            key='the_number',
                            value=str(num))
        if kafka_producer is not None:
            kafka_producer.close()
