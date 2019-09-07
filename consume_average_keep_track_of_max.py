from kafka import KafkaConsumer, KafkaProducer



def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message', key_bytes, value_bytes, 'published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def print_and_publish_if_new_max_reached(consumer, producer):
    best_max = -9999
    for message in consumer:
        number = float(message.value)
        if number > best_max:
            best_max = number
            publish_message(producer, 'highest_average_so_far', 'highest_average_so_far', str(best_max))



if __name__ == '__main__':

    # Notify if a recipe has more than 200 calories
    rolling_average_consumer = KafkaConsumer('rolling_average',
                             auto_offset_reset='latest',
                             bootstrap_servers=['localhost:9092'],
                             api_version=(0, 10),
                             consumer_timeout_ms=30000)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))



    print_and_publish_if_new_max_reached(rolling_average_consumer, producer)

