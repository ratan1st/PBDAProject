from kafka import KafkaConsumer, KafkaProducer
import time
import random

def publish_message(producer_instance, topic_name, value):
    try:
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, value_bytes)
        producer_instance.flush()
        print("PUBLISHED: " + value)
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

    kafka_producer = connect_kafka_producer()
    input_file_path = 'C:\\Users\\ratan\\Downloads\\spark-2.3.2-bin-hadoop2.7\\spark-2.3.2-bin-hadoop2.7\\bin\\access_log.txt'  

    f = open(input_file_path, "r")
    content = f.readlines()

    for line in content:
        time.sleep(random.randint(1,4))
        publish_message(kafka_producer, 'test', line)

    if kafka_producer is not None:
        kafka_producer.close()