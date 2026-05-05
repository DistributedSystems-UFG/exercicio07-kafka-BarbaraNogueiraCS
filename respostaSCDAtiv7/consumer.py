from kafka import KafkaConsumer
from const import BROKER_ADDR, BROKER_PORT
import sys


def main():
    if len(sys.argv) != 2:
        print('Usage: python3 consumer.py <topic_name>')
        exit(1)

    topic = sys.argv[1]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode('utf-8')
    )

    print('Consumer waiting for messages from topic: ' + topic)

    try:
        for msg in consumer:
            print('Received from ' + msg.topic + ': ' + msg.value)
    except KeyboardInterrupt:
        print('\nConsumer interrupted by user.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
