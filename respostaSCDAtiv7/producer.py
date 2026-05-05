from kafka import KafkaProducer
from const import BROKER_ADDR, BROKER_PORT
import sys


def main():
    if len(sys.argv) != 2:
        print('Usage: python3 producer.py <topic_name>')
        exit(1)

    topic = sys.argv[1]

    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda value: value.encode('utf-8')
    )

    for i in range(100):
        msg = 'My ' + str(i) + 'st message for topic ' + topic
        print('Sending message: ' + msg)
        producer.send(topic, value=msg)

    producer.flush()
    producer.close()
    print('All messages were sent to topic: ' + topic)


if __name__ == '__main__':
    main()
