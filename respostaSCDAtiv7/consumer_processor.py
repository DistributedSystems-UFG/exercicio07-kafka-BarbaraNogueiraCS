from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT
import sys


def process_event(original_event):
    """
    This function represents the processing step required by the activity.
    It receives one event from topic1 and creates a new event for topic2.
    """
    return 'Processed event: ' + original_event.upper()


def main():
    if len(sys.argv) != 3:
        print('Usage: python3 consumer_processor.py <input_topic> <output_topic>')
        print('Example: python3 consumer_processor.py topic1 topic2')
        exit(1)

    input_topic = sys.argv[1]
    output_topic = sys.argv[2]

    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode('utf-8')
    )

    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda value: value.encode('utf-8')
    )

    print('Processor running...')
    print('Consuming from topic: ' + input_topic)
    print('Publishing processed events to topic: ' + output_topic)

    try:
        for msg in consumer:
            original_event = msg.value
            processed_event = process_event(original_event)

            print('Original event:  ' + original_event)
            print('Processed event: ' + processed_event)

            producer.send(output_topic, value=processed_event)
            producer.flush()
    except KeyboardInterrupt:
        print('\nProcessor interrupted by user.')
    finally:
        consumer.close()
        producer.close()


if __name__ == '__main__':
    main()
