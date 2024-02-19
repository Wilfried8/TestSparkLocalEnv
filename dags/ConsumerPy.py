from confluent_kafka import Consumer, KafkaError
import json

def consume_messages():
    conf = {
        'bootstrap.servers': 'localhost:9192',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'fetch.max.bytes': 50000000,  # Set fetch.max.bytes to a proper value
        'receive.message.max.bytes': 50000512
    }

    consumer = Consumer(conf)
    topics = ['users_created']

    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(msg.error())
                    break

            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8'))
            print("Received message:")
            print(f"Key: {key}, Value: {json.dumps(value, indent=4)}")  # Print the received message

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
