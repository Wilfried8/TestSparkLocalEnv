from confluent_kafka import DeserializingConsumer
from confluent_kafka import Consumer, KafkaError
import json
import psycopg2


def consume_messages():
    conf = {
        'bootstrap.servers': 'localhost:9192',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'fetch.max.bytes': 50000000,  # Set fetch.max.bytes to a proper value
        'receive.message.max.bytes': 50000512
    }
    postgres_conf = {
        'database': 'usersKafka',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432'
    }

    consumer = DeserializingConsumer(conf)
    topics = ['users_created']  # Replace with your Kafka topic

    consumer.subscribe(topics)

    try:
        # conn = psycopg2.connect(postgres_conf)
        print("connection !!!!!!!!!!!!!!")
        conn = psycopg2.connect(
            dbname=postgres_conf['database'],
            user=postgres_conf['user'],
            password=postgres_conf['password'],
            host=postgres_conf['host'],
            port=postgres_conf['port']
        )
        cursor = conn.cursor()
        print("connection !!!!!!!!!!")

        #cursor.execute("select * from equipes")
        #records = cursor.fetchall()

        #print(records)
        while True:
            msg = consumer.poll(timeout=10)

            if msg is None:
                print("messge none")
                continue
            if msg.error():
                print("messge error")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(msg.error())
                    break


            print(f"Received message: {msg.value().decode('utf-8')}")

            data = json.loads(msg.value())
            insert_query = """
                            INSERT INTO users (first_name, last_name, gender, post_code, email, username, 
                                                    dob, registered_date, phone)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
            cursor.execute(insert_query, (
                data['first_name'],
                data['last_name'],
                data['gender'],
                data['post_code'],
                data['email'],
                data['username'],
                data['dob'],
                data['registered_date'],
                data['phone']
            ))
            conn.commit()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_messages()
