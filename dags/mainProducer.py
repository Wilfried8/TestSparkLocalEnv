import json
import time

from faker import Faker
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka import deserializing_consumer

fake = Faker()

def generate_profile():
    user = fake.simple_profile()

    return {
        'userid': fake.uuid4(),
        'username': user['username'],
        'name': user['name'],
        'sex': user['sex'],
        'mail': user['mail']
    }


def get_data():
    import requests
    import json

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    #print(json.dumps(response, indent=2))

    return response


def format_data(response):
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    # commend to send data to postgres localConsumer
    data['location'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                       f"{location['city']}, {location['state']}, {location['country']}"

    data['post_code'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    # commend to send data to postgres localConsumer
    data['picture'] = response['picture']['medium']

    return data

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    topic = "users_created"
    producer = SerializingProducer({
        'bootstrap.servers':'localhost:9192'
    })

    # Time this line is execute
    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 90:
        try:
            #p = generate_profile()
            g = get_data()
            p = format_data(g)
            print(p)

            producer.produce(
                topic=topic,
                key=p['username'],
                value=json.dumps(p),
                on_delivery=delivery_report

            )
            producer.poll(0)

            time.sleep(2)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(2)
        except Exception as e:
            print(e)

if __name__=="__main__":
    main()