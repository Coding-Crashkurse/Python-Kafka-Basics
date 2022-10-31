from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

messages = ["meine message", "hallo", "hallo coders"]

def delivery_report(err, msg):
    if err is not None:
        print(f'Error in message: {err}')
    else:
        print(f'Message gesendet an to {msg.topic()} in partition {msg.partition()}')

for msg in messages:
    producer.poll(0)
    producer.produce('topic1', msg.encode('utf-8'), callback=delivery_report)

producer.flush()
