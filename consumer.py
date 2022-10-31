from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['topic1'])

while True:
    msg = consumer.poll(1.0)

    print(msg)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    print(f'Message erhalten: {msg.value().decode("utf-8")}')
consumer.close()