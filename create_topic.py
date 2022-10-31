from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

new_topic = NewTopic("topic1", num_partitions=3, replication_factor=1)
new_topic2 = NewTopic("topic2", num_partitions=3, replication_factor=1)

fs = admin.create_topics([new_topic, new_topic2])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} wurde erstellt")
    except Exception as e:
        print(f"Error: Topic {topic} konnte nicht erstellt werden: {e}")