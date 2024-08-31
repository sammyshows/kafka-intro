from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'multi-partition-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')} from partition: {message.partition}")
