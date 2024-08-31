from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send messages to different partitions
for i in range(10):
    partition = i % 3
    message = f'partition {partition} message {i}'
    future = producer.send('multi-partition-topic', key=b'key', value=message.encode('utf-8'), partition=partition)
    try:
        record_metadata = future.get(timeout=10)
        print(f'Sent {message} to partition {record_metadata.partition}')
    except KafkaError as e:
        print(f'Failed to send message {i}: {e}')

producer.flush()
