from confluent_kafka import Consumer, KafkaError
from collections import Counter

# Setup consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'wordcount-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['word-count-topic'])

# Count the words
word_counts = Counter()

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            break
        else:
            print(msg.error())
            break

    word = msg.value().decode('utf-8')
    word_counts[word] += 1
    print(f"Word count: {word_counts}")

consumer.close()

