from confluent_kafka import Producer

# Setup producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Sample input text
text = "Kafka Streams is a client library for building applications and microservices"

# Produce each word as a separate message
for word in text.split():
    producer.produce('word-count-topic', word)
producer.flush()
