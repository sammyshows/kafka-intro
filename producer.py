from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    message = f'message {i}'
    producer.send('test-topic', message.encode('utf-8'))
    print(f'Sent: {message}')

producer.flush()
