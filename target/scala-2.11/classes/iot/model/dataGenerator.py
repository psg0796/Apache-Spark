from kafka import KafkaConsumer
consumer = KafkaConsumer('output5', group_id='iot', bootstrap_servers=['localhost:9092'], auto_offset_reset='latest', enable_auto_commit=False)
for message in consumer:
