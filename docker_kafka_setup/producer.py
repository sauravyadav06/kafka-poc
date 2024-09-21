import json
from kafka import KafkaProducer
import time


# Kafka producer configuration
bootstrap_servers ='localhost:9092'
topic_name = 'warehouse_events'

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Define a function to send a message to the Kafka topic
def send_message(event_type, data):
    message = {
        'event_type': event_type,
        'data': data
    }
    producer.send(topic_name, value=json.dumps(message).encode('utf-8'))

# Define the data to be sent
shipment_data = {
    'shipment_id': 123,
    'items': [
        {'item_id': 1, 'quantity': 10},
        {'item_id': 2, 'quantity': 20}
    ]
}

# Set the number of data points to produce and the interval
num_data_points = 200
interval_seconds = 120  # 2 minutes

# Produce data points
for i in range(num_data_points):
    if(i < 20):
     send_message('shipment_shipped {}'.format(i), shipment_data)
     time.sleep(interval_seconds / num_data_points)  # distribute the interval across the data points

# Close the producer
producer.close()