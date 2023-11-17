import pandas as pd
import time
from datetime import datetime
from kafka import KafkaProducer

# Read CSV file into a DataFrame
# Replace 'your_file.csv' with the actual file name
df = pd.read_csv('ecommerce.csv', low_memory=False)

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: str(v).encode('utf-8'))

# Iterate over rows and simulate real-time data flow
for index, row in df.iterrows():
    order_info = {
        'item_id': int(row['item_id']),
        'status': str(row['status']),
        'created_at': str(row['created_at']),
        'price': float(row['price']),
        'qty_ordered': int(row['qty_ordered']),
        'grand_total': float(row['grand_total']),
    }

    # Send data to Kafka topic
    producer.send('sales', value=order_info)

    # Simulate a delay to represent real-time processing
    time.sleep(10)
