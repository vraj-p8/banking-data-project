import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking.public.customers',
    'banking.public.accounts',
    'banking.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP", "minio-landing-group"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
buffer = {
    'banking.public.customers': [],
    'banking.public.accounts': [],
    'banking.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")
print(f"Available topics: {consumer.topics()}")
for message in consumer:
    # print(f"Received raw message from {message.topic}") # Check if loop is even entered
    event = message.value
    
    # Temporarily print the whole event to see the structure
    # print(f"Full event: {event}") 
    
    payload = event.get("payload", event)
    # If using Debezium, sometimes the data is top-level, or under 'after'
    record = payload.get("after") or payload.get("before")
    if record is None:
        print("No record found in message payload; skipping.")
        continue

    buffer.setdefault(message.topic, []).append(record)
    if len(buffer[message.topic]) >= batch_size:
        write_to_minio(message.topic.split('.')[-1], buffer[message.topic])
        buffer[message.topic] = []
