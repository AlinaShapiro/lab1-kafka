import json
import time
import random
import logging
import argparse
import pandas as pd
from confluent_kafka import Producer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    def produce_message(self, message_key, message_value):
        try:
            self.producer.produce(self.topic, key=message_key, value=message_value, callback=self.delivery_report)
            logger.info(f"Message sent successfully: key={message_key}, value={message_value}")
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")

    def flush(self):
        self.producer.flush()
        logger.info("All messages have been flushed.")

    def send_data_from_csv(self, csv_file, sleep_range=(0.05, 0.15)):
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Reading data from CSV file: {csv_file}")
            for _, row in df.iterrows():
                message = {'text': row['text'], 'sentiment': row['label']}
                message_value = json.dumps(message)
                message_key = str(time.time())
                self.produce_message(message_key, message_value)

                time.sleep(random.uniform(*sleep_range))
            logger.info("All messages have been sent successfully.")
        except Exception as e:
            logger.error(f"An error occurred while sending data from CSV: {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer for sending messages from CSV")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file to read data from")
    parser.add_argument('--bootstrap_servers', type=str, default='localhost:9095', help="Bootstrap servers for Kafka")
    parser.add_argument('--topic', type=str, default='raw_data', help="Kafka topic to send messages to")
    
    args = parser.parse_args()

    try:
        logger.info("Starting Kafka producer...")
        producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
        producer.send_data_from_csv(args.csv_file)
        producer.flush()
        logger.info("Kafka producer finished successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")