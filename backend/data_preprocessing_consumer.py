import re
import json
import logging
from nltk.corpus import stopwords

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_text(text):
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)
    text = re.sub(r'\d+', '', text)
    text = text.lower()
    stop_words = set(stopwords.words('english'))
    text = ' '.join(word for word in text.split() if word not in stop_words)
    return text

def main():
    consumer_config = {
        'bootstrap.servers': 'localhost:9095',
        'group.id': 'text-processing-group',
        'auto.offset.reset': 'earliest'
    }

    producer_config = {
        'bootstrap.servers': 'localhost:9095', 
        'client.id': 'text_processor'
    }

    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    consumer.subscribe(['raw_data'])
    processed_topic = 'processed_data'

    try:
        logger.info("Starting Kafka Text Processing Consumer...")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                    continue
                else:
                    raise KafkaException(msg.error())
            try:
                data = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message: {data}")

                if 'text' not in data:
                    logger.warning("No 'text' field found in the message")
                    continue
                cleaned_text = clean_text(data['text'])
                processed_data = {
                    'cleaned_text': cleaned_text,
                    'sentiment': data.get('sentiment', None)  
                }

                producer.produce(processed_topic, key=msg.key(), value=json.dumps(processed_data))
                producer.flush()
                logger.info(f"Processed message sent to {processed_topic} on second broker: {processed_data}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()