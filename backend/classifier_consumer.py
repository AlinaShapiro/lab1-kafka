import json
import logging
from transformers import pipeline
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LABEL2NAME = {'LABEL_0': 'Negative', 'LABEL_1': 'Positive'}
ID2NAME = {0: 'Negative', 1: 'Positive'}

class TweetClassifier:
    def __init__(self, model_name):
        self.classifier = pipeline("sentiment-analysis", model=model_name)

    def classify(self, tweet_text):
        result = self.classifier(tweet_text)
        return result[0]['label'], result[0]['score']

class TweetConsumerProducer:
    def __init__(self, bootstrap_servers, input_topic, output_topic):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'classifier_group',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.classifier = TweetClassifier("pig4431/Sentiment140_roBERTa_5E")

    def process_tweets(self):
        self.consumer.subscribe([self.input_topic])

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                    else:
                        logger.error(f"Error: {msg.error()}")
                    continue

                try:
                    tweet = json.loads(msg.value().decode('utf-8'))
                    text = tweet['cleaned_text']
                    label = tweet['sentiment']
                    sentiment, confidence = self.classifier.classify(text)

                    result = {
                        'tweet': text,
                        'sentiment': LABEL2NAME[sentiment],
                        'label': ID2NAME[label],
                        'confidence': confidence
                    }
                    self.producer.produce(self.output_topic, json.dumps(result).encode('utf-8'))
                    self.producer.flush()
                    logger.info(f"Processed: {result}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

if __name__ == '__main__':
    bootstrap_servers = 'localhost:9095'
    input_topic = 'processed_data'
    output_topic = 'ml_result'

    processor = TweetConsumerProducer(bootstrap_servers, input_topic, output_topic)
    processor.process_tweets()