echo "Create topics in Kafka.."

#Topic for data crawling imitation (raw_data)
docker exec -it lab1-kafka-kafka kafka-topics.sh --create --topic raw_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1

#Topic for data pre-processing (processed_data):
docker exec -it lab1-kafka-kafka kafka-topics.sh --create --topic processed_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1

#Topic for data pre-processing (processed_data):
docker exec -it lab1-kafka-kafka kafka-topics.sh --create --topic processed_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1

#Topic for classification results (ml_result):
docker exec -it lab1-kafka-kafka kafka-topics.sh --create --topic ml_result --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1

#Topic for visualization_data:
docker exec -it lab1-kafka-kafka kafka-topics.sh --create --topic visualization_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1
