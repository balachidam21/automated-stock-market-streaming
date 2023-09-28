from kafka import KafkaProducer




def start_streaming():
    """
    Retrieves the JSON data from the API every 30 seconds and send to Kafka topic.
    """
