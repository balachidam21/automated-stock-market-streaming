import os
import requests
import time
import json
from dotenv import load_dotenv
from kafka import KafkaProducer

def createKafkaProducer() -> KafkaProducer:
    """
        Creates Kafka Producer object which is used to send messages into Kafka topic.
    """
    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'],
                         api_version=(2,0,2))

def get_response(symbols: str) -> dict:
    url = "http://api.marketstack.com/v1/intraday/latest?"
    api_key = os.getenv('ACCESS_KEY', '')
    request_url = url + 'access_key=' + api_key + '&symbols=' + symbols
    response = requests.get(request_url)
    if response.status_code == 200:
        data = response.json()
        return data['data'][0]
    return None

def create_kafka_message(stock: dict) -> list[dict]:
    """
    Creates desired JSON message to be sent to Kafka topic with necessary values to be stored
    """
    kafka_data = {}
    kafka_data['key'] = f"{stock['symbol']}.{stock['date']}"
    kafka_data['symbol'] = stock['symbol']
    kafka_data['datetime'] = stock['date']
    kafka_data['open'] = stock['open']
    kafka_data['close'] = stock['close']
    kafka_data['current'] = stock['last']
    kafka_data['volume'] = stock['volume']
    return kafka_data

def start_streaming():
    """
    Retrieves stockmarket data from the API every hour and send to Kafka topic.
    """
    # load environment variables which stores API access key
    load_dotenv()

    producer = createKafkaProducer()
    response = get_response(symbols="AAPL")
    kafka_data = create_kafka_message(response)
    # send received api response as a kafka message to the Kafka Producer topic 'stock_market'
    producer.send("stock_market", json.dumps(kafka_data).encode('utf-8'))

if __name__ == "__main__":
    start_streaming()