import logging
import time

import pika

exchange_name = "model_logs_exchange"
batch_size = 100
logging.basicConfig(level=logging.INFO)

def wait_for_broker(host="rabbit"):
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host))
        except pika.exceptions.AMQPConnectionError:
            logging.info("Waiting for RabbitMQ broker to be ready...")
            time.sleep(1)
