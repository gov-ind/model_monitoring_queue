import logging
import sys
import time

import pika

exchange_name = "model_logs_exchange"
batch_size = 100
logging.basicConfig(level=logging.INFO)


def parse_args():
    args = sys.argv[1:]
    n_args = len(args)
    assert n_args > 0, "A model ID must be passed."
    if n_args != 2:
        args.append("constant")
    return args


def wait_for_broker(host="rabbit"):
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host))
        except pika.exceptions.AMQPConnectionError:
            logging.info("Waiting for RabbitMQ broker to be ready...")
            time.sleep(1)
