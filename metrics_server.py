from io import StringIO
from json import loads
import logging
import sys
import time
import threading

import pandas as pd
from prometheus_client import Gauge, start_http_server

from common import batch_size, exchange_name, wait_for_broker

metrics = {}

def start_prometheus(port=8000):
    start_http_server(port)
    
def mean(data, client_id):
    for col_name in data.columns:
        if col_name == "time":
            continue
        if col_name not in metrics:
            metrics[col_name] = Gauge(
                f"mean_{col_name}",
                f"Mean of column {col_name}",
                labelnames=[
                    "client_id",
                ]
            )
        metrics[col_name].labels(
            client_id=client_id
        ).set(data[col_name].mean())

metric_fns = [mean]

if __name__ == "__main__":
    threading.Thread(target=start_prometheus).start()
    
    connection = wait_for_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
    
    n_args = len(sys.argv)
    assert n_args > 1, "A model ID must be passed."
    model_id = sys.argv[1]
    
    try:    
        channel.queue_declare(
            queue=model_id,
            arguments={'x-max-length': 100}
        )
        channel.queue_bind(exchange=exchange_name, queue=model_id, routing_key=model_id)

        def callback(ch, method, properties, body):
            payload = loads(body.decode())
            client_id = payload['client_id']
            data = pd.read_json(StringIO(payload["data"]))
            data.time = pd.to_datetime(data.time)
            logging.info(f"Received data from model: {payload['model_id']}, client: {client_id}, batch: {data.index[0] // batch_size}, time: {data.iloc[0].time}")
            
            for calculate_metric in metric_fns:
                calculate_metric(data, client_id)
            
            time.sleep(6)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=model_id, on_message_callback=callback)

        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interrupted")
        exit()