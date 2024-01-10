from io import StringIO
from json import loads
import logging
import sys
import time

import pandas as pd

from common import batch_size, exchange_name, wait_for_broker

if __name__ == "__main__":
    connection = wait_for_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
    
    assert len(sys.argv) > 1, "A model name must be passed."
    model_name = sys.argv[1]
    
    try:    
        channel.queue_declare(
            queue=model_name,
            arguments={'x-max-length': 100}
        )
        channel.queue_bind(exchange=exchange_name, queue=model_name, routing_key=model_name)

        def callback(ch, method, properties, body):
            #data = pd.read_json(StringIO(body.decode()))
            payload = loads(body.decode())
            model_id = payload["model_id"]
            data = pd.read_json(StringIO(payload["data"]))
            logging.info(f"Received data from model: {model_id}, batch: {data.index[0] // batch_size}")
            time.sleep(3)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=model_name, on_message_callback=callback)

        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted')
        exit()