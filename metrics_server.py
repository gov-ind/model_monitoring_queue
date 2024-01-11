from io import StringIO
from json import loads
import logging
import time

import pandas as pd

from common import batch_size, exchange_name, parse_args, wait_for_broker


def callback(ch, method, properties, body):
    payload = loads(body.decode())
    data = pd.read_json(StringIO(payload["data"]))
    data.time = pd.to_datetime(data.time)
    logging.info(f"Received data from model: {payload['model_id']}, "
                 f"batch: {data.index[0] // batch_size}, "
                 f"time: {data.iloc[0].time}")

    time.sleep(6)
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    model_id, _ = parse_args()

    connection = wait_for_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    channel.queue_declare(
        queue=model_id,
        arguments={"x-max-length": 10}
    )
    channel.queue_bind(
        exchange=exchange_name,
        queue=model_id,
        routing_key=model_id
    )

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=model_id,
        on_message_callback=callback
    )
    channel.start_consuming()
