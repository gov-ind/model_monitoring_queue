from datetime import datetime
from io import StringIO
from json import loads
import logging
import time

import pandas as pd

from common import batch_size, exchange_name, parse_args, wait_for_broker


def callback(start_time, end_time, ch, method, properties, body):
    payload = loads(body.decode())
    data = pd.read_json(StringIO(payload["data"]))
    data.time = pd.to_datetime(data.time)

    if start_time < data.iloc[0].time < end_time:
        logging.info(
            f"Received data from model: {payload['model_id']}, "
            f"batch: {data.index[0] // batch_size}, "
            f"time: {data.iloc[0].time}"
        )

        time.sleep(.2)
    else:
        logging.info("Out of date range. Skipping.")
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    model_id, start_time, end_time = parse_args(n=3)
    start_time = datetime.strptime(start_time, "%y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end_time, "%y-%m-%d %H:%M:%S")

    connection = wait_for_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=model_id + "_stream",
        on_message_callback=lambda *a, **kw: callback(start_time, end_time, *a, **kw),
        arguments={"x-stream-offset": "first"}
    )
    channel.start_consuming()
