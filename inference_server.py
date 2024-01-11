from datetime import datetime
import logging
import time
from json import dumps

import numpy as np
import pandas as pd

from common import batch_size, exchange_name, parse_args, wait_for_broker


class MLServer:
    def __init__(self, model_id):
        self.connection = wait_for_broker()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

        self.start_ix = 0
        self.model_id = model_id

    def simulate_post(self):
        data = np.random.normal(1, 1, batch_size)
        df = pd.DataFrame({
            "feature_0": data,
            "pred": data > 1,
            "time": datetime.now()
        }, index=range(self.start_ix, self.start_ix + batch_size))
        payload = dumps({
            "model_id": self.model_id,
            "data": df.to_json(date_format="iso")
        })

        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=self.model_id,
            body=payload,
        )
        logging.info(f"Sent batch {self.start_ix // batch_size} to queue.")
        self.start_ix += batch_size

        time.sleep(2)


if __name__ == "__main__":
    model_id, drift_type = parse_args()
    server = MLServer(model_id)

    while True:
        server.simulate_post()
