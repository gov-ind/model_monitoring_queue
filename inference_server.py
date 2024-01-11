from datetime import datetime
import logging
import time
from json import dumps

import numpy as np
import pandas as pd

from common import batch_size, exchange_name, parse_args, wait_for_broker


class Drifter:
    def __init__(self, drift_type="constant"):
        self.drift_type = drift_type
        self.last_drift_time = time.time()
        self.drift_interval_start, self.drift_interval_end = 60, 120
        self.drift_interval_mid = (
            self.drift_interval_end + self.drift_interval_start
        ) / 2

    def add_noise(self, data):
        self.time_since_last_drift = time.time() - self.last_drift_time
        if self.time_since_last_drift > self.drift_interval_start:
            # Time to drift
            logging.info("Drifting")
            if drift_type == "constant":
                data += 5
            else:
                noise = self.drift_interval_mid - abs(
                    self.drift_interval_mid - self.time_since_last_drift
                )
                noise -= self.drift_interval_start
                data += noise
            if self.time_since_last_drift > self.drift_interval_end:
                self.last_drift_time = time.time()
        return data


class MLServer:
    def __init__(self, model_id, drifter):
        self.connection = wait_for_broker()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

        self.start_ix = 0
        self.model_id = model_id
        self.drifter = drifter

    def simulate_post(self):
        data = np.random.normal(1, 1, batch_size)
        self.drifter.add_noise(data)
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
    server = MLServer(
        model_id,
        drifter=Drifter(drift_type)
    )

    while True:
        server.simulate_post()
