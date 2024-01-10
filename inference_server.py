from datetime import datetime
import logging
import random
import sys
import time
from json import dumps

import numpy as np
import pandas as pd

from common import batch_size, exchange_name, wait_for_broker

if __name__ == "__main__":
    connection = wait_for_broker()
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    n_args = len(sys.argv)
    assert n_args > 1, "A model ID must be passed."
    model_id = sys.argv[1]
    if n_args > 2:
        client_id = sys.argv[2]
    else:
        logging.info("No client ID was passed. Assuming that the model ID is the client ID.")
        client_id = model_id
 
    def make_inferences(start_ix):
        end_ix = start_ix + batch_size
        data = np.ones(batch_size) + np.random.normal(0, 1, batch_size)
        
        if random.randint(0, 10) == 0:
            data += 5
        
        data = pd.DataFrame({
            "feature_1": data,
            "pred": data > 1,
            "time": datetime.now()
        }, index=range(start_ix, end_ix))
        payload = dumps({
            "model_id": model_id,
            "client_id": client_id,
            "data": data.to_json(date_format="iso")
        })

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=model_id,
            body=payload,
        )
        time.sleep(2)
        
        return end_ix

    ix = 0
    try:
        while True:
            logging.info(f"Making inference for batch {ix // batch_size}")
            ix = make_inferences(ix)
    except:
        connection.close()
