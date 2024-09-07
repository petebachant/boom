import time

import redis
from pymongo import UpdateOne

from config import load_config
from mongo import db_from_config
from ml_utils import ACAI_H_AlertClassifierBulk

ALLOWED_STREAMS = ["ZTF"]

PERMISSIONS_PER_STREAM = {
    "ZTF": [1,2,3]
}

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to the config file")
    parser.add_argument("--stream", type=str, default="ZTF", help="Stream to process")
    args = parser.parse_args()

    # Load the config
    config_file = args.config if args.config else "config.yaml"
    config = load_config([config_file])

    stream = args.stream
    if stream not in ALLOWED_STREAMS:
        raise ValueError(f"Stream {stream} not recognized by this worker. Must be one of {ALLOWED_STREAMS}")
    
    # Connect to MongoDB
    db = db_from_config(config)
    alerts_collection = db[f"{stream}_alerts"]

    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    redis_queue = f"{stream}_alerts_classifier_queue"

    # TODO:
    # - Load the models per stream, even if we only have models for the ZTF stream.
    # - Load the models specified in the config, and not a hardcoded one.
    # for now we just load ACAI_H (that has been retrained to support tensorflow 2.16)
    model_path = "data/models/acai_h_20240731_130724.keras"
    bulk_classifier = ACAI_H_AlertClassifierBulk(model_path)

    while True:
        start = time.time()

        # Retrieve 1000 candids from Redis
        candids = r.rpop(redis_queue, 1000)
        if candids is None:
            print("No values in Redis")
            time.sleep(2)
            continue
        candids = [int(candid) for candid in candids]
        print(f"Retrieved {len(candids)} values from Redis")


        # Query MongoDB for the corresponding alerts
        alerts = alerts_collection.find({"candid": {"$in": candids}})
        alerts = list(alerts)

        if not alerts:
            print("No alerts found")
            continue

        # handle missing alerts in the database
        missing_candids = set(candids) - {alert["candid"] for alert in alerts}
        if missing_candids:
            print(f"Missing alerts for candid(s): {missing_candids}")
            candids = list(set(candids) - missing_candids)
            # push the missing candids back to the left of the queue
            r.lpush(redis_queue, *missing_candids)

        # Run inference on the alerts
        probabilities = bulk_classifier.predict_bulk(alerts)

        # Write the results back to MongoDB
        bulk_updates = [
            UpdateOne(
                {"candid": alert["candid"]},
                {"$set": {
                    "classifications.acai_h": probability.numpy().item(),
                    "classifications.acai_h_version": "20240731_130724",
                }}
            )
            for alert, probability in zip(alerts, probabilities)
        ]
        result = alerts_collection.bulk_write(bulk_updates)

        # send candids to filter worker (1000 candids max / lpush)
        # group the candids by the alert.candidate.programid
        # this is so that filters can run on their corresponding programid
        candids_grouped = {
            alert["candidate"]["programid"]: [alert["candid"] for alert in alerts]
            for alert in alerts
        }
        # for programid, candids in candids_grouped.items():
        #     r.lpush(f"{stream}_alerts_programid_{str(programid)}_filter_queue", *candids)

        # instead of sending to a queue per programid, we send to sent it to streams per programid
        # that way, multiple workers can consume from the same stream
        # if a worker has access to a permission that is higher than the programid,
        # it should get it in its stream
        # e.g. if programid = 1, it will go in streams for permissions 1, 2, 3
        # but if programid = 3, it will only go in stream for permission 3
        for programid, candids in candids_grouped.items():
            for candid in candids:
                for permission in PERMISSIONS_PER_STREAM[stream]:
                    if programid <= permission:
                        r.xadd(f"{stream}_alerts_programid_{str(permission)}_filter_stream", {"candid": candid})

        print(f"Total query + inference + write time: {time.time() - start} seconds")
        time.sleep(1)
