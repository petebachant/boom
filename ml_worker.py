import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from astropy.io import fits
import gzip
import io
from pymongo import MongoClient
import redis
import time
from pymongo import UpdateOne

tf.config.optimizer.set_jit(True)

ACAI_H_FEATURES = ('drb', 'diffmaglim', 'ra', 'dec', 'magpsf', 'sigmapsf', 'chipsf', 'fwhm', 'sky', 'chinr', 'sharpnr', 'sgscore1', 'distpsnr1', 'sgscore2', 'distpsnr2', 'sgscore3', 'distpsnr3', 'ndethist', 'ncovhist', 'scorr', 'nmtchps', 'clrcoeff', 'clrcounc', 'neargaia', 'neargaiabright')

class AlertClassifier:
    def __init__(self, model_path: str):
        # load model from .h5 file given path
        self.model = load_model(model_path)

    def make_triplet(self, alert, normalize: bool = True):
        """
        Feed in alert packet, return triplet of cutouts as numpy array for the model
        """
        triplet = np.zeros((63, 63, 3))

        for i, cutout in enumerate(("science", "template", "difference")):
            data = alert[f"cutout{cutout.capitalize()}"]["stampData"]

            # unzip
            with gzip.open(io.BytesIO(data), "rb") as f:
                with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                    data = hdu[0].data
                    # replace nans with zeros
                    data = np.nan_to_num(data)
                    # normalize
                    if normalize:
                        data /= np.linalg.norm(data)

            # pad to 63x63 if smaller
            shape = data.shape
            if shape != (63, 63):
                data = np.pad(
                    data,
                    [(0, 63 - shape[0]), (0, 63 - shape[1])],
                    mode="constant",
                    constant_values=1e-9,
                )

            triplet[:, :, i] = data

        return triplet
    
    def make_metadata(self, alert):
        raise NotImplementedError
    
    def predict(self, alert):
        raise NotImplementedError

    
class ACAI_H_AlertClassifier(AlertClassifier):
    def __init__(self, model_path: str):
        super().__init__(model_path)

    def make_metadata(self, alert):
        return np.array([alert['candidate'][field] for field in ACAI_H_FEATURES], dtype=np.float32)

    def predict(self, alert):
        # calling the model with the triplet and metadata returns a prediction of the form [[probability]]
        # return the probability as a float, not a list
        return self.model([
            np.expand_dims(self.make_triplet(alert), axis=[0, -1]),
            np.expand_dims(self.make_metadata(alert), axis=[0, -1]),
        ])[0][0]
    
class ACAI_H_AlertClassifierBulk(ACAI_H_AlertClassifier):
    def predict_bulk(self, alerts):
        triplets = np.zeros((len(alerts), 63, 63, 3))
        metadata = np.zeros((len(alerts), len(ACAI_H_FEATURES)), dtype=np.float32)
        
        for i, alert in enumerate(alerts):
            triplets[i] = self.make_triplet(alert)
            metadata[i] = self.make_metadata(alert)
        
        return self.model([triplets, metadata])


# Load the model
model_path = "data/models/acai_h_20240731_130724.keras"
bulk_classifier = ACAI_H_AlertClassifierBulk(model_path)

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client.zvar
alerts_collection = db.alerts

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Process 1000 alerts at a time, running inference in batches
while True:
    start = time.time()

    # Retrieve 1000 candids from Redis
    candids = r.rpop('alertclassifierqueue', 1000)
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
        r.lpush('alertclassifierqueue', *missing_candids)

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
    print(f"Total query + inference + write time: {time.time() - start} seconds")
    time.sleep(1)




