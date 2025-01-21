import gzip
import io

import numpy as np
import tensorflow as tf
from astropy.io import fits
from tensorflow.keras.models import load_model

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