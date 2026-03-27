import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from statsmodels.tsa.seasonal import STL


class DataTransformer(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        epsilon,
        remove_peak,
        test_stl,
        stl_period
    ):
        self.epsilon = epsilon
        self.remove_peak = remove_peak
        self.test_stl = test_stl
        self.stl_period = stl_period
        self.isfitted = False

    def fit(self, X):
        if self.remove_peak:
            q_min, q_max = np.percentile(X, [25, 75], axis=0)
            iqr = q_max - q_min
            self.iqr_min = q_min - 1.5 * iqr
            self.iqr_max = q_max + 1.5 * iqr
        self.isfitted = True
        return self

    def transform(self, X):
        X_ = X.copy()
        if self.test_stl:
            for col in range(X_.shape[1]):
                res = STL(X_[:, col], period=self.stl_period, robust=True).fit()
                X_[:, col] = res.trend + res.seasonal
        if not self.isfitted:
            self.fit(X_)
        if self.remove_peak:
            X_ = np.clip(X_, a_min=self.iqr_min, a_max=self.iqr_max)
        X_ = np.where(X_ < 0, self.epsilon, X_)
        return X_

    def inverse_transform(self, X):
        X_ = X.copy()
        return X_
