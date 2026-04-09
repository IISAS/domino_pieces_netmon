import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.seasonal import STL
from tensorflow import keras


class RelativeTimeSeriesPipeline(BaseEstimator, RegressorMixin):

    def __init__(
        self,
        seq_len_in=24,
        seq_len_out=1,
        forecast_steps=1,
        roll_windows=(),
        gru_units=60,
        dropout_rate=0.6,
        learning_rate=1e-3,
        clipnorm=1.0,
        epochs=100,
        diff=False,
        batch_size=1,
        stl_epsilon=1,
        stl_remove_peak=False,
        stl_test=False,
        stl_period=None,
        random_state: int = None,
    ):
        self.seq_len_in = seq_len_in
        self.seq_len_out = seq_len_out
        self.forecast_steps = forecast_steps
        self.roll_windows = roll_windows
        self.gru_units = gru_units
        self.dropout_rate = dropout_rate
        self.learning_rate = learning_rate
        self.clipnorm = clipnorm
        self.epochs = epochs
        self.batch_size = batch_size
        self.diff = diff
        # STL
        self.stl_epsilon = stl_epsilon
        self.stl_remove_peak = stl_remove_peak
        self.stl_test = stl_test
        self.stl_period = self.seq_len_in if stl_period is None else stl_period
        self.stl_isfitted = False
        # model
        self.loss = keras.losses.MeanSquaredError()
        self.metrics = [
            self.loss,
            keras.metrics.MeanAbsoluteError()
        ]

        self.random_state = random_state

    # Difference transformer
    def _difference(self, X: np.ndarray) -> np.ndarray:
        return X[1:] - X[:-1]

    def _stl_fit(self, X: np.ndarray):
        if self.stl_remove_peak:
            q_min, q_max = np.percentile(X, [25, 75], axis=0)
            iqr = q_max - q_min
            self.stl_iqr_min = q_min - 1.5 * iqr
            self.stl_iqr_max = q_max + 1.5 * iqr
        self.stl_isfitted = True
        return self

    def _stl_transform(self, X: np.ndarray):
        if self.stl_test:
            for col in range(X.shape[1]):
                res = STL(X[:, col], period=self.stl_period, robust=True).fit()
                X[:, col] = res.trend + res.seasonal
        if not self.stl_isfitted:
            self._stl_fit(X)
        if self.stl_remove_peak:
            X = np.clip(X, a_min=self.stl_iqr_min, a_max=self.stl_iqr_max)
        X = np.where(X < 0, self.stl_epsilon, X)
        return X

    # Feature engineering
    def _feature_engineer(self, X: np.ndarray) -> np.ndarray:
        n, m = X.shape
        feats = [X]

        for w in self.roll_windows:
            if n < w or w <= 0:
                feats.extend([np.full((n, m), np.nan) for _ in range(4)])
                continue
            win = np.lib.stride_tricks.sliding_window_view(X, w, axis=0)
            mean = win.mean(axis=2)
            std = win.std(axis=2)
            min_ = win.min(axis=2)
            max_ = win.max(axis=2)
            for stat in (mean, std, min_, max_):
                f = np.full((n, m), np.nan)
                f[w - 1:] = stat
                feats.append(f)

        return np.hstack(feats)

    def _transform(self, X: np.ndarray):
        """Feature engineering / scaling applied to X before fit and predict."""

        y_dim = X.shape[1]

        # transform to differential data
        if self.diff:
            X = self._difference(X)

        # scaling
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        X = self.scaler.fit_transform(X)

        # seasonal-trend decomposition using Loess
        X = self._stl_transform(X)

        # Feature engineering
        X = self._feature_engineer(X)
        X = SimpleImputer(strategy='mean').fit_transform(X)

        y = X[self.forecast_steps:, :y_dim]
        X = X[:-self.forecast_steps, :]

        # Time-series generator
        tsg = keras.preprocessing.sequence.TimeseriesGenerator(
            X, y,
            **{
                "length": self.seq_len_in,
                "sampling_rate": 1,
                "stride": 1,
                "batch_size": self.batch_size
            },
        )

        return tsg

    def _build_model(
        self,
        input_shape: tuple,
        output_dim: int,
    ) -> keras.Model:
        """
        Build and return a compiled keras.Model.
        """
        inputs = keras.Input(shape=input_shape)
        h = keras.layers.GRU(self.gru_units, return_sequences=True)(inputs)
        h = keras.layers.Dropout(self.dropout_rate)(h)
        h = keras.layers.GRU(self.gru_units, return_sequences=False)(h)
        h = keras.layers.Dropout(self.dropout_rate)(h)
        outputs = keras.layers.Dense(output_dim, activation='linear')(h)

        model = keras.Model(inputs=inputs, outputs=outputs)
        optimizer = keras.optimizers.Adam(
            learning_rate=self.learning_rate,
            clipnorm=self.clipnorm
        )
        model.compile(
            optimizer=optimizer,
            loss=self.loss,
            metrics=self.metrics
        )
        return model

    # --- fit ---
    def fit(self, X: np.ndarray, callbacks: list = None):

        if self.random_state is not None:
            np.random.seed(self.random_state)

        X = np.asarray(X, dtype=float)

        tsg = self._transform(X)

        # tsg[0] is the first batch: shape (batch_size, seq_len_in, n_features)
        sample_X, sample_y = tsg[0]
        input_shape = sample_X.shape[1:]  # (seq_len_in, n_features)
        output_dim = sample_y.shape[-1]  # n_target_vars

        # build model
        self.model_ = self._build_model(
            input_shape=input_shape,
            output_dim=output_dim
        )

        # train the model
        self.history = self.model_.fit(
            tsg,
            epochs=self.epochs,
            verbose=1,
            callbacks=callbacks,
        )

        return self
