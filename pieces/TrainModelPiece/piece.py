import asyncio
import datetime
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
from domino.base_piece import BasePiece
from tensorflow import keras

from . import data_utils
from . import utils
from .models import InputModel, OutputModel
from .time_series_pipeline import RelativeTimeSeriesPipeline


class TrainModelPiece(BasePiece):

    def get_results_path(self):
        if self.__class__.__name__ == "DryPiece":
            return Path(self.results_path) / self._metadata_['name']
        return Path(self.results_path)

    def build_model(
        self,
        num_vars: int,
        seq_len_in: int,
        seq_len_out: int,
        units: int,
        batch_size: int,
        dropout_rate: float,
        teacher_forcing: bool,
        loss_function: keras.losses.Loss,
        metrics: List[keras.metrics.Metric],
    ) -> keras.Model:

        if teacher_forcing:
            inputs = keras.Input(batch_shape=(batch_size, seq_len_in, num_vars))
            h = keras.layers.GRU(units=units, stateful=True, return_sequences=True)(inputs)  # activation='tanh'
            h = keras.layers.Dropout(dropout_rate)(h)
            h = keras.layers.GRU(units=units, stateful=True, return_sequences=False)(h)
            h = keras.layers.Dropout(dropout_rate)(h)
        else:
            inputs = keras.Input(shape=(seq_len_in, num_vars))
            h = keras.layers.GRU(units=units, return_sequences=True)(inputs)
            h = keras.layers.Dropout(dropout_rate)(h)
            h = keras.layers.GRU(units=units, return_sequences=False)(h)
            h = keras.layers.Dropout(dropout_rate)(h)

        # Adding the output layer:
        outputs = keras.layers.Dense(units=seq_len_out * num_vars, activation='sigmoid')(h)

        model = keras.Model(
            inputs=inputs,
            outputs=outputs
        )

        # compile model
        opt = keras.optimizers.Adam(learning_rate=0.001, clipnorm=1.0)
        model.compile(
            loss=loss_function,
            optimizer=opt,
            metrics=metrics,
        )

        return model

    def load_data(self, input_file: Path, X, Y):
        # resolve columns to load
        columns = list(set(X + Y))
        columns.sort()

        df = asyncio.run(
            data_utils.read_jsonl_to_df_stream(
                file_path=input_file,
                columns=columns
            )
        )
        if not df.empty:
            df.index.name = "ts"  # restore index name if desired
            df.index = pd.to_datetime(df.index.astype(int), unit="ms")

        # interpolate missing data
        df = df.interpolate(method="time")

        # fill NaNs with zeros
        df = df.fillna(0.0)

        return df

    def piece_function(self, input_data: InputModel):

        # ensure the results path dir exists
        self.get_results_path().mkdir(parents=True, exist_ok=True)

        last_model_file_path = self.get_results_path() / "model_last.h5"
        best_model_file_path = self.get_results_path() / "model_best.h5"

        # load data
        df = self.load_data(Path(input_data.input_file), input_data.X, input_data.Y)

        time_step_unit = datetime.timedelta(minutes=15)

        pipeline = RelativeTimeSeriesPipeline(
            seq_len_in=utils.timedelta_to_steps(datetime.timedelta(hours=1), time_step_unit),
            seq_len_out=utils.timedelta_to_steps(time_step_unit, time_step_unit),
            roll_windows=[],
            units=input_data.units,
            dropout_rate=input_data.dropout_rate,
            epochs=input_data.epochs,
            batch_size=input_data.batch_size,
            time_step_unit=datetime.timedelta(minutes=15)  # step size of data
        )

        pipeline.fit(df)

        history = pipeline.history
        model = pipeline.model_
        metrics = pipeline.metrics
        loss_function = pipeline.loss

        # save also the current model
        model.save(last_model_file_path)

        self.logger.info('Producing training report...')

        # Create a single figure with one subplot per metric
        fig, axes = plt.subplots(len(metrics), 1, figsize=(8, 6))  # stacked vertically

        # plot the metrics
        for i, metric in enumerate(metrics):
            metric_label = f'{metric.name} (loss)' if metric.name == loss_function.name else f'{metric.name}'
            ax = axes[i] if len(metrics) > 1 else axes
            ax.plot(history.history[metric.name])
            legend = ['train']
            if f'val_{metric.name}' in history.history:
                legend.append('val')
                ax.plot(history.history[f'val_{metric.name}'])
            ax.set_title(f'Model {metric_label}')
            ax.set_ylabel(metric.name, fontsize='large')
            ax.set_xlabel("epoch", fontsize='large')
            ax.legend(legend, loc='best')
        plt.tight_layout()

        # Save single image with both graphs
        fig_path = self.get_results_path() / "training_metrics.png"
        plt.savefig(fig_path, dpi=300, bbox_inches="tight")
        plt.close(fig)

        # Set display result
        self.display_result = {
            'file_type': 'image',
            'file_path': fig_path
        }

        self.logger.info('Training report created.')

        return OutputModel(model_file=str(last_model_file_path))


if __name__ == '__main__':
    input_data = InputModel.model_construct(
        **{
            "input_file": str(Path(".") / "dry_run_results" / "TimeWindowAggregationPiece" / "PT10M.jsonl")
        }
    )
    piece = TrainModelPiece(
        deploy_mode='dry_run',
        task_id='0',
        dag_id='0',
    )

    piece.results_path = Path("dry_run_results") / "TrainModelPiece"
    piece.piece_function(input_data)
