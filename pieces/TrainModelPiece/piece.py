import asyncio
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from domino.base_piece import BasePiece
from tensorflow import keras

from common.data_utils import read_jsonl_to_df_stream
from common.time_series_pipeline import RelativeTimeSeriesPipeline
from .models import InputModel, OutputModel


class TrainModelPiece(BasePiece):

    def get_results_path(self):
        if self.__class__.__name__ == "DryPiece":
            return Path(self.results_path) / self._metadata_['name']
        return Path(self.results_path)

    def load_data(self, input_file: Path, X, Y) -> pd.DataFrame:
        # resolve columns to load
        columns = list(set(X + Y))
        columns.sort()

        df = asyncio.run(
            read_jsonl_to_df_stream(
                file_path=input_file,
                columns=columns
            )
        )
        if not df.empty:
            df.index.name = "ts"  # restore index name
            df.index = pd.to_datetime(df.index.astype(int), unit="ms")  # timestep in milliseconds
        return df

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:

        # interpolate missing data
        df = df.interpolate(method="time")

        # fill NaNs with zeros
        df = df.fillna(0.0)

        return df

    def piece_function(self, input_data: InputModel):

        # ensure the results path dir exists
        self.get_results_path().mkdir(parents=True, exist_ok=True)

        last_model_file_path = self.get_results_path() / "model_last.keras"
        best_model_file_path = self.get_results_path() / "model_best.keras"

        # load data
        df = self.load_data(
            Path(input_data.input_file),
            input_data.X,
            input_data.Y
        )

        # preprocess data
        df = self.preprocess_data(df)

        pipeline = RelativeTimeSeriesPipeline(
            seq_len_in=input_data.seq_len_in,
            seq_len_out=input_data.seq_len_out,
            roll_windows=[],
            gru_units=input_data.gru_units,
            dropout_rate=input_data.dropout_rate,
            epochs=input_data.epochs,
            batch_size=input_data.batch_size
        )

        best_model_checkpoint = keras.callbacks.ModelCheckpoint(
            filepath=str(best_model_file_path),
            monitor='loss',
            save_best_only=True,
            verbose=0,
        )

        pipeline.fit(df, callbacks=[best_model_checkpoint])

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

        return OutputModel(
            best_model_file_path=str(best_model_file_path),
            last_model_file_path=str(last_model_file_path),
        )


if __name__ == '__main__':
    input_data = InputModel.model_construct(
        **{
            "input_file": str(Path(".") / "dry_run_results" / "TimeWindowAggregationPiece" / "PT10M.jsonl"),
            "seq_len_in": 6,
        }
    )
    piece = TrainModelPiece(
        deploy_mode='dry_run',
        task_id='0',
        dag_id='0',
    )

    piece.results_path = Path("dry_run_results") / "TrainModelPiece"
    piece.piece_function(input_data)
