import asyncio
import json
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from domino.base_piece import BasePiece
from tensorflow import keras

from common.data_utils import read_jsonl_to_df_stream
from common.time_series_pipeline import RelativeTimeSeriesPipeline
from .models import InputModel, OutputModel


class InferModelPiece(BasePiece):

    def get_results_path(self):
        if self.__class__.__name__ == "DryPiece":
            return Path(self.results_path) / self._metadata_['name']
        return Path(self.results_path)

    def load_data(self, input_file: Path, X: List[str], Y: List[str]) -> pd.DataFrame:
        columns = sorted(set(X + Y))
        df = asyncio.run(read_jsonl_to_df_stream(file_path=input_file, columns=columns))
        if not df.empty:
            df.index.name = "ts"
            df.index = pd.to_datetime(df.index.astype(int), unit="ms")
        df = df.interpolate(method="time")
        df = df.fillna(0.0)
        return df

    def piece_function(self, input_data: InputModel) -> OutputModel:
        self.get_results_path().mkdir(parents=True, exist_ok=True)

        # Load trained model
        model = keras.models.load_model(input_data.model_file_path)

        # Derive seq_len_in from model input shape: (None, seq_len_in, n_features)
        seq_len_in = model.input_shape[1]

        # Load and preprocess data using the same pipeline as training
        df = self.load_data(Path(input_data.input_file), input_data.X, input_data.Y)
        X_arr = np.asarray(df, dtype=float)

        pipeline = RelativeTimeSeriesPipeline(seq_len_in=seq_len_in)
        tsg = pipeline._transform(X_arr)

        if len(tsg) == 0:
            raise ValueError(
                f"Not enough data for inference: need more than {seq_len_in + 1} rows, got {len(X_arr)}."
            )

        # Run inference and inverse scale
        predictions_scaled = model.predict(tsg, verbose=0)
        predictions = pipeline.scaler.inverse_transform(predictions_scaled)

        # Align timestamps: TSG window i predicts X_arr[i + seq_len_in]
        timestamps = df.index[seq_len_in: seq_len_in + len(predictions)]

        columns = sorted(set(input_data.X + input_data.Y))
        output_path = self.get_results_path() / "predictions.jsonl"

        with open(output_path, "w") as f:
            for ts, pred in zip(timestamps, predictions):
                record = {"ts": int(ts.value // 1_000_000)}
                for col, val in zip(columns, pred):
                    record[col] = float(val)
                f.write(json.dumps(record) + "\n")

        self.logger.info(f"Saved {len(predictions)} predictions to {output_path}")

        return OutputModel(output_file=str(output_path))


if __name__ == '__main__':
    input_data = InputModel.model_construct(
        **{
            "model_file_path": str(
                Path("dry_run_results") / "TrainModelPiece" / "model_best.keras"
            ),
            "input_file": str(
                Path("dry_run_results") / "TimeWindowAggregationPiece" / "PT10M.jsonl"
            ),
        }
    )
    piece = InferModelPiece(
        deploy_mode='dry_run',
        task_id='0',
        dag_id='0',
    )
    piece.results_path = Path("dry_run_results") / "InferModelPiece"
    piece.piece_function(input_data)
