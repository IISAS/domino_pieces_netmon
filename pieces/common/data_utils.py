from pathlib import Path
from typing import Tuple

import aiofiles
import numpy as np
import orjson
import pandas as pd
from keras.src.preprocessing.sequence import TimeseriesGenerator


async def read_jsonl_to_df_stream(file_path: Path, columns=None) -> pd.DataFrame:
    """
    Reads a JSONL file line by line, where each line is a JSON string produced by df.to_json(orient='index').
    Streams the data into a single DataFrame without creating intermediate DataFrames list.
    """
    first_chunk = True
    df_all = pd.DataFrame()

    async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
        async for line in f:
            line = line.strip()
            if not line:
                continue
            # parse line to dict using orjson
            data = orjson.loads(line)
            # convert dict to DataFrame
            df_chunk = pd.DataFrame.from_dict(data, orient='index', dtype=np.float32, columns=columns)
            if first_chunk:
                df_all = df_chunk
                first_chunk = False
            else:
                df_all = pd.concat([df_all, df_chunk], axis=0, copy=False)

    return df_all


def split_df(df: pd.DataFrame, split: float = 0.7) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not 0 < split < 1:
        raise ValueError("split must be between 0 and 1")
    rows = df.shape[0]
    at = int(split * rows)
    return df[:at], df[at:]


def klagdiff(df: pd.DataFrame, k: int = 1) -> pd.DataFrame:
    """
    Compute k-lag difference.

    Args:
        df: Input DataFrame
        k: Lag

    Returns:
        Differenced DataFrame
    """
    return df.diff(periods=k).iloc[k:] if k > 0 else df


def transform(df: pd.DataFrame, klag_steps=0) -> pd.DataFrame:
    df = klagdiff(df, klag_steps)
    return df


def inverse_transform(df: pd.DataFrame, df_orig: pd.DataFrame, klag_steps=0, seq_len=1) -> pd.DataFrame:
    if klag_steps > 0:
        df = df + df_orig[seq_len + klag_steps - 1:-klag_steps].values
    return df


def create_tsg(X, Y, forecast_steps, **args):
    if forecast_steps > 1:
        return TimeseriesGenerator(
            X[:-forecast_steps + 1],
            Y[forecast_steps - 1:],
            **args
        )
    else:
        return TimeseriesGenerator(
            X,
            Y,
            **args
        )
