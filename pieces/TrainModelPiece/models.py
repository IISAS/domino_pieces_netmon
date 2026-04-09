from typing import List

from pydantic import BaseModel
from pydantic import Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="input.file",
        description="Path to a file containing training data.",
    )
    split: float = Field(
        title="split.ratio",
        description="The proportion of training data to be used for training the model.",
        default=0.7
    )
    diff: bool = Field(
        title="differencing",
        description="Apply first-order differencing to the input sequence before training. Each value is replaced by its difference from the previous timestep: [x₁, x₂, ..., xₙ] → [x₂−x₁, x₃−x₂, ..., xₙ−xₙ₋₁].",
        default=False,
    )
    seq_len_in: int = Field(
        title="seq.len.in",
        description="Length of the model's input sequence (in number of timesteps).",
        default=24,
    )
    seq_len_out: int = Field(
        title="seq.len.out",
        description="Length of the model's output sequence (in number of timesteps).",
        default=1,
    )
    teacher_forcing: bool = Field(
        title="teacher.forcing",
        description="Whether or not to use teacher forcing.",
        default=False,
    )
    gru_units: int = Field(
        title="GRU units",
        description="The number of GRU units in the model.",
        default=60
    )
    dropout_rate: float = Field(
        title="dropout.rate",
        description="The dropout rate for the model.",
        default=0.6,
    )
    klag_steps: int = Field(
        title="klag.steps",
        description="The number of timesteps to compute diff. 0 - no diff is computed.",
        default=1,
    )
    batch_size: int = Field(
        title="batch.size",
        description="The batch size for training the model.",
        default=1,
    )
    epochs: int = Field(
        title="epochs",
        description="The number of training epochs.",
        default=100,
    )
    epochs_patience: int = Field(
        title="epochs_patience",
        description="The number of epochs to wait before stopping training.",
        default=10,
    )
    tsg_sampling_rate: int = Field(
        title="tsg.sampling_rate",
        description="Period between successive individual timesteps within sequences. For rate r, timesteps data[i], data[i-r], ... data[i - length] are used for create a sample sequence.",
        default=1,
    )
    tsg_stride: int = Field(
        title="tsg.stride",
        description="Period between successive output sequences. For stride s, consecutive output samples would be centered around data[i], data[i+s], data[i+2*s], etc.",
        default=1,
    )
    X: List[str] = Field(
        title="Feature Columns (X)",
        description="List of column names to be used as input features for the model training (independent variables).",
        default=[
            "conn_count_uid_in",
            "conn_count_uid_out",
            "dns_count_uid_out",
            "http_count_uid_in",
            "ssl_count_uid_in"
        ]
    )
    Y: List[str] = Field(
        title="Target Columns (Y)",
        description="List of column names to be used as target variables for the model training (dependent variables).",
        default=[
            "conn_count_uid_in",
            "conn_count_uid_out",
            "dns_count_uid_out",
            "http_count_uid_in",
            "ssl_count_uid_in"
        ]
    )


class OutputModel(BaseModel):
    best_model_file_path: str = Field(
        title="best_model_file_path",
        default="best_model.keras",
        description="Path to the saved best model."
    )
    last_model_file_path: str = Field(
        title="last_model_file_path",
        default="last_model.keras",
        description="Path to the saved last model."
    )
