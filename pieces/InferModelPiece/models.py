from typing import List

from pydantic import BaseModel
from pydantic import Field


class InputModel(BaseModel):
    model_file_path: str = Field(
        ...,
        title="model.file.path",
        description="Path to the trained Keras model file (from TrainModelPiece).",
        json_schema_extra={"from_upstream": "always"},
    )
    input_file: str = Field(
        ...,
        title="input.file",
        description="Path to a JSONL file containing input data for inference.",
        json_schema_extra={"from_upstream": "always"},
    )
    X: List[str] = Field(
        title="Feature Columns (X)",
        description="List of column names to use as input features. Must match the training configuration.",
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
        description="List of column names used as targets. Must match the training configuration.",
        default=[
            "conn_count_uid_in",
            "conn_count_uid_out",
            "dns_count_uid_out",
            "http_count_uid_in",
            "ssl_count_uid_in"
        ]
    )


class OutputModel(BaseModel):
    output_file: str = Field(
        title="output.file",
        description="Path to the JSONL file containing model predictions.",
        default="predictions.jsonl",
    )