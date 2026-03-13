from typing import Dict

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="Input JSONL File",
        description="Path to the input JSONL file that will be partitioned.",
    )
    field: str = Field(
        ...,
        title="Partition Field",
        description="Name of the field in each JSON object used to partition the JSONL file.",
        json_schema_extra={
            "from_upstream": "never",
        }
    )


class OutputModel(BaseModel):
    partitions: Dict[str, str] = Field(
        ...,
        title="Partitions Mapping",
        description="Mapping of partition names to their corresponding JSONL file paths.",
    )
