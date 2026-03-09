from typing import Dict

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="Input JSONL File",
        description="Path to the input JSONL file that will be partitioned.",
        example="data/messages.jsonl",
    )
    output_dir: str = Field(
        ...,
        title="Output Directory",
        description="Directory where partitioned JSONL files will be saved.",
        example="output/partitions",
    )
    field: str = Field(
        ...,
        title="Partition Field",
        description="Name of the field in each JSON object used to partition the JSONL file.",
        example="kafka_key",
    )


class OutputModel(BaseModel):
    partitions: Dict[str, str] = Field(
        ...,
        title="Partitions Mapping",
        description="Mapping of partition names to their corresponding JSONL file paths.",
        example={"key1": "output/partitions/key1/messages.jsonl"},
    )
