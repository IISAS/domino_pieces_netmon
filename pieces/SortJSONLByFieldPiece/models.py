from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="Input JSONL File",
        description="Path to the input JSONL file containing messages to process.",
    )
    output_file: str = Field(
        ...,
        title="Output JSONL File",
        description="Path to the output JSONL file.",
    )
    field: str = Field(
        ...,
        title="Field Name",
        description="Name of the field in each JSON message that contains JSON to extract.",
    )
    num_workers: int = Field(
        title="Number of Workers",
        description="Number of workers to use.",
        default=4,
    )


class OutputModel(BaseModel):
    output_file: str = Field(
        title="output.file",
        default="messages.jsonl",
        description="Path to the final sorted JSONL output file.",
    )
