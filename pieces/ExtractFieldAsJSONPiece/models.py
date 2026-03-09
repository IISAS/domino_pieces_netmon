from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="Input JSONL File",
        description="Path to the input JSONL file containing messages to process.",
        example="data/messages.jsonl",
    )
    output_file: str = Field(
        ...,
        title="Output JSONL File",
        description="Path where the extracted JSON messages will be written.",
        example="output/extracted_messages.jsonl",
    )
    field: str = Field(
        ...,
        title="Field Name",
        description="Name of the field in each JSON message that contains JSON to extract.",
        example="value",
    )
    num_workers: int = Field(
        title="Number of Workers",
        description="Number of workers to use.",
        default=4,
        example="4"
    )


class OutputModel(BaseModel):
    output_file: str = Field(
        ...,
        title="Output JSONL File Path",
        description="Path to the output JSONL file containing extracted JSON values.",
        example="output/extracted_messages.jsonl",
    )
