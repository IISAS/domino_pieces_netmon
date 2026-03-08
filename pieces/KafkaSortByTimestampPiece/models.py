from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_messages_file_path: str = Field(
        title="input.messages.file.path",
        default="messages.jsonl",
        description="Path to a file containing messages to process.",
    )
    output_messages_file_path: str = Field(
        title="output.messages.file.path",
        default="messages.jsonl",
        description="Output file path.",
    )


class OutputModel(BaseModel):
    output_messages_file_path: str = Field(
        title="output.messages.file.path",
        default="messages.jsonl",
    )
