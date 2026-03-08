from typing import Set

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_messages_file_path: str = Field(
        title="input.messages.file.path",
        default="messages.jsonl",
        description="Path to a file containing messages to process.",
    )
    output_messages_dir_name: str = Field(
        title="output.messages.dir.path",
        default="output-messages.jsonl",
        description="Path to a directory to write the splitted messages.",
    )


class OutputModel(BaseModel):
    files: Set[str] = Field(
        title="files",
        description="List of file paths with splitted messages.",
    )
    protocols: Set[str] = Field(
        title="protocols",
        description="Dictionary of protocols used to split the messages.",
    )
