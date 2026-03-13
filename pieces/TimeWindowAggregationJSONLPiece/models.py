import json
from datetime import timedelta
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="input.file",
        description="Path to a file containing messages to process.",
        json_schema_extra={
            "from_upstream": "always",
        }
    )
    field: str = Field(
        title="field",
        description="Timestamp field to perform windowing over.",
        default="ts_end",
        json_schema_extra={
            "from_upstream": "never",
        }

    )
    num_workers: int = Field(
        title="num.workers",
        description="Number of parallel workers",
        default=4,
        json_schema_extra={
            "from_upstream": "never",
        }

    )
    data_cleaning_rules: Optional[Dict[str, Any]] = Field(
        title="data.cleaning.rules",
        description="Rules for cleaning dataset fields before aggregation.",
        default={},
        json_schema_extra={
            "from_upstream": "never",
            'widget': "codeeditor-json",
        }
    )
    aggregation_period: timedelta = Field(
        title="aggregation.period",
        description="Time window length for aggregation and minimum wait for delayed logs.",
        default=timedelta(minutes=10),
        json_schema_extra={
            "from_upstream": "never",
        }
    )
    aggregation_rules: Optional[Dict[str, Any]] = Field(
        title="aggregation.rules",
        default={
            'agg': {
            },
            'groupby': {
            }
        },
        description="JSON aggregation rules configuration.",
        json_schema_extra={
            "from_upstream": "never",
            'widget': "codeeditor-json",
        }
    )

    net_direction: Optional[Dict[str, Any]] = Field(
        title="net.direction",
        default={
        },
        description="Configuration for computing network direction based on local network regex.",
        json_schema_extra={
            "from_upstream": "never",
            'widget': "codeeditor-json",
        }
    )

    @field_validator("aggregation_rules", "data_cleaning_rules", "net_direction", mode="before")
    @classmethod
    def parse_rules(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v


class OutputModel(BaseModel):
    output_file: str = Field(
        title="output.file",
        description="Ouput file path."
    )
