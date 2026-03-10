from datetime import timedelta
from typing import Dict, List, Optional, Literal

from pydantic import BaseModel, Field


class ToNumericRule(BaseModel):
    coerce: List[str] = Field(
        description="Columns to convert to numeric with coercion."
    )


class FillNaRule(BaseModel):
    RootModel: Dict[int, List[str]]


class AstypeRule(BaseModel):
    RootModel: Dict[Literal["float", "int", "str"], List[str]]


class DatasetCleaningRules(BaseModel):
    to_numeric: Optional[ToNumericRule] = None
    fillna: Optional[Dict[int, List[str]]] = None
    astype: Optional[Dict[Literal["float", "int", "str"], List[str]]] = None


AggregationOp = Literal[
    "count",
    "nunique",
    "sum",
    "mean",
    "min",
    "max"
]


class ProtocolAggregation(BaseModel):
    RootModel: Dict[str, List[AggregationOp]]


class AggregationSection(BaseModel):
    RootModel: Dict[str, ProtocolAggregation]


class GroupBySection(BaseModel):
    RootModel: Dict[str, ProtocolAggregation]


class AggregationRules(BaseModel):
    agg: Dict[str, List[AggregationOp]] = Field(
        description="Aggregation rules per protocol and field."
    )
    groupby: Optional[Dict[str, List[AggregationOp]]] = Field(
        default=None,
        description="Optional group-by aggregation rules per protocol."
    )


class NetDirection(BaseModel):
    regex: Optional[str] = Field(
        default="",
        description="Regex matching local networks. Leave empty if not used."
    )
    field_name: str = Field(
        default="mods_dir",
        description="Name of the field storing computed network direction."
    )
    orig_field: str = Field(
        default="id.orig_h",
        description="Field containing origin IP address."
    )
    resp_field: str = Field(
        default="id.resp_h",
        description="Field containing responder IP address."
    )


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="input.file",
        description="Path to a file containing messages to process.",
    )
    output_file: str = Field(
        ...,
        title="output.file",
        description="Path to a directory to write the processed messages.",
    )
    field: str = Field(
        title="field",
        default="ts_end",
        description="Timestamp field to perform windowing over."
    )
    num_workers: int = Field(
        title="num.workers",
        default=4,
        description="Number of parallel workers"
    )
    data_cleaning_rules: Optional[DatasetCleaningRules] = Field(
        default={},
        title="data.cleaning.rules",
        description="Rules for cleaning dataset fields before aggregation.",
    )
    aggregation_period: timedelta = Field(
        default=timedelta(minutes=10),
        title="aggregation.period",
        description="Time window length for aggregation and minimum wait for delayed logs.",
    )
    aggregation_rules: Optional[AggregationRules] = Field(
        default={
            'agg': {
                'uid': ['count', 'nunique'],
                'query': ['count', 'nunique'],
                'proto': ['count', 'nunique']
            },
            'groupby': {
                'AA': ['count'],
                'RA': ['count'],
                'RD': ['count'],
                'TC': ['count'],
                'rejected': ['count']
            }
        },
        title="aggregation.rules",
        description="JSON aggregation rules configuration.",
    )
    net_direction: Optional[NetDirection] = Field(
        default={
            # regex matching local networks. set to empty, if not used
            'regex': r'147\.213\..+',
            # name of the field containing networking direction according to local networks regex
            'field': 'mods_dir',
            'orig_field': 'id.orig_h',
            'resp_field': 'id.resp_h'
        },
        title="net.direction",
        description="Configuration for computing network direction based on local network regex.",
    )


class OutputModel(BaseModel):
    output_file: str = Field(
        title="output.file",
        description="Ouput file path."
    )
