import json
from datetime import timedelta

from pydantic import BaseModel
from pydantic import Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="input.file",
        description="Path to a file containing messages to process.",
        json_schema_extra={
            "from_upstream": "always",
        }
    )
    value_field: str = Field(
        title="value.field",
        description="Timestamp field to perform windowing over.",
        default="value",
        json_schema_extra={
            "from_upstream": "never",
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
    data_cleaning_rules: str = Field(
        title="data.cleaning.rules",
        description="Rules for cleaning dataset fields before aggregation.",
        default=json.dumps(
            {
                'conn': {
                    'to_numeric': {
                        'coerce': [
                            'duration',
                            'orig_bytes',
                            'resp_bytes'
                        ]
                    },
                    'fillna': [
                        {
                            'value': 0.0,
                            'type': 'float',
                            'fields': [
                                'duration'
                            ]
                        },
                        {
                            'value': 0,
                            'type': 'int',
                            'fields': [
                                'orig_bytes',
                                'resp_bytes'
                            ]
                        }

                    ],
                    'astype': {
                        'float': [
                            'duration'
                        ],
                        'int': [
                            'orig_bytes',
                            'resp_bytes'
                        ]
                    }
                }
            }
        ),
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
    aggregation_rules: str = Field(
        title="aggregation.rules",
        default=json.dumps(
            {
                'agg': {
                    'conn': {
                        'uid': ['count', 'nunique'],
                        'orig_bytes': ['sum'],
                        'resp_bytes': ['sum'],
                        'duration': ['mean']
                    },
                    'dns': {
                        'uid': ['count', 'nunique'],
                        'query': ['count', 'nunique'],
                        'proto': ['count', 'nunique']
                    },
                    'sip': {
                        'uid': ['count', 'nunique'],
                        'id.orig_h': ['count', 'nunique'],
                        'id.orig_p': ['count', 'nunique'],
                        'id.resp_h': ['count', 'nunique'],
                        'id.resp_p': ['count', 'nunique']
                    },
                    'ssh': {
                        'uid': ['count', 'nunique']
                    },
                    'ssl': {
                        'uid': ['count', 'nunique']
                    },
                    'http': {
                        'uid': ['count', 'nunique']
                    }
                },
                'groupby': {
                    'dns': {
                        'AA': ['count'],
                        'RA': ['count'],
                        'RD': ['count'],
                        'TC': ['count'],
                        'rejected': ['count']
                    }
                }
            }
        ),
        description="JSON aggregation rules configuration.",
        json_schema_extra={
            "from_upstream": "never",
            'widget': "codeeditor-json",
        }
    )

    net_direction: str = Field(
        title="net.direction",
        default=json.dumps(
            {
                # regex matching local networks. set to empty, if not used
                'regex': r'147\.213\..+',
                # name of the field in the ZEEK's log containing networking direction according to local networks regex
                'field': 'mods_dir',
                'orig_field': 'id.orig_h',
                'resp_field': 'id.resp_h'
            }
        ),
        description="Configuration for computing network direction based on local network regex.",
        json_schema_extra={
            "from_upstream": "never",
            'widget': "codeeditor-json",
        }
    )


class OutputModel(BaseModel):
    output_file: str = Field(
        title="output.file",
        description="Ouput file path."
    )
