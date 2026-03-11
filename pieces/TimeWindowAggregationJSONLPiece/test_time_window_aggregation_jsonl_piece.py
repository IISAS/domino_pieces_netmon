import logging

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


@skip_envs('github')
def test_time_window_aggregation_jsonl_piece():
    input_data = {
        'input_file': 'dry_run_results/output-sort/dns/messages.jsonl',
        'output_file': 'output-agg/dns/PT10M.jsonl',
        'aggregation_period': 'PT10M',
        'aggregation_rules': {
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
        }
    }

    output = piece_dry_run(
        piece_name="TimeWindowAggregationJSONLPiece",
        input_data=input_data,
    )
    print(output)
