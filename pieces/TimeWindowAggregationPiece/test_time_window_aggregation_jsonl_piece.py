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
        'input_file': 'data/messages2.jsonl',
        'aggregation_period': 'PT10M',
        'num_workers': 12
    }

    output = piece_dry_run(
        piece_name="TimeWindowAggregationPiece",
        input_data=input_data,
    )
    print(output)
