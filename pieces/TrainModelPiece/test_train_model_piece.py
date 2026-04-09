import logging

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


@skip_envs('github')
def test_train_model_piece():
    input_data = {
        'input_file': 'dry_run_results/TimeWindowAggregationPiece/PT10M.jsonl',
    }

    output = piece_dry_run(
        piece_name="TrainModelPiece",
        input_data=input_data,
    )
    print(output)
