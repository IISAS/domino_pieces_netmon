import logging

from domino.testing import piece_dry_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


def test_sort_by_timestamp_piece():
    input_data = {
        'input_messages_file_path': '/mnt/data/workspace/SPICE/domino_pieces_netmon/data/input-messages.jsonl',
        'output_messages_file_path': 'output-sort/messages.jsonl',
    }

    output = piece_dry_run(
        piece_name="KafkaSortByTimestampPiece",
        input_data=input_data,
    )
    print(output)
