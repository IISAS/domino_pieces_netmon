import logging

from domino.testing import piece_dry_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


def test_split_by_key_piece():
    input_data = {
        'input_messages_file_path': '/mnt/data/workspace/SPICE/domino_pieces_netmon/dry_run_results/output-sort/messages.jsonl',
        'output_messages_dir_name': 'output-split',
    }

    output = piece_dry_run(
        piece_name="KafkaSplitByKeyPiece",
        input_data=input_data,
    )
    print(output)
