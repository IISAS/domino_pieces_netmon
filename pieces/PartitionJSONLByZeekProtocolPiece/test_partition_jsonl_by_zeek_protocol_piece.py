import json
import logging
import tempfile
from pathlib import Path

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

from .models import OutputModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


@skip_envs('github')
def test_partition_jsonl_by_field_piece_local():
    input_data = {
        'input_file': 'data/messages.jsonl',
    }

    output = piece_dry_run(
        piece_name="PartitionJSONLByZeekProtocolPiece",
        input_data=input_data,
    )

    print(output)

    # Optionally, verify that output files exist
    for key, path in {p: output[p] for p in OutputModel.protocols}.items():
        assert Path(path).exists(), f"Partition file {path} does not exist"
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            logger.info("Partition '%s' has %d lines", key, len(lines))


@skip_envs("dev")
def test_partition_jsonl_by_field_piece():
    # Create a temporary directory for input and output
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        input_file = tmp_dir_path / "test_messages.jsonl"

        # Generate a small test JSONL file
        test_messages = [
            {"key": "conn", "value": 1},
            {"key": "conn", "value": 2},
            {"key": "dns", "value": 3},
            {"key": "ftp", "value": 4},
            {"key": "conn", "value": 5}
        ]
        with input_file.open("w", encoding="utf-8") as f:
            for msg in test_messages:
                f.write(json.dumps(msg) + "\n")

        # Prepare input data for the piece
        output_dir = tmp_dir_path / "output_split"
        input_data = {
            "input_file": str(input_file),
            "output_dir": str(output_dir),
            "field": "key"
        }

        # Run the piece dry run
        output = piece_dry_run(
            piece_name="PartitionJSONLByZeekProtocolPiece",
            input_data=input_data,
        )

        print(output)

        # Optionally, verify that output files exist
        for key, path in ((k, p) for k, p in output.items() if k in OutputModel.protocols):
            assert Path(path).exists(), f"Partition file {path} does not exist"
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                logger.info("Partition '%s' has %d lines", key, len(lines))
