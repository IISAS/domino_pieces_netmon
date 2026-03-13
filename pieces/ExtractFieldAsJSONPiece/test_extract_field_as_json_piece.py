import json
import logging
import tempfile
from pathlib import Path

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


@skip_envs('github')
def test_extract_field_as_json_piece_local():
    input_data = {
        'input_file': 'dry_run_results/PartitionJSONLByZeekProtocolPiece/dns.jsonl',
        'field': 'value',
        'num_workers': 10
    }

    output = piece_dry_run(
        piece_name="ExtractFieldAsJSONPiece",
        input_data=input_data,
    )
    print(output)


@skip_envs("github", "dev")
def test_extract_field_as_json_piece():
    # Create a temporary directory for input and output
    with tempfile.TemporaryDirectory(dir='.') as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        input_file = tmp_dir_path / "test.jsonl"

        # Generate a small JSONL test file
        test_messages = [
            {"value": '{"foo": ' + str(i) + '}', "other": "a"} for i in range(20)
        ]
        with input_file.open("w", encoding="utf-8") as f:
            for msg in test_messages:
                f.write(json.dumps(msg) + "\n")

        # Prepare input data for the piece
        input_data = {
            "input_file": str(input_file),
            "field": "value"
        }

        # Run the piece dry run
        output = piece_dry_run(
            piece_name="ExtractFieldAsJSONPiece",
            input_data=input_data,
            secrets_data={}
        )

        # Print the output partitions/results
        print(output)

        # Verify output file exists and content is correct
        output_file = Path(output['output_file'])
        assert output_file.exists(), "Output file was not created"
        with output_file.open("r", encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) == len(test_messages), "Output line count mismatch"
            logger.info("Extracted %d lines", len(lines))
