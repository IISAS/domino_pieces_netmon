import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

import json
import logging
import random
import tempfile
from pathlib import Path

from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


@skip_envs("github")
def test_sort_jsonl_by_field_piece_local():
    """Integration test using existing dataset."""
    input_data = {
        "input_file": "dry_run_results/output-values/dns/messages.jsonl",
        "output_file": "output-sort/dns/messages.jsonl",
        "field": "ts_end",
        "num_workers": 4,
    }

    output = piece_dry_run(
        piece_name="SortJSONLByFieldPiece",
        input_data=input_data,
    )

    print(output)


def test_sort_jsonl_by_field_piece():
    """Unit test using generated JSONL data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        input_file = tmpdir / "input.jsonl"
        output_file = tmpdir / "sorted.jsonl"

        # Generate random JSONL data
        records = []
        for i in range(2000):
            record = {
                "id": i,
                "ts_end": random.randint(1, 100000),
                "value": random.random(),
            }
            records.append(record)

        # Write unsorted data
        with open(input_file, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        input_data = {
            "input_file": str(input_file),
            "output_file": str(output_file),
            "field": "ts_end",
            "num_workers": 4,
        }

        result = piece_dry_run(
            piece_name="SortJSONLByFieldPiece",
            input_data=input_data,
        )

        output_path = Path(result["output_file"])

        # Load sorted output
        sorted_records = []
        with open(output_path, "r", encoding="utf-8") as f:
            for line in f:
                sorted_records.append(json.loads(line))

        # Validate sorting
        values = [r["ts_end"] for r in sorted_records]
        assert values == sorted(values)

        logger.info("Generated data sort test passed (%d records)", len(values))
