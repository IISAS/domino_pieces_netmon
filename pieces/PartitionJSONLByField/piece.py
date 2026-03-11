import json
from collections import OrderedDict
from pathlib import Path

from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class PartitionJSONLByField(BasePiece):
    """Splits a JSONL file into multiple JSONL files by a field value using LRU file cache."""

    encoding = "utf-8"
    max_open_files = 256  # avoid hitting OS file descriptor limits

    def piece_function(self, input_data: InputModel):
        input_path = Path(input_data.input_file)

        output_dir = Path(input_data.output_dir)
        if not output_dir.is_absolute():
            output_dir = Path(self.results_path) / output_dir

        partitions: dict[str, str] = {}
        open_files: "OrderedDict[str, object]" = OrderedDict()
        num_lines = 0
        num_invalid = 0

        def get_file(key: str):
            """Return an open file handle for a key, using LRU cache."""
            if key in open_files:
                open_files.move_to_end(key)
                return open_files[key]

            filepath = output_dir / key / input_path.name
            filepath.parent.mkdir(parents=True, exist_ok=True)
            fp = open(filepath, "a", encoding=self.encoding)
            open_files[key] = fp
            partitions.setdefault(key, str(filepath))

            if len(open_files) > self.max_open_files:
                old_key, old_fp = open_files.popitem(last=False)
                old_fp.close()

            return fp

        with open(input_path, "r", encoding=self.encoding) as fp_in:
            for line in fp_in:
                num_lines += 1
                try:
                    data = json.loads(line)
                    key = data[input_data.field]
                except json.JSONDecodeError as e:
                    num_invalid += 1
                    self.logger.warning("Invalid JSON on line %d: %s", num_lines, e)
                    continue
                except KeyError:
                    num_invalid += 1
                    self.logger.warning("Missing field '%s' on line %d", self.field, num_lines)
                    continue

                get_file(key).write(line)

                if num_lines % 1000 == 0:
                    self.logger.info("Processed %dk lines", num_lines // 1000)

        # close remaining files
        for fp in open_files.values():
            fp.close()

        if num_invalid:
            self.logger.warning("Total invalid JSON lines: %d", num_invalid)

        return OutputModel(partitions=partitions)
