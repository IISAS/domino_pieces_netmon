from collections import OrderedDict
from pathlib import Path
from typing import Dict, BinaryIO, List

import orjson
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class PartitionJSONLByZeekProtocolPiece(BasePiece):
    """High-throughput JSONL partitioner for very large files."""

    field = "key"
    max_open_files = 256
    buffer_size = 4096  # number of lines per write

    def piece_function(self, input_data: InputModel):

        input_path = Path(input_data.input_file)

        output_dir = Path(self.results_path)
        if self.__class__.__name__ == "DryPiece":
            output_dir = output_dir / "PartitionJSONLByZeekProtocolPiece"
        output_dir.mkdir(parents=True, exist_ok=True)

        partitions: Dict[str, str] = {}
        for p in OutputModel.protocols:
            file_path = output_dir / f"{p}.jsonl"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("")
            partitions.setdefault(p, file_path)

        open_files: "OrderedDict[str, BinaryIO]" = OrderedDict()
        buffers: Dict[str, List[bytes]] = {}

        num_lines = 0
        num_invalid = 0

        def get_file(key: str) -> BinaryIO:
            """LRU cached file handles."""
            if key in open_files:
                open_files.move_to_end(key)
                return open_files[key]

            filepath = partitions[key]

            fp = open(filepath, "ab", buffering=1024 * 1024)
            open_files[key] = fp
            buffers.setdefault(key, [])

            # Evict old file if too many open
            if len(open_files) > self.max_open_files:
                old_key, old_fp = open_files.popitem(last=False)
                buf = buffers.pop(old_key, [])
                if buf:
                    old_fp.write(b"".join(buf))
                old_fp.close()

            return fp

        with input_path.open("rb") as fp_in:
            for line in fp_in:
                num_lines += 1
                try:
                    data = orjson.loads(line)
                    key = str(data[self.field])
                except Exception:
                    num_invalid += 1
                    continue

                if key not in OutputModel.protocols:
                    continue

                fp = get_file(key)
                buf = buffers[key]

                buf.append(line)

                if len(buf) >= self.buffer_size:
                    fp.write(b"".join(buf))
                    buf.clear()

                if num_lines % 1_000_000 == 0:
                    self.logger.info("Processed %dM lines", num_lines // 1_000_000)

        # Flush remaining buffers
        for key, fp in open_files.items():
            buf = buffers.get(key)
            if buf:
                fp.write(b"".join(buf))
            fp.close()

        if num_invalid:
            self.logger.warning("Total invalid lines: %d", num_invalid)

        return OutputModel(
            conn=str(partitions['conn']),
            dns=str(partitions['dns']),
            ftp=str(partitions['ftp']),
            sip=str(partitions['sip']),
            smtp=str(partitions['smtp']),
            ssh=str(partitions['ssh']),
            ssl=str(partitions['ssl']),
            http=str(partitions['http']),
        )
