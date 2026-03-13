import asyncio
import heapq
import tempfile
from pathlib import Path
from typing import List

import orjson
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class SortJSONLByFieldPiece(BasePiece):
    """High-throughput sorter for JSONL files by a field."""

    chunk_size = 100_000
    buffer_size = 2000

    @staticmethod
    def parse_value(value):
        if isinstance(value, str):
            value_parsed = orjson.loads(value.encode())
        elif isinstance(value, bytes):
            value_parsed = orjson.loads(value)
        else:
            value_parsed = value
        return value_parsed

    async def _sort_chunk(self, chunk: List[dict], sort_field: str) -> Path:
        """Sort chunk and write to temp file."""
        chunk.sort(key=lambda x: x[sort_field])

        fd, path = tempfile.mkstemp(suffix=".jsonl")
        temp_path = Path(path)

        with open(fd, "wb") as f:
            for record in chunk:
                f.write(orjson.dumps(record))
                f.write(b"\n")

        return temp_path

    async def _read_chunks(self, input_path: Path):
        """Yield parsed JSON chunks."""
        chunk: List[dict] = []

        with input_path.open("rb") as f:
            for line in f:
                chunk.append(self.parse_value(line))

                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []

        if chunk:
            yield chunk

    def _iter_file(self, path: Path):
        """Yield parsed JSON objects from a sorted chunk file."""
        with path.open("rb") as f:
            for line in f:
                yield self.parse_value(line)

    async def _merge_sorted_chunks(
        self,
        temp_files: List[Path],
        output_path: Path,
        sort_field: str,
    ):
        """Merge sorted chunk files."""

        iterators = [self._iter_file(p) for p in temp_files]

        buffer: List[bytes] = []

        with output_path.open("wb") as f_out:
            for record in heapq.merge(*iterators, key=lambda x: x[sort_field]):
                buffer.append(orjson.dumps(record))

                if len(buffer) >= self.buffer_size:
                    f_out.write(b"\n".join(buffer))
                    f_out.write(b"\n")
                    buffer.clear()

            if buffer:
                f_out.write(b"\n".join(buffer))
                f_out.write(b"\n")

        for f in temp_files:
            f.unlink(missing_ok=True)

    async def _run(self, input_path: Path, output_path: Path, sort_field: str, workers: int):
        """Main pipeline."""

        semaphore = asyncio.Semaphore(workers)
        tasks = []

        async for chunk in self._read_chunks(input_path):
            async def worker(c=chunk):
                async with semaphore:
                    return await self._sort_chunk(c, sort_field)

            tasks.append(asyncio.create_task(worker()))

        temp_files = await asyncio.gather(*tasks)

        await self._merge_sorted_chunks(temp_files, output_path, sort_field)

    def piece_function(self, input_data: InputModel):

        input_path = Path(input_data.input_file)

        output_file = Path(self.results_path)
        if self.__class__.__name__ == "DryPiece":
            output_file = output_file / "SortJSONLByFieldPiece"
        output_file = output_file / input_path.name

        output_file.parent.mkdir(parents=True, exist_ok=True)

        asyncio.run(
            self._run(
                input_path,
                output_file,
                input_data.field,
                input_data.num_workers,
            )
        )

        self.logger.info(
            "Sorting complete: %s by '%s'",
            str(output_file),
            input_data.field,
        )

        return OutputModel(output_file=str(output_file))
