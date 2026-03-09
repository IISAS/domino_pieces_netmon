import asyncio
import heapq
import json
import tempfile
from pathlib import Path
from typing import List

import aiofiles
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class SortJSONLByFieldPiece(BasePiece):
    """Async high-throughput sorter for JSONL files by a field with parallel chunk sorting."""

    encoding = "utf-8"
    chunk_size = 100_000  # number of lines per in-memory chunk
    buffer_size = 1000  # lines per buffered write
    num_workers = 4  # number of parallel chunk sort tasks

    async def _chunk_worker(
        self,
        sort_field: str,
        chunk_queue: asyncio.Queue,
        temp_files: List[Path],
    ):
        """Consume chunks from the queue, sort, and write temp files."""
        while True:
            chunk = await chunk_queue.get()
            if chunk is None:  # sentinel
                break
            sorted_chunk = sorted(chunk, key=lambda x: x[sort_field])
            temp_file = Path(tempfile.mkstemp(suffix=".jsonl")[1])
            async with aiofiles.open(temp_file, "w", encoding=self.encoding) as f:
                for record in sorted_chunk:
                    await f.write(json.dumps(record) + "\n")
            temp_files.append(temp_file)
            chunk_queue.task_done()

    async def _reader(self, input_path: Path, chunk_queue: asyncio.Queue):
        """Single async reader that fills chunk queue with lines."""
        chunk: List[dict] = []
        async with aiofiles.open(input_path, "r", encoding=self.encoding) as f:
            async for line in f:
                data = json.loads(line)
                chunk.append(data)
                if len(chunk) >= self.chunk_size:
                    await chunk_queue.put(chunk.copy())
                    chunk.clear()
            if chunk:
                await chunk_queue.put(chunk.copy())

        # Send sentinel values to stop workers
        for _ in range(self.num_workers):
            await chunk_queue.put(None)

    async def _merge_sorted_chunks(self, temp_files: List[Path], output_path: Path, sort_field: str):
        """Merge sorted temp files asynchronously using heapq."""

        def iter_file(file_path: Path):
            with file_path.open("r", encoding=self.encoding) as f:
                for line in f:
                    yield json.loads(line)

        iterators = [iter_file(f) for f in temp_files]

        buffer: List[str] = []
        async with aiofiles.open(output_path, "w", encoding=self.encoding) as f_out:
            for record in heapq.merge(*iterators, key=lambda x: x[sort_field]):
                buffer.append(json.dumps(record))
                if len(buffer) >= self.buffer_size:
                    await f_out.write("\n".join(buffer) + "\n")
                    buffer.clear()
            if buffer:
                await f_out.write("\n".join(buffer) + "\n")

        # Cleanup temp files
        for f in temp_files:
            f.unlink(missing_ok=True)

    def piece_function(self, input_data: InputModel):
        input_path = Path(input_data.input_file)
        output_path = Path(self.results_path) / input_data.output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_files: List[Path] = []
        self.num_workers = input_data.num_workers

        async def main():
            chunk_queue: asyncio.Queue = asyncio.Queue(maxsize=self.num_workers * 2)

            # Start worker tasks
            workers = [
                asyncio.create_task(self._chunk_worker(input_data.field, chunk_queue, temp_files))
                for _ in range(self.num_workers)
            ]

            # Start single reader
            reader_task = asyncio.create_task(self._reader(input_path, chunk_queue))

            await reader_task
            await asyncio.gather(*workers)

            # Merge sorted chunks
            await self._merge_sorted_chunks(temp_files, output_path, input_data.field)

        asyncio.run(main())

        self.logger.info(
            "Sorting complete: %s by field '%s' using %d workers",
            str(output_path),
            input_data.field,
            self.num_workers,
        )
        return OutputModel(output_file=str(output_path))
