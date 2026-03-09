import asyncio
import json
from pathlib import Path
from typing import List

import aiofiles
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class ExtractFieldAsJSONPiece(BasePiece):
    """High-throughput async extraction of a JSON field from JSONL files."""

    encoding = "utf-8"
    buffer_size = 1000  # lines per batch write
    num_workers = 4  # number of concurrent parser tasks

    async def _reader(self, input_path: Path, parse_queue: asyncio.Queue):
        """Reads lines from the input file and pushes them to parse_queue."""
        async with aiofiles.open(input_path, "r", encoding=self.encoding) as f_in:
            async for line in f_in:
                await parse_queue.put(line)

        # Send sentinel values to signal parser workers to exit
        for _ in range(self.num_workers):
            await parse_queue.put(None)

    async def _parser(self, field: str, parse_queue: asyncio.Queue, output_queue: asyncio.Queue):
        """Parses lines from parse_queue and pushes JSON values to output_queue."""
        while True:
            line = await parse_queue.get()
            if line is None:  # sentinel
                break
            try:
                data = json.loads(line)
                value = data[field]
                value_json = json.loads(value)
                await output_queue.put(json.dumps(value_json))
            except json.JSONDecodeError:
                self.logger.warning("Invalid JSON line skipped")
            except KeyError:
                self.logger.warning("Missing field '%s' in line", field)

    async def _writer(self, output_path: Path, output_queue: asyncio.Queue):
        """Async writer with buffered writes."""
        buffer: List[str] = []
        async with aiofiles.open(output_path, "w", encoding=self.encoding) as f_out:
            while True:
                line = await output_queue.get()
                if line is None:  # sentinel
                    break
                buffer.append(line)
                if len(buffer) >= self.buffer_size:
                    await f_out.write("\n".join(buffer) + "\n")
                    buffer.clear()
            if buffer:
                await f_out.write("\n".join(buffer) + "\n")

    def piece_function(self, input_data: InputModel):
        input_file = Path(input_data.input_file)
        output_file = Path(self.results_path) / input_data.output_file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        field = input_data.field

        self.num_workers = input_data.num_workers

        parse_queue: asyncio.Queue = asyncio.Queue(maxsize=self.buffer_size * 4)
        output_queue: asyncio.Queue = asyncio.Queue(maxsize=self.buffer_size * 4)

        async def main():
            # Start writer task
            writer_task = asyncio.create_task(self._writer(output_file, output_queue))

            # Start parser workers
            parser_tasks = [
                asyncio.create_task(self._parser(field, parse_queue, output_queue))
                for _ in range(self.num_workers)
            ]

            # Start reader task
            reader_task = asyncio.create_task(self._reader(input_file, parse_queue))

            # Wait for reader and parsers
            await asyncio.gather(reader_task, *parser_tasks)

            # Signal writer to finish
            await output_queue.put(None)
            await writer_task

        asyncio.run(main())

        self.logger.info("Extraction complete: %s", str(output_file))
        return OutputModel(output_file=str(output_file))
