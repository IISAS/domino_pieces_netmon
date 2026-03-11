import asyncio
from pathlib import Path
from typing import List

import orjson
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class ExtractFieldAsJSONPiece(BasePiece):
    """High-throughput extraction of a JSON field from JSONL files using orjson and binary I/O."""

    buffer_size = 1000  # lines per batch write
    num_workers = 4  # number of concurrent parser tasks

    @staticmethod
    def parse_value(value):
        if isinstance(value, str):
            value_parsed = orjson.loads(value.encode())
        elif isinstance(value, bytes):
            value_parsed = orjson.loads(value)
        else:
            value_parsed = value
        return value_parsed

    async def _reader(self, input_path: Path, parse_queue: asyncio.Queue):
        """Reads lines from the input file and pushes them to parse_queue."""
        with input_path.open("rb") as f_in:
            for line in f_in:
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
                data = self.parse_value(line)
                value = data[field]
                # if the field itself is a JSON string, decode it
                if isinstance(value, (bytes, str)):
                    try:
                        value_parsed = self.parse_value(value)
                    except Exception:
                        value_parsed = value  # fallback if not JSON
                else:
                    value_parsed = value
                await output_queue.put(orjson.dumps(value_parsed, option=orjson.OPT_APPEND_NEWLINE))
            except KeyError:
                self.logger.warning("Missing field '%s' in line", field)
            except Exception:
                self.logger.warning("Invalid JSON line skipped")

    async def _writer(self, output_path: Path, output_queue: asyncio.Queue):
        """Async writer with buffered binary writes."""
        buffer: List[bytes] = []

        with output_path.open("wb") as f_out:
            while True:
                line = await output_queue.get()
                if line is None:  # sentinel
                    break
                buffer.append(line)
                if len(buffer) >= self.buffer_size:
                    f_out.write(b"".join(buffer))
                    buffer.clear()
            if buffer:
                f_out.write(b"".join(buffer))

    def piece_function(self, input_data: InputModel):
        input_file = Path(input_data.input_file)

        output_file = Path(input_data.output_file)
        if not output_file.is_absolute():
            output_file = Path(self.results_path) / output_file

        output_file.parent.mkdir(parents=True, exist_ok=True)

        self.num_workers = input_data.num_workers

        parse_queue: asyncio.Queue = asyncio.Queue(maxsize=self.buffer_size * 4)
        output_queue: asyncio.Queue = asyncio.Queue(maxsize=self.buffer_size * 4)

        async def main():
            # Start writer task
            writer_task = asyncio.create_task(self._writer(output_file, output_queue))

            # Start parser workers
            parser_tasks = [
                asyncio.create_task(self._parser(input_data.field, parse_queue, output_queue))
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
