import heapq
import json
import os
import tempfile
from pathlib import Path

import pandas as pd
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class KafkaSortByTimestampPiece(BasePiece):
    encoding = "utf-8"
    output_messages_file_name = "messages.jsonl"

    @staticmethod
    def sort_jsonl(input_file, output_file, chunksize=100000):
        temp_files = []

        # Step 1: Read JSONL in chunks with pandas, clean/process, sort, write temp files
        for chunk in pd.read_json(input_file, lines=True, chunksize=chunksize):
            # Extract numeric timestamp for sorting
            chunk["_ts"] = chunk["timestamp"].apply(lambda x: x[1])

            # Optional: any cleaning or preprocessing here
            # chunk["value"] = chunk["value"].apply(lambda x: json.loads(x))

            chunk_sorted = chunk.sort_values("_ts")
            chunk_sorted = chunk_sorted.drop(columns="_ts")

            # Write sorted chunk to temp file
            temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
            for record in chunk_sorted.to_dict(orient="records"):
                temp_file.write(json.dumps(record) + "\n")
            temp_file.close()
            temp_files.append(temp_file.name)

        # Step 2: Merge all sorted temp files using heapq
        def iter_file(file_name):
            with open(file_name, "r", encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)

        iterators = [iter_file(f) for f in temp_files]

        with open(output_file, "w", encoding="utf-8") as out_f:
            for record in heapq.merge(*iterators, key=lambda x: x["timestamp"][1]):
                out_f.write(json.dumps(record) + "\n")

        # Step 3: Cleanup temp files
        for f in temp_files:
            os.remove(f)

    def piece_function(self, input_data: InputModel):
        self.input_messages_file_path = input_data.input_messages_file_path

        # output dir is relative to the results_path
        self.output_messages_file_path = os.path.join(
            self.results_path,
            input_data.output_messages_file_path,
        )
        output_dir = os.path.dirname(self.output_messages_file_path)
        if not Path(output_dir).exists():
            os.makedirs(output_dir)

        self.sort_jsonl(self.input_messages_file_path, self.output_messages_file_path)

        return OutputModel(
            output_messages_file_path=self.output_messages_file_path,
        )
