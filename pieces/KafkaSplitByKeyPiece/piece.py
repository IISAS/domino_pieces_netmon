import json
import os
from contextlib import ExitStack
from pathlib import Path

from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


class KafkaSplitByKeyPiece(BasePiece):
    encoding = "utf-8"
    output_messages_file_name = "messages.jsonl"

    def piece_function(self, input_data: InputModel):

        self.input_messages_file_path = input_data.input_messages_file_path

        # output dir is relative to the results_path
        self.output_messages_dir_path = os.path.join(
            self.results_path,
            input_data.output_messages_dir_name,
        )
        if not Path(self.output_messages_dir_path).exists():
            os.makedirs(self.output_messages_dir_path)

        protocols = set()
        files = set()

        num_invalid_json_message_lines = 0
        with (ExitStack() as stack):
            fp_in = stack.enter_context(
                open(self.input_messages_file_path, "r", encoding=self.encoding)
            )
            fp_out = {}

            total_lines = 0
            for msg_line in fp_in:
                total_lines += 1
                try:
                    message = json.loads(msg_line)
                    message_key = message['key']
                    protocols.add(message_key)
                    # create output context
                    if not message_key in fp_out:
                        filename = os.path.join(
                            self.output_messages_dir_path,
                            message_key,
                            "messages.jsonl"
                        )
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        fp_out[message_key] = stack.enter_context(
                            open(
                                filename,
                                "w",
                                encoding=self.encoding,
                            )
                        )
                        files.add(filename)

                    fp_out[message_key].write(msg_line)

                    if total_lines % 1000 == 0:
                        self.logger.info('message lines processed: %dk' % (total_lines // 1000))

                except Exception as e:
                    num_invalid_json_message_lines += 1
                    self.logger.warning(f"Failed to parse JSON message on line {total_lines}: '{msg_line}'\n{e}")
                    continue

        if num_invalid_json_message_lines > 0:
            self.logger.warning(f"Total invalid json message lines: {num_invalid_json_message_lines}")

        return OutputModel(
            files=files,
            protocols=protocols,
        )
