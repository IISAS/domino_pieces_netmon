import asyncio
import datetime
import json
import re
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any

import aiofiles
import isodate
import pandas as pd
from domino.base_piece import BasePiece
from tzlocal import get_localzone

from .models import InputModel, OutputModel

TYPE_MAP = {
    "str": str,
    "float": float,
    "int": int,
    "bool": bool,
}


class FlowDirection(str, Enum):
    IN = 'in'
    OUT = 'out'
    INTERNAL = 'internal'

    @classmethod
    def from_string(cls, value: str) -> "PieceEnum":
        """
        Parse enum member from string (case-insensitive).
        Raises ValueError if invalid.
        """
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(
            f"{value!r} is not a valid {cls.__name__}. "
            f"Allowed values: {sorted(cls.values())}"
        )

    def __str__(self) -> str:
        return self.value


class TimeWindowAggregationPiece(BasePiece):
    """
    High-throughput async grouping of JSONL records.

    Records are grouped by a value computed from a field,
    and an aggregation function is applied to each group.
    """

    encoding = "utf-8"
    tz_local = get_localzone()
    buffer_size = 10000
    num_workers = 4

    # -------------------------
    # User-defined operations
    # -------------------------

    @staticmethod
    def get_net_direction(orig_h, resp_h, regex_local) -> FlowDirection:
        if re.match(regex_local, orig_h):
            if re.match(regex_local, resp_h):
                return FlowDirection.INTERNAL
            else:
                return FlowDirection.OUT
        else:
            return FlowDirection.IN

    def add_direction(self, df):
        if 'regex' in self.net_direction and 'orig_field' in self.net_direction and 'resp_field' in self.net_direction:
            if self.net_direction['orig_field'] in df.columns and self.net_direction['resp_field'] in df.columns:
                self.logger.debug('add_direction call: %s' % df)
                df[self.net_direction['field']] = df.apply(
                    lambda row: self.get_net_direction(
                        row[self.net_direction['orig_field']],
                        row[self.net_direction['resp_field']],
                        self.net_direction['regex']
                    ),
                    axis=1
                )
                self.logger.debug('add_direction result: %s' % df)
        return df

    #
    # fields being collected before aggregation
    #
    # protocol -> [columns]
    #
    # !!! do not edit. this variable is populated automatically !!!
    def resolve_fields(
        self,
        aggregation_rules: dict,
        net_direction: dict,
    ):

        fields = {}

        # extract column names
        for k, v in aggregation_rules.items():
            for p, r in v.items():
                if p not in fields:
                    fields[p] = []
                fields[p].extend(r)

        # get sorted unique column names
        for k in fields:
            # add fields required for networking direction resolution
            if net_direction['regex']:
                fields[k].extend([net_direction['orig_field'], net_direction['resp_field']])
            fields[k] = sorted(set(fields[k]))

        return fields

    #
    # cleans data and sets dtype
    #
    def clean(self, df, rules):
        if 'to_numeric' in rules:
            for errors, columns in rules['to_numeric'].items():
                df[columns] = df[columns].apply(pd.to_numeric, errors=errors)
        if 'fillna' in rules:
            for rule in rules['fillna']:
                columns = rule['fields']
                type = TYPE_MAP[rule['type']]
                value = type(rule['value'])
                df[columns] = df[columns].fillna(value)
        if 'astype' in rules:
            for t, columns in rules['astype'].items():
                df[columns] = df[columns].astype(t)
        return df

    def vectorizedf(self, df: pd.DataFrame, prefix=''):
        self.logger.debug('vectorized, df: %s, prefix: %s' % (df, prefix))
        while not isinstance(df, pd.Series):
            df = df.stack().swaplevel()
        df = df[df.notna()]
        df.index = df.index.map(lambda x: '_'.join(x))
        df.index = df.index.map(('%s{0}' % prefix).format)
        self.logger.debug(df)
        return df.to_frame().T.sort_index(axis="columns", level=1)

    #
    # computes time window for time t; i.e., <begin, end)
    #
    def epoch(self, t: int, period: datetime.timedelta):
        """
        Compute the time window [begin, end) of size `period`
        that contains timestamp `t`.
        """

        if period.total_seconds() <= 0:
            raise ValueError("period must be positive")

        period_seconds = int(period.total_seconds())

        # convert timestamp to epoch seconds
        ts = int(t * 1e-3)

        # align timestamp to period boundary
        beg_ts = ts - (ts % period_seconds)

        beg = datetime.datetime.fromtimestamp(beg_ts, tz=datetime.timezone.utc)
        end = beg + period

        return beg, end

    def aggregate_group(self, zeek_log_name, records: List[Dict[str, Any]]):
        """
        Function applied to each group.

        Override to implement custom aggregation.
        Default: count records in group.
        """

        self.logger.info("aggregating %s ...", zeek_log_name)
        df = pd.DataFrame(records)

        # resolve network direction
        df = self.add_direction(df)

        # clean data
        if zeek_log_name in self.data_cleaning_rules:
            df = self.clean(df, self.data_cleaning_rules[zeek_log_name])

        # add grouping by networking direction field
        net_dir_grp = self.net_direction['field'] in df.columns if 'field' in self.net_direction else False

        # 1-level agg
        df_agg1 = pd.DataFrame()
        if zeek_log_name in self.aggregation_rules['agg']:
            df_agg1 = (df.groupby(self.net_direction['field']) if net_dir_grp else df) \
                .agg(self.aggregation_rules['agg'][zeek_log_name])
            df_agg1 = self.vectorizedf(df_agg1, prefix='%s_' % zeek_log_name)

        # 2-level agg
        df_agg2 = pd.DataFrame()
        if zeek_log_name in self.aggregation_rules['groupby']:
            gb_rules = self.aggregation_rules['groupby'][zeek_log_name]
            for gb, agg in gb_rules.items():
                tmp = df.groupby([self.net_direction['field'], gb] if net_dir_grp else gb).agg({gb: agg}).T
                df_agg2 = pd.concat([df_agg2, tmp])
                df_agg2 = df_agg2.fillna(0)
            df_agg2 = self.vectorizedf(df_agg2, prefix='%s_' % zeek_log_name)

        # join columns and sort
        df_agg = df_agg1.join(df_agg2).sort_index(axis="columns", level=1)

        self.logger.info("aggregating %s ... done", zeek_log_name)

        return df_agg

    def produce_json_from_agg(self, ts, df):
        index = pd.Index([ts], name='ts')
        df_agg = df.set_index(index)
        # df_agg.reset_index(level=0, inplace=True)
        df_json = df_agg.to_json(orient='index')
        return df_json

    # -------------------------
    # Pipeline stages
    # -------------------------

    async def _reader(self, input_path: Path, parse_queue: asyncio.Queue):
        async with aiofiles.open(input_path, "r", encoding=self.encoding) as f:
            async for line in f:
                await parse_queue.put(line)

        for _ in range(self.num_workers):
            await parse_queue.put(None)

    async def _worker(
        self,
        zeek_log_field: str,
        zeek_log_ts_field: str,
        parse_queue: asyncio.Queue,
        group_queue: asyncio.Queue,
    ):
        """Parse records and compute group keys."""
        while True:
            line = await parse_queue.get()

            if line is None:
                await group_queue.put(None)
                break

            try:
                record = json.loads(line)

                zeek_log_name = record['key']
                zeek_log_str = record[zeek_log_field]
                zeek_log = json.loads(zeek_log_str)

                # resolve timestamp of the zeek log
                zeek_log_ts = int(zeek_log[zeek_log_ts_field])

                # select fields
                zeek_log = {k: v for k, v in zeek_log.items() if k in self.distillation_rules[zeek_log_name]}

                # compute time window
                time_window = self.epoch(
                    t=zeek_log_ts,
                    period=self.aggregation_period
                )

                # group by ZEEK log and time window
                await group_queue.put(((zeek_log_name, time_window), zeek_log))

            except json.JSONDecodeError:
                self.logger.warning("Invalid JSON line skipped")

            except KeyError:
                self.logger.warning("Missing zeek_log_field '%s' or 'zeek_log_ts_field '%s'", zeek_log_ts_field,
                                    zeek_log_ts_field)

    def reduce_window(self, ts, dfs):
        """
        Reduce all Zeek log aggregates within the same time window.
        """

        self.logger.info("window %s reducing ...", ts)

        if not dfs:
            self.logger.info("window %s reducing ... done", ts)
            return None

        self.logger.info("window %s concatenating dfs ...", ts)
        df = pd.concat(dfs, axis=1)
        self.logger.info("window %s concatenating dfs ... done", ts)

        # ensure deterministic column order
        self.logger.info("window %s sorting ...", ts)
        df = df.sort_index(axis="columns")
        self.logger.info("window %s sorting ... done", ts)

        self.logger.info("window %s reducing ... done", ts)
        return self.produce_json_from_agg(ts, df)

    async def _aggregator(
        self,
        group_queue: asyncio.Queue,
        output_queue: asyncio.Queue,
    ):
        """
        Collect records by group and apply aggregation.
        """
        self.logger.info("aggregating ...")

        groups: Dict[Any, List[dict]] = defaultdict(list)

        finished_workers = 0

        self.logger.info("grouping records for aggregation ...")
        while True:
            item = await group_queue.get()

            if item is None:
                finished_workers += 1
                self.logger.info("finished workers %d", finished_workers)
                if finished_workers == self.num_workers:
                    self.logger.info("grouping records for aggregation ... done. groups: %d", len(groups.keys()))
                    break
                continue

            key, record = item
            groups[key].append(record)

        # Perform aggregation

        self.logger.info("aggregation ...")

        # Stage 1: aggregate per (zeek_log_name, window)
        window_groups = defaultdict(list)

        for key, records in groups.items():
            zeek_log_name, window = key
            ts = window[1]

            df_agg = self.aggregate_group(zeek_log_name, records)

            window_groups[ts].append(df_agg)

        self.logger.info("aggregation ... done")
        self.logger.info("reduction ...")

        # Stage 2: reduce across Zeek logs
        for ts in sorted(window_groups.keys()):
            dfs = window_groups[ts]
            json_agg = self.reduce_window(ts, dfs)
            if json_agg:
                await output_queue.put(json_agg)

        self.logger.info("reduction ... done")

        await output_queue.put(None)

    async def _writer(self, output_path: Path, output_queue: asyncio.Queue):
        buffer: List[str] = []

        async with aiofiles.open(output_path, "w", encoding=self.encoding) as f:
            while True:
                item = await output_queue.get()

                if item is None:
                    break

                buffer.append(item)

                if len(buffer) >= self.buffer_size:
                    await f.write("\n".join(buffer) + "\n")
                    buffer.clear()

            if buffer:
                await f.write("\n".join(buffer) + "\n")

    # -------------------------
    # Main entry
    # -------------------------

    def piece_function(self, input_data: InputModel):

        input_file = Path(input_data.input_file)

        output_file = Path(self.results_path)
        if self.__class__.__name__ == "DryPiece":
            output_file = output_file / "TimeWindowAggregationPiece"
        td = isodate.duration_isoformat(input_data.aggregation_period)
        output_file = output_file / f"{td}.jsonl"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        value_field = input_data.value_field
        field = input_data.field
        self.num_workers = input_data.num_workers

        def load_data_cleaning_rules(v):

            if isinstance(v, str):
                v = json.loads(v)

            for key, value in v.items():
                if "astype" in value:
                    new_types = {}

                    for type_name, fields in value["astype"].items():

                        if isinstance(type_name, str) and type_name in TYPE_MAP:
                            new_types[TYPE_MAP[type_name]] = fields
                        else:
                            new_types[type_name] = fields

                    value["astype"] = new_types

            return v

        self.data_cleaning_rules = load_data_cleaning_rules(input_data.data_cleaning_rules)

        self.aggregation_period = input_data.aggregation_period
        self.aggregation_rules = json.loads(input_data.aggregation_rules)
        self.net_direction = json.loads(input_data.net_direction)

        self.distillation_rules = self.resolve_fields(
            self.aggregation_rules,
            self.net_direction,
        )

        parse_queue = asyncio.Queue(maxsize=self.buffer_size * 4)
        group_queue = asyncio.Queue(maxsize=self.buffer_size * 4)
        output_queue = asyncio.Queue(maxsize=self.buffer_size * 4)

        async def main():
            writer_task = asyncio.create_task(
                self._writer(output_file, output_queue)
            )

            aggregator_task = asyncio.create_task(
                self._aggregator(group_queue, output_queue)
            )

            worker_tasks = [
                asyncio.create_task(
                    self._worker(value_field, field, parse_queue, group_queue)
                )
                for _ in range(self.num_workers)
            ]

            reader_task = asyncio.create_task(
                self._reader(input_file, parse_queue)
            )

            await asyncio.gather(reader_task, *worker_tasks)

            await aggregator_task
            await writer_task

        asyncio.run(main())

        self.logger.info("Grouping complete: %s", str(output_file))

        return OutputModel(output_file=str(output_file))
