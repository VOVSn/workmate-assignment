from abc import ABC, abstractmethod
import argparse
import csv
import operator
import os
import re
import sys
from typing import Any, Callable, Dict, List, Tuple, TypeAlias

from rich.console import Console
from rich.table import Table
from rich.style import Style


CSVData: TypeAlias = List[Dict[str, str]]


class DataLoader(ABC):
    @abstractmethod
    def load(self, source: str) -> CSVData:
        pass


class CSVDataLoader(DataLoader):
    def load(self, filename: str) -> CSVData:
        try:
            with open(filename, mode='r', newline='', encoding='utf-8') as file:
                if not file.read(1):
                    return[]
                file.seek(0)
                reader = csv.DictReader(file)
                if not reader.fieldnames:
                    return []
                return list(reader)
        except FileNotFoundError:
            print(f'Error: file {filename} was not found', file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(
                f'Error occurred reading file or wrong format ({filename}):'
                f'{e}', file=sys.stderr
            )
            sys.exit(1)


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: CSVData) -> CSVData:
        pass



class WhereProcessor(DataProcessor):

    OPERATORS: Dict[str, Callable[[Any, Any], bool]] = {
        '=': operator.eq,
        '>': operator.gt,
        '<': operator.lt,
    }

    def __init__(self, key: str, op_str: str, value: str):
        self.key = key
        if op_str not in self.OPERATORS:
            raise ValueError(f'Unsupported operator: {op_str}. Use one of {List(self.OPERATORS.keys())}')
        self.op = self.OPERATORS[op_str]
        self.value_str = value
        self.is_numeric_comparison = self._is_float(value)

    @staticmethod
    def _is_float(val: str) -> bool:
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False
        
    def process(self, data: CSVData) -> CSVData:
        if not data:
            return []
        
        if self.key not in data[0]:
            print(f'Warning: key {self.key}, not found in columns', file=sys.stderr)
            return []
        
        filtered_data = []
        for row in data:
            row_value_str = row.get(self.key)
            if row_value_str is None:
                continue


            if self.is_numeric_comparison:
                try:
                        row_value_float = float(row_value_str)
                        condition_value_float = float(self.value_str)
                        if self.op(row_value_float, condition_value_float):
                            filtered_data.append(row)
                except (ValueError, TypeError):
                    continue
            
            else:
                if self.op(row_value_str, self.value_str):
                    filtered_data.append(row)
        return filtered_data


class DataViewer(ABC):
    @abstractmethod
    def show(self, data: CSVData, source_name: str):
        pass


class ConsoleViewer(DataViewer):

    STYLE_TEXT = Style(color='cyan')
    STYLE_TEXT_RED = Style(color='red')
    STYLE_NUMERIC = Style(color='bright_green')
    STYLE_HEADER = 'bold yellow'
    STYLE_BORDER = 'bright_black'
    STYLE_TITLE = 'bold default'

    @staticmethod
    def _generate_title_from_filename(filename: str) -> str:
        base_name = os.path.basename(filename)
        title, _ = os.path.splitext(base_name)
        return title.replace('_', ' ').title()


    @staticmethod
    def _is_float_or_int(value: str) -> bool:
        if not value:
            return False
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
        

    def _determine_column_types(
            self, data: CSVData, headers: List[str]
        ) -> Dict[str, bool]:
        column_is_numeric = {}
        for header in headers:
            column_is_numeric[header] = False
            for row in data:
                value = row.get(header, '')
                if value:
                    column_is_numeric[header] = self._is_float_or_int(value)
                    break
        return column_is_numeric


    def show(self, data: CSVData, source_name: str):
        console = Console()

        if not data:
            console.print('Nothing to display', style=self.STYLE_TEXT_RED)
            return
        
        table_title = self._generate_title_from_filename(source_name)
        table = Table(
            title=table_title,
            show_header=True,
            header_style=self.STYLE_HEADER,
            border_style=self.STYLE_BORDER,
            title_style=self.STYLE_TITLE,
        )

        headers = list(data[0].keys())
        column_types = self._determine_column_types(data, headers)

        for header in headers:
            is_numeric = column_types.get(header, False)
            table.add_column(
                header,
                justify='right' if is_numeric else 'left',
                style=self.STYLE_NUMERIC if is_numeric else self.STYLE_TEXT,
                no_wrap=False,
            )

        for row in data:
            str_row_values = [
                str(val) if val is not None else '' for val in row.values()]
            table.add_row(*str_row_values)

        console.print(table)


class CSVApplication:

    def __init__(self, loader: DataLoader, viewer: DataViewer, processors: List[DataProcessor]):
        self.loader = loader
        self.viewer = viewer
        self.processors = processors
        self.data: CSVData = []


    def run(self, source_file: str):
        console = Console()
        console.print(f'Loading data from: {source_file} ...\n')
        self.data = self.loader.load(source_file)

        processed_data = self.data
        if self.processors:
            console.print('Processing data')
        for processor in self.processors:
            processed_data = processor.process(processed_data)

        self.viewer.show(processed_data, source_name=source_file)

        if processed_data:
            console.print(f'\nSuccessfully displayed {len(processed_data)} records')
        elif self.data:
            console.print(f'\nNothing to display')


def parse_where_clause(clause: str) -> Tuple[str, str, str]:
    match = re.match(r'^([^=<>]+)([=<>])(.*)$', clause)
    if not match:
        raise ValueError(
            f'Invalid --where clause format: {clause}'
            'Expected format: "key=value" , "key<value" or "key>value"'
        )
    key, op, value = match.groups()
    return key.strip(), op.strip(), value.strip()


def main():
    parser = argparse.ArgumentParser(
        description=' Script for loading and showing csv data'
    )
    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Path to the file to be loaded'
    )
    parser.add_argument(
        '--where',
        type=str,
        help='Filter with condition like "rating>4.0"'
    )


    args = parser.parse_args()

    csv_loader = CSVDataLoader()
    console_viewer = ConsoleViewer()
    processors: List[DataProcessor] = []

    if args.where:
        try:
            key, op, value = parse_where_clause(args.where)
            where_processor = WhereProcessor(key, op, value)
            processors.append(where_processor)
        except ValueError as e:
            print(f'Error: {e}', file=sys.stderr)
            sys.exit(1)


    app = CSVApplication(loader=csv_loader, viewer=console_viewer, processors=processors)
    app.run(source_file=args.file)


if __name__ == "__main__":
    main()