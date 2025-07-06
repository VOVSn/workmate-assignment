"""
This script is solution for test assignment
"""

from abc import ABC, abstractmethod
import argparse
import csv
import operator
import re
import statistics
import sys
from typing import Any, Callable, Dict, List, TypeAlias

from rich.console import Console
from rich.style import Style
from rich.table import Table


CSVData: TypeAlias = List[Dict[str, str]]


def is_numeric(value: Any) -> bool:
    """Checks if a value can be converted to a float"""
    if value is None:
        return False
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


class DataLoader(ABC):
    """Abstract base class for data loading"""
    @abstractmethod
    def load(self, source: str) -> CSVData:
        """Loads data from specified source"""


class DataProcessor(ABC):
    """Abstract base class for data processing"""
    @abstractmethod
    def process(self, data: CSVData) -> CSVData:
        """Processes the data given"""


class DataViewer(ABC):
    """Abstract base class for data viewing"""
    @abstractmethod
    def show(self, data: CSVData, source_name: str):
        """Displays the data to user"""


class CliFeature(ABC):
    """Abstract base class for features exposed in CLI"""
    @property
    @abstractmethod
    def arg_name(self) -> str: 
        """The command argument name like '--where' """

    @property
    @abstractmethod
    def arg_help(self) -> str:
        """Help text for the command"""

    @property
    def dest_name(self) -> str:
        """Converts args syntax to arg_name"""
        return self.arg_name.lstrip('-').replace('-', '_')
    
    @abstractmethod
    def create_processor(self, value: str) -> DataProcessor:
        """Creates Dataprocessor instance for command"""


class CSVDataLoader(DataLoader):
    """Loads data from a CSV file"""
    def load(self, filename: str) -> CSVData:
        """Reads a CSV file into list of dictionaries"""
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


class ConsoleViewer(DataViewer):
    """Class for displaying in the console"""
    STYLE_TEXT = Style(color='cyan')
    STYLE_TEXT_RED = Style(color='red')
    STYLE_NUMERIC = Style(color='bright_green')
    STYLE_HEADER = 'bold yellow'
    STYLE_BORDER = 'bright_black'
    STYLE_TITLE = 'bold default'

    def _determine_column_types(
            self, data: CSVData, headers: List[str]
        ) -> Dict[str, bool]:
        """Determins if the column has numeric values"""
        column_is_numeric = {}
        for header in headers:
            column_is_numeric[header] = False
            for row in data:
                value = row.get(header, '')
                if value:
                    column_is_numeric[header] = is_numeric(value)
                    break
        return column_is_numeric

    def show(self, data: CSVData, source_name: str):
        """Renders data in table format"""
        console = Console()
        if not data:
            console.print('Nothing to display', style=self.STYLE_TEXT_RED)
            return
        table = Table(
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


class WhereProcessor(DataProcessor):
    """Filters data based on a column value"""
    OPERATORS: Dict[str, Callable[[Any, Any], bool]] = {
        '=': operator.eq,
        '>': operator.gt,
        '<': operator.lt,
    }

    def __init__(self, key: str, op_str: str, value: str):
        """Initializes whereprocessor"""
        self.key = key
        if op_str not in self.OPERATORS:
            raise ValueError(
                f'Unsupported operator: {op_str}. '
                f'Use one of {List(self.OPERATORS.keys())}'
            )
        self.op = self.OPERATORS[op_str]
        self.value_str = value
        self.is_numeric_comparison = is_numeric(value)

    def process(self, data: CSVData) -> CSVData:
        """Filters the data based on the condition"""
        if not data:
            return []
        if self.key not in data[0]:
            print(
                f'Warning: key {self.key} not found', file=sys.stderr)
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


class OrderByProcessor(DataProcessor):
    """Sorts data by a specifit column"""
    def __init__(self, key: str, reverse: bool = False):
        """Initializes order processor"""
        self.key = key
        self.reverse = reverse
        
    def process(self, data: CSVData) -> CSVData:
        """Sorts the data on the key and direction"""
        if not data or self.key not in data[0]:
            if data and self.key not in data[0]:
                print(f'Warning: key {self.key} not found', file=sys.stderr)
            return data
        is_col_numeric = any(is_numeric(row.get(self.key,'')) for row in data)

        def sort_key(row: Dict[str, str]):
            """Defines the value to use for sorting each row"""
            val = row.get(self.key)
            if is_col_numeric:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return float('-inf') if self.reverse else float('inf')
            return val or ""
        return sorted(data, key=sort_key, reverse=self.reverse)
    

class AggregateProcessor(DataProcessor):
    """Calculates and aggregate value"""
    AGG_FUNCS: Dict[str, Callable[[List[float]], float]] = {
        'min': min,
        'max': max,
        'avg': statistics.mean,
    }

    def __init__(self, key: str, agg_func_str: str):
        """Initializes aggreagate processor"""
        self.key = key
        if agg_func_str not in self.AGG_FUNCS:
            raise ValueError(f'Unsupported aggregate function: {agg_func_str}')
        self.agg_func_str = agg_func_str
        self.agg_func = self.AGG_FUNCS[agg_func_str]

    def process(self, data: CSVData) -> CSVData:
        """Performs aggregate process on the specified column"""
        if not data or self.key not in data[0]:
            if data and self.key not in data[0]:
                print(
                    f'Warning: key {self.key} for aggregation not found',
                    file=sys.stderr
                )
                return []
        numeric_values = []
        for row in data:
            try:
                numeric_values.append(float(row[self.key]))
            except (ValueError, TypeError, KeyError):
                continue
        if not numeric_values:
            print(
                f'Warning: no numeric data in column {self.key} to aggregate',
                file=sys.stderr
            )
            return []
        result = self.agg_func(numeric_values)
        formatted_result = (
            f'{result:.2f}' if self.agg_func_str == 'avg' else str(result))
        return [{self.agg_func_str: formatted_result}]


class WhereFeature(CliFeature):
    """CLI feature for filtering data"""
    arg_name = '--where'
    arg_help = 'Filter with a condition like "rating>4.0"'

    def create_processor(self, value: str) -> DataProcessor:
        """Parses a condition string and creates where processor"""
        match = re.match(r'^([^=<>]+)([=<>])(.*)$', value)
        if not match:
            raise ValueError(f'Invalid --where clause: {value}')
        key, op, val = match.groups()
        return WhereProcessor(key.strip(), op.strip(), val.strip())


class OrderByFeature(CliFeature):
    """Cli feature for sorting"""
    arg_name = '--order-by'
    arg_help = 'Sorts by a column, "rating=desc"'

    def create_processor(self, value: str) -> DataProcessor:
        """Parses condition for sorting and creates order processor"""
        parts = value.split('=')
        key = parts[0].strip()
        direction = parts[1].strip().lower() if len(parts) > 1 else 'asc'
        if direction not in ['asc', 'desc']:
            raise ValueError(f'Invalid sort direction {direction}')
        return OrderByProcessor(key, direction == 'desc')
    

class AggregateFeature(CliFeature):
    """CLI feature for aggregate command"""
    arg_name = '--aggregate'
    arg_help = 'Aggregate a numeric column, like "rating=avg" or "price=min"'

    def create_processor(self, value: str) -> DataProcessor:
        """Parses aggregate string and create aggregate processor"""
        parts = value.split('=')
        if len(parts) != 2:
            raise ValueError(f'Invalid aggregate format: {value}')
        key, func = parts[0].strip(), parts[1].strip().lower()
        return AggregateProcessor(key, func)


class CSVApplication:
    """Orchestrates loading, processing and viewing CSV data"""
    def __init__(
            self,
            loader: DataLoader,
            viewer: DataViewer,
            processors: List[DataProcessor]
        ):
        """Initializes the application with its components"""
        self.loader = loader
        self.viewer = viewer
        self.processors = processors
        self.data: CSVData = []

    def run(self, source_file: str):
        """Runs the main app logic"""
        self.data = self.loader.load(source_file)
        processed_data = self.data
        for processor in self.processors:
            processed_data = processor.process(processed_data)
        self.viewer.show(processed_data, source_name=source_file)


def main():
    """Main function to parse arguments and run the application"""
    features: List[CliFeature] = [
        WhereFeature(),
        OrderByFeature(),
        AggregateFeature(),

    ]
    parser = argparse.ArgumentParser(
        description=' Script for loading, processing and showing csv data'
    )
    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Path to the file to be loaded'
    )
    for feature in features:
        parser.add_argument(feature.arg_name, type=str, help=feature.arg_help)
    args = parser.parse_args()
    args_dict = vars(args)
    processors: List[DataProcessor] = []
    try:
        for feature in features:
            arg_value = args_dict.get(feature.dest_name)
            if arg_value:
                processor = feature.create_processor(arg_value)
                processors.append(processor)
    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)
    csv_loader = CSVDataLoader()
    console_viewer = ConsoleViewer()
    app = CSVApplication(
        loader=csv_loader, viewer=console_viewer, processors=processors)
    app.run(source_file=args.file)


if __name__ == "__main__":
    main()