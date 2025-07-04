from abc import ABC, abstractmethod
import argparse
import csv
import os
import sys
from typing import List, Dict, Any

from rich.console import Console
from rich.table import Table
from rich.style import Style


def generate_title_from_filename(filename: str) -> str:
    base_name = os.path.basename(filename)
    title, _ = os.path.splitext(base_name)
    return title.replace('_', ' ').title()


def is_float_or_int(value: str) -> bool:
    if not value:
        return False
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


class DataLoader(ABC):
    @abstractmethod
    def load(self, source: str) -> List[Dict[str, Any]]:
        pass


class DataViewer(ABC):
    @abstractmethod
    def show(self, data: List[Dict[str, Any]], source_name: str):
        pass


class CSVDataLoader(DataLoader):
    def load(self, filename: str) -> List[Dict[str, Any]]:
        try:
            with open(filename, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                return list(reader)
        except FileNotFoundError:
            print(f'Error: file {filename} was not found', file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f'Error occurred reading file or wrong format ({filename})')
            sys.exit(1)


class ConsoleViewer(DataViewer):

    STYLE_TEXT = Style(color='cyan')
    STYLE_NUMERIC = Style(color='bright_green')
    STYLE_HEADER = 'bold yellow'
    STYLE_BORDER = 'bright_black'
    STYLE_TITLE = 'bold default'

    def show(self, data: List[Dict[str, Any]], source_name: str):
        if not data:
            print('Nothing to display', style='yellow')
            return
        
        console = Console()
        table_title = generate_title_from_filename(source_name)

        table = Table(
            title=table_title,
            show_header=True,
            header_style=self.STYLE_HEADER,
            border_style=self.STYLE_BORDER,
            title_style=self.STYLE_TITLE,
        )

        headers = data[0].keys()
        first_row = data[0]

        for header in headers:
            cell_value = first_row.get(header, '')
            is_numeric = is_float_or_int(cell_value)

            table.add_column(
                header,
                justify='right' if is_numeric else 'left',
                style=self.STYLE_NUMERIC if is_numeric else self.STYLE_TEXT,
                no_wrap=False,
            )


        for row in data:
            table.add_row(*row.values())

        console.print(table)


class CSVApplication:
    def __init__(self, loader: DataLoader, viewer: DataViewer):
        self.loader = loader
        self.viewer = viewer
        self.data: List[Dict[str, Any]] = []

    def run(self, source_file: str):
        print(f'Loading data from: {source_file}')
        self.data = self.loader.load(source_file)

        self.viewer.show(self.data, source_name=source_file)
        print(f'Successfully displayed {len(self.data)} records')

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
    args = parser.parse_args()

    csv_loader = CSVDataLoader()
    console_viewer = ConsoleViewer()

    app = CSVApplication(loader=csv_loader, viewer=console_viewer)
    app.run(source_file=args.file)

if __name__ == "__main__":
    main()