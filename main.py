import argparse
import csv
import sys
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from rich.console import Console
from rich.table import Table
from rich.style import Style


def generate_title_from_filename(filename: str) -> str:
    base_name = os.path.basename(filename)
    title, _ = os.path.splitext(base_name)
    title = title.replace('_', ' ').title()
    return title


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

    def show(self, data: List[Dict[str, Any]], source_name: str):
        if not data:
            print('Nothing to display')
            return
        
        console = Console()

        text_style = Style(color='cyan')
        numeric_style = Style(color='green')

        table_title = generate_title_from_filename(source_name)

        table = Table(
            title=table_title,
            show_header=True,
            header_style='bold magenta',
            border_style='dim',
        )

        headers = data[0].keys()

        for header in headers:
            is_numeric = data[0][header].isnumeric() if data and data[0].get(header) else False

            table.add_column(
                header,
                justify='right' if is_numeric else 'left',
                style=numeric_style if is_numeric else text_style,
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