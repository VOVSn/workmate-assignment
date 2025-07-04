import argparse
import csv
import sys
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class DataLoader(ABC):
    def load(self, source: str) -> List[Dict[str, Any]]:
        pass


class DataViewer(ABC):
    def show(self, data: List[Dict[str, Any]]):
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

    def show(self, data: List[Dict[str, Any]]):
        if not data:
            print('Nothing to display')
            return
        
        headers = data[0].keys()
        column_widths = {key: len(key) for key in headers}
        for row in data:
            for key, val in row.items():
                if len(val) > column_widths[key]:
                    column_widths[key] = len(val)
        header_line = ' | '.join(
            header.ljust(column_widths[header]) for header in headers)


        print(header_line)

        separator_line = '-+-'.join('-' * column_widths[header] for header in headers)
        print(separator_line)

        for row in data:
            row_line = ' | '.join(str(row[header]).ljust(column_widths[header]) for header in headers)
            print(row_line)


class CSVApplication:
    def __init__(self, loader: DataLoader, viewer: DataViewer):
        self.loader = loader
        self.viewer = viewer
        self.data: List[Dict[str, Any]] = []

    def run(self, source_file: str):
        print(f'Loading data from: {source_file}')
        self.data = self.loader.load(source_file)

        self.viewer.show(self.data)
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