import io
import pytest
from unittest.mock import patch

from main import (
    is_numeric,
    CSVDataLoader,
    WhereProcessor,
    OrderByProcessor,
    AggregateProcessor,
    main,
)

@pytest.fixture
def sample_csv_text():
    return """name,brand,price,rating
iphone 15 pro,apple,999,4.9
galaxy s23 ultra,samsung,1199,4.8
redmi note 12,xiaomi,199,4.6
iphone 14,apple,799,4.7
galaxy a54,samsung,349,4.2
poco x5 pro,xiaomi,299,4.4
iphone se,apple,429,4.1
galaxy z flip 5,samsung,999,4.6
redmi 10c,xiaomi,149,4.1
iphone 13 mini,apple,599,4.5
"""


@pytest.fixture
def sample_csv_data(sample_csv_text):
    loader = CSVDataLoader()
    with patch('builtins.open', return_value=io.StringIO(sample_csv_text)):
        return loader.load('anyfile.csv')
    

def test_is_numeric():
    assert is_numeric('123') is True
    assert is_numeric('123.45') is True
    assert is_numeric('-10') is True
    assert is_numeric('abc') is False
    assert is_numeric(None) is False
    assert is_numeric("") is False


def test_csv_data_loader_success(sample_csv_text):
    loader = CSVDataLoader()
    with patch('builtins.open', return_value=io.StringIO(sample_csv_text)):
        data = loader.load('anyfile.csv')
        assert len(data) == 10
        assert data[0]['name'] == 'iphone 15 pro'
        assert data[0]['price'] == '999'


def test_csv_data_loader_empty_file():
    loader = CSVDataLoader()
    with patch('builtins.open', return_value=io.StringIO('')):
        data = loader.load('anyfile.csv')
        assert data == []