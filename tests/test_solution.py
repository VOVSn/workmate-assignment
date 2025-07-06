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


def test_where_processor_numeric(sample_csv_data):
    processor = WhereProcessor(key='rating', op_str='>', value='4.7')
    result = processor.process(sample_csv_data)
    assert len(result) == 2
    names = {row['name'] for row in result}
    assert names == {'iphone 15 pro', 'galaxy s23 ultra'}


def test_where_processor_string(sample_csv_data):
    processor = WhereProcessor(key='brand', op_str='=', value='apple')
    result = processor.process(sample_csv_data)
    assert len(result) == 4
    assert all(row['brand'] == 'apple' for row in result)


def test_order_by_processor_numeric_desc(sample_csv_data):
    processor = OrderByProcessor(key='price', reverse=True)
    result = processor.process(sample_csv_data)
    assert result[0]['name'] == 'galaxy s23 ultra'
    assert result[1]['name'] == 'iphone 15 pro'
    assert result[-1]['name'] == 'redmi 10c'


def test_order_by_processor_string_asc(sample_csv_data):
    processor = OrderByProcessor(key='name', reverse=False)
    result = processor.process(sample_csv_data)
    assert result[0]['name'] == 'galaxy a54'
    assert result[1]['name'] == 'galaxy s23 ultra'
    assert result[-1]['name'] == 'redmi note 12'


def test_aggregate_processor_avg(sample_csv_data):
    processor = AggregateProcessor(key="rating", agg_func_str='avg')
    result = processor.process(sample_csv_data)
    assert len(result) == 1
    assert float(result[0]['avg']) == pytest.approx(4.49)


def test_aggregate_processor_max(sample_csv_data):
    processor = AggregateProcessor(key="price", agg_func_str='max')
    result = processor.process(sample_csv_data)
    assert result == [{'max': '1199.0'}]


def test_aggregate_processor_min(sample_csv_data):
    processor = AggregateProcessor(key="price", agg_func_str='min')
    result = processor.process(sample_csv_data)
    assert result == [{'min': '149.0'}]


def test_main_integration(monkeypatch, capsys, sample_csv_text):
    argv = [
        'main.py',
        '--file', 'anyfile.csv',
        '--where', 'price<500',
        '--order-by', 'rating=desc',
    ]
    monkeypatch.setattr('sys.argv', argv)
    mock_open = patch('builtins.open', return_value=io.StringIO(sample_csv_text))
    with mock_open:
        main()
    stdout = capsys.readouterr().out
    assert 'poco x5 pro' in stdout
    assert 'iphone se' in stdout
    assert stdout.find('poco x5 pro') < stdout.find('iphone se')
    assert 'iphone 15 pro' not in stdout


def test_main_aggregate_integration(monkeypatch, capsys, sample_csv_text):
    argv = [
        'main.py',
        '--file', 'anyfile.csv',
        '--aggregate', 'price=avg'
    ]
    monkeypatch.setattr('sys.argv', argv)
    with patch('builtins.open', return_value=io.StringIO(sample_csv_text)):
        main()
    stdout = capsys.readouterr().out
    assert '602.00' in stdout
    assert 'avg' in stdout
