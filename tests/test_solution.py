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
    """Provides sample csv string for mock testing"""
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
    """Provides parsed csv data from the sample text"""
    loader = CSVDataLoader()
    with patch('builtins.open', return_value=io.StringIO(sample_csv_text)):
        return loader.load('anyfile.csv')
    

@pytest.mark.parametrize(
        'value, expected',
        [
            ('123', True),
            ('123.45', True),
            ('-10', True),
            ('abc', False),
            (None, False),
            ('', False),

        ],
)
def test_is_numeric(value, expected):
    """Tests the is_numeric function with parametrization"""
    assert is_numeric(value) is expected


def test_csv_data_loader_success(sample_csv_data):
    """Tests that the csv loader ssuccessfully parses data"""
    assert len(sample_csv_data) == 10
    assert sample_csv_data[0]['name'] == 'iphone 15 pro'
    assert sample_csv_data[0]['price'] == '999'


def test_csv_data_loader_empty_file():
    """Tests empty file"""
    loader = CSVDataLoader()
    with patch('builtins.open', return_value=io.StringIO('')):
        data = loader.load('anyfile.csv')
        assert data == []



@pytest.mark.parametrize(
    'key, op_str, value, expected_names',
    [
        ('rating', '>', '4.7', {'iphone 15 pro', 'galaxy s23 ultra'}),
        ('brand', '=', 'apple', {
            'iphone 15 pro', 'iphone 14', 'iphone se', 'iphone 13 mini'}),
        ('price', '<', '200', {'redmi note 12', 'redmi 10c'}),
    ],
)
def test_where_processor(sample_csv_data, key, op_str, value, expected_names):
    """Tests whereprocessor for numeric and strings"""
    processor = WhereProcessor(key=key, op_str=op_str, value=value)
    result = processor.process(sample_csv_data)
    assert len(result) == len(expected_names)
    assert {row['name'] for row in result} == expected_names


@pytest.mark.parametrize(
    'key, reverse, expected_first, expected_last',
    [
        ('price', True, 'galaxy s23 ultra', 'redmi 10c'),
        ('rating', False, 'iphone se', 'iphone 15 pro'),
        ('name', False, 'galaxy a54', 'redmi note 12'),
    ],
)
def test_order_by_processor(
    sample_csv_data, key, reverse, expected_first, expected_last):
    """Tests orderby processor for sorting"""
    processor = OrderByProcessor(key=key, reverse=reverse)
    result = processor.process(sample_csv_data)
    assert result[0]['name'] == expected_first
    assert result[-1]['name'] == expected_last


@pytest.mark.parametrize(
    'key, func_str, expected_result',
    [
        ('rating', 'avg', [{'avg': '4.49'}]),
        ('price', 'max', [{'max': '1199.0'}]),
        ('price', 'min', [{'min': '149.0'}]),
    ],
)
def test_aggregate_processor(sample_csv_data, key, func_str, expected_result):
    """Tests aggregate processor with parametrize"""
    processor = AggregateProcessor(key=key, agg_func_str=func_str)
    result = processor.process(sample_csv_data)
    if func_str == 'avg':
        assert result[0].get('avg') is not None
        assert float(result[0]['avg']) == pytest.approx(float(expected_result[0]['avg']))
    else:
        assert result == expected_result


def test_main_integration(monkeypatch, sample_csv_text):
    """Tests main application with viewer mocking"""
    argv = [
        'main.py',
        '--file', 'anyfile.csv',
        '--where', 'price<500',
        '--order-by', 'rating=desc',
    ]
    monkeypatch.setattr('sys.argv', argv)
    with patch('main.ConsoleViewer.show') as mock_show:
        with patch('builtins.open', return_value=io.StringIO(sample_csv_text)):
            main()
    mock_show.assert_called_once()
    processed_data = mock_show.call_args[0][0]
    names = [row['name'] for row in processed_data]
    expected_order = [
        'redmi note 12', 'poco x5 pro', 'galaxy a54', 'iphone se', 'redmi 10c'
    ]
    assert names == expected_order


def test_main_aggregate_integration(monkeypatch, capsys, sample_csv_text):
    """Tests teh main flow with aggregate"""
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
