import io
import logging
from unittest.mock import patch

import pytest

from mrp_system.runtime.console_logging import ContrastFormatter, configure_cli_logging, supports_color


def record(level=logging.INFO, message='Loaded %s rows', args=(12,), exc_info=None):
    return logging.LogRecord('mrp', level, 'etl.py', 1, message, args, exc_info)


def test_plain_logging_has_aligned_labels_and_no_escape_codes():
    formatter = ContrastFormatter(color=False)
    info = formatter.format(record())
    warning = formatter.format(record(logging.WARNING))
    assert '  INFO     | Loaded 12 rows' in info
    assert '  WARNING  | Loaded 12 rows' in warning
    assert '\033[' not in info
    assert info.index('|') == warning.index('|')


@pytest.mark.parametrize('level,color', [(logging.INFO, '\033[1;96m'),
                                        (logging.WARNING, '\033[1;93m'),
                                        (logging.ERROR, '\033[1;91m')])
def test_severity_colors_and_reset(level, color):
    line = ContrastFormatter(color=True).format(record(level))
    assert color in line
    assert 'Loaded 12 rows' in line
    assert line.endswith('\033[0m')


def test_formatter_keeps_tracebacks():
    import sys
    try:
        raise ValueError('missing column')
    except ValueError:
        log = record(logging.ERROR, message='Validation failed', args=(), exc_info=sys.exc_info())
    text = ContrastFormatter(color=True).format(log)
    assert 'Validation failed' in text
    assert 'Traceback' in text
    assert 'ValueError: missing column' in text


def test_redirection_disables_color():
    assert not supports_color(io.StringIO())


@pytest.mark.parametrize('env', [{'NO_COLOR': ''}, {'TERM': 'dumb'}])
def test_no_color_and_dumb_terminal_disable_color(env):
    stream = io.StringIO()
    stream.isatty = lambda: True
    with patch.dict('os.environ', env, clear=True):
        assert not supports_color(stream)


def test_interactive_console_uses_color_when_supported():
    stream = io.StringIO()
    stream.isatty = lambda: True
    with patch.dict('os.environ', {}, clear=True), \
         patch('mrp_system.runtime.console_logging._enable_windows_color', return_value=True):
        assert supports_color(stream)


def test_configuration_uses_plain_formatter_for_a_file_stream():
    with patch('mrp_system.runtime.console_logging.logging.basicConfig') as configure:
        configure_cli_logging(io.StringIO())
    assert configure.call_args.kwargs['level'] == logging.INFO
    handler = configure.call_args.kwargs['handlers'][0]
    assert isinstance(handler.formatter, ContrastFormatter)
    assert not handler.formatter.color
