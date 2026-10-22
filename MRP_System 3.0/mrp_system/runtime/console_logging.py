"""High-contrast CLI logging with plain text for files and unsupported consoles."""
import logging
import os


RESET = '\033[0m'
GRAY = '\033[90m'
BOLD = '\033[1m'
CYAN = '\033[1;96m'
YELLOW = '\033[1;93m'
RED = '\033[1;91m'


def _enable_windows_color(stream):
    """Enable ANSI processing on Windows consoles without a new dependency."""
    import ctypes
    from ctypes import wintypes
    import msvcrt

    try:
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        handle = wintypes.HANDLE(msvcrt.get_osfhandle(stream.fileno()))
        mode = wintypes.DWORD()
        return bool(kernel.GetConsoleMode(handle, ctypes.byref(mode))
                    and kernel.SetConsoleMode(handle, mode.value | 0x0004))
    except (OSError, ValueError, AttributeError):
        return False


def supports_color(stream):
    if 'NO_COLOR' in os.environ or os.environ.get('TERM') == 'dumb':
        return False
    if not getattr(stream, 'isatty', lambda: False)():
        return False
    return os.name != 'nt' or _enable_windows_color(stream)


class ContrastFormatter(logging.Formatter):
    def __init__(self, color=False):
        super().__init__('%(asctime)s  %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.color = color

    def format(self, record):
        plain = super().format(record)
        if not self.color:
            return plain
        _, _, message = plain.partition(' | ')
        if record.levelno >= logging.ERROR:
            level_color = message_color = RED
        elif record.levelno >= logging.WARNING:
            level_color = message_color = YELLOW
        elif record.levelno >= logging.INFO:
            level_color, message_color = CYAN, BOLD
        else:
            level_color = message_color = GRAY
        return (f'{GRAY}{self.formatTime(record, self.datefmt)}{RESET}  '
                f'{level_color}{record.levelname:<8}{RESET} | '
                f'{message_color}{message}{RESET}')


def configure_cli_logging(stream=None):
    handler = logging.StreamHandler(stream)
    handler.setFormatter(ContrastFormatter(color=supports_color(handler.stream)))
    # Like basicConfig's previous behavior, do not replace an embedding app's handlers.
    logging.basicConfig(level=logging.INFO, handlers=[handler])
