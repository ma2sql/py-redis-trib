from itertools import (
    groupby
)
import sys
from termcolor import colored, cprint

# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# https://en.wikipedia.org/wiki/ANSI_escape_code#3/4_bit
_MESSAGE_COLOR = {
    ">>>": (None,     ["bold"]),
    "[ER": ("red",    ["bold"]),
    "[WA": ("red",    ["bold"]),
    "[OK": ("green",  []),
    "[FA": ("yellow", []),
    "***": ("yellow", []),
}

def xprint(msg):
    header = msg[:3]
    color, attrs = _MESSAGE_COLOR.get(header) or (None, None)
    cprint(msg, color, attrs=attrs)


def group_by(iterable, key):
    return {k: list(v) for k, v in groupby(sorted(iterable, key=key), key=key)}
