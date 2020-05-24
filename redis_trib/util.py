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


def query_yes_no(question, default=None):
    '''ref: http://code.activestate.com/recipes/577058/'''
    _valid = {'yes': True, 'y': True, 'no': False, 'n': False}

    y, n = 'y', 'n'
    if default is True:
        y, default = 'Y', True
    elif default is False:
        n, default = 'N', False

    prompt = f" [{y}/{n}] "

    while True:
        print(f"> {question}{prompt}", file=sys.stderr, end='')
        choice = input().strip().lower()
        if default is not None and choice == '':
            return default
        elif choice in _valid.keys():
            return _valid[choice]
        else:
            print(f"> Please respond with 'yes' or 'no' (or 'y' or 'n').", file=sys.stderr)

