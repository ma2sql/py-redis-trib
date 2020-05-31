from itertools import (
    groupby
)
import sys
from .xprint import xprint


def group_by(iterable, key):
    return {k: list(v) for k, v in groupby(sorted(iterable, key=key), key=key)}


def chunk(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


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
        xprint(f"> {question}{prompt}", file=sys.stderr, end='')
        choice = input().strip().lower()
        if default is not None and choice == '':
            return default
        elif choice in _valid.keys():
            return _valid[choice]
        else:
            xprint(f"> Please respond with 'yes' or 'no' (or 'y' or 'n').", file=sys.stderr)
 

def summarize_slots(slots):
    _temp_slots = []
    for slot in sorted(slots):
        if not _temp_slots or _temp_slots[-1][-1] != (slot-1): 
            _temp_slots.append([])
        _temp_slots[-1][1:] = [slot]
    return ','.join(map(lambda slot_exp: '-'.join(map(str, slot_exp)), _temp_slots)) 
   
