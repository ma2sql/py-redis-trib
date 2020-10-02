import sys
from termcolor import colored, cprint

_LOG_OK = 'OK'
_LOG_FAIL = 'FAIL'
_LOG_ERROR = 'ERROR'
_LOG_VERBOSE = 'VERBOSE'
_LOG_INFO = 'INFO'
_LOG_WARNING = 'WARNING'
_LOG_PRINT = ''

LOG_LEVEL_VERBOSE = 1 
LOG_LEVEL_INFO = 2 
LOG_LEVEL_WARNING = 3
LOG_LEVEL_ERROR = 4 
LOG_LEVEL_NONE = 99

_LOG_LEVELS = [LOG_LEVEL_VERBOSE, LOG_LEVEL_INFO, LOG_LEVEL_WARNING,
               LOG_LEVEL_ERROR, LOG_LEVEL_NONE]

# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# https://en.wikipedia.org/wiki/ANSI_escape_code#3/4_bit
_LOG_LEVELS_COLORS = {
    _LOG_VERBOSE: (LOG_LEVEL_VERBOSE, None, []),
    _LOG_INFO: (LOG_LEVEL_INFO, None, ["bold"]),
    _LOG_WARNING: (LOG_LEVEL_WARNING, "yellow", ["bold"]),
    _LOG_OK: (LOG_LEVEL_ERROR, "green",  ["bold"]),
    _LOG_FAIL: (LOG_LEVEL_ERROR, "red", ["bold"]),
    _LOG_ERROR: (LOG_LEVEL_ERROR, "red", ["bold"]),
    _LOG_PRINT: (LOG_LEVEL_NONE, None, []),
}


class XPrint:

    def __init__(self, level=LOG_LEVEL_INFO):
        self._log_level = level

    def __call__(self, *msg, **kwargs):
        self._xprint(_LOG_PRINT, *msg, **kwargs)

    def set_loglevel(self, level):
        if level not in _LOG_LEVELS:
            raise ValueError('Unknown log level')
    
        self._log_level = level

    def _xprint(self, header, *msg, **kwargs):
        level, color, attrs = _LOG_LEVELS_COLORS.get(header) or (None, None, None)
        if level and self._log_level > level:
            return
   
        ignore_header = kwargs.get('ignore_header')
        if ignore_header is not None:
            del kwargs['ignore_header']

        _header = f"[{header}] " if header and not ignore_header else "" 
        _msg = ' '.join(map(str, msg))
        cprint(f"{_header}{_msg}", color, attrs=attrs, **kwargs)
                   
    def verbose(self, *msg, **kwargs):
        self._xprint(_LOG_VERBOSE, *msg, **kwargs) 
     
    def info(self, *msg, **kwargs):
        self._xprint(_LOG_INFO, *msg, **kwargs)
    
    def warning(self, *msg, **kwargs):
        self._xprint(_LOG_WARNING, *msg, **kwargs) 
    
    def error(self, *msg, **kwargs):
        self._xprint(_LOG_ERROR, *msg, **kwargs) 
    
    def ok(self, *msg, **kwargs):
        self._xprint(_LOG_OK, *msg, **kwargs)
    
    def fail(self, *msg, **kwargs):
        self._xprint(_LOG_FAIL, *msg, **kwargs)

    def quiet_or_not(self, quiet):
        if quiet:
            return lambda *msg, **kwargs: None
        return self._xprint


xprint = XPrint()


__all__ = ['xprint', 
           'LOG_LEVEL_ERROR', 'LOG_LEVEL_INFO', 'LOG_LEVEL_NONE',
           'LOG_LEVEL_VERBOSE', 'LOG_LEVEL_WARNING'] 
