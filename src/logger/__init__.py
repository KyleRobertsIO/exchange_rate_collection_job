import logging
import sys
import json
import os
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):

        extra = getattr(record, "__dict__", {})
        json_record = {
            "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Level": getattr(record, "levelname", None),
            "File": getattr(record, "pathname", None),
            "Line": getattr(record, "lineno", None),
            "Msg": getattr(record, "msg", None),
            "additional_detail": extra.get("additional_detail"),
        }
        return json.dumps(json_record)
    
class StandardOutColorFormatter(logging.Formatter):    
    def _get_formats(self) -> dict:
        grey = "\x1b[38;20m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        pink = '\x1b[35m'
        blue = '\x1b[34m'
        reset = "\x1b[0m"
        format = "[%(levelname)s | %(asctime)s] %(message)s" 
        FORMATS = {
            logging.DEBUG: blue + format + reset,
            logging.INFO: blue + format + reset,
            logging.WARNING: yellow + format + reset,
            logging.ERROR: red + format + reset,
            logging.CRITICAL: bold_red + format + reset,
            logging.FATAL: bold_red + format + reset
        }
        return FORMATS

    def format(self, record):
        log_fmt = self._get_formats().get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def build_stdout_logging_handler() -> logging.StreamHandler:
    formatter = StandardOutColorFormatter()
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    return stdout_handler

def build_json_logging_handler(file_name: str) -> logging.FileHandler:
    formatter = JsonFormatter()
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    json_handler = logging.FileHandler(filename = file_name)
    json_handler.setFormatter(formatter)
    return json_handler