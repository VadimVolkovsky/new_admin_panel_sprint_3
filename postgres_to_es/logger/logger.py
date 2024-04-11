import logging
import sys


class CustomLogger(logging.Logger):
    def __init__(self, name: str):
        super().__init__(name)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.addHandler(console_handler)
        self.setLevel(level='INFO')
