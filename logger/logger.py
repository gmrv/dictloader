import logging
import sys


class Logger:

    def __init__(self, name='default'):
        self.log = logging.getLogger(name)
        self.log.setLevel(logging.DEBUG)
        frmtr = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

        output_file_handler = logging.FileHandler(f'logs/{name}.log')
        output_file_handler.setFormatter(frmtr)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(frmtr)

        self.log.addHandler(output_file_handler)
        self.log.addHandler(stdout_handler)