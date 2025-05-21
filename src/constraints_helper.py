import logging


class ConstraintsHelper:
    def __init__(self, config):
        self.constraints = config
        self.logger = logging.getLogger()

    def get_constraints(self):
        return self.constraints
