# -*- coding: utf-8 -*-
"""Helps you to handle your local files. Will raise Error if Redis creds or 1 datafolder not in your yaml."""
import pathlib
from ruamel.yaml import YAML

yaml = YAML()
yaml.default_flow_style = False


class Helper():
    def __init__(self):
        # this is the recommended path we read your fastcounting.yaml file from.
        self.home = pathlib.Path.home()
        self.path_to_config = pathlib.Path(f'{self.home}/.config/fastcounting.yaml')
        self.configs = self.read_config()
        
    def read_config(self):
        """Example config is in root in your cloned repo, named fastcounting.yaml."""
        if self.path_to_config is None:
            raise ValueError('path_to_config canot be empty')

        # we read the main config file
        with open(self.path_to_config, 'r') as f:
            configs = yaml.load(f)
        return configs

    def datafolder(self, month):
        """
        Month has format of 'YYYY-MM' and 'YYYY-13' for complete year.
        It is also the expected foldername.
        """
        relative_path = self.configs['rel_to_home_datafolder']
        absolute_path = self.configs['absolut_datafolder']

        if relative_path:
            return pathlib.Path(self.home / relative_path / month)
        elif absolute_path:
            return pathlib.Path(absolute_path / month)
        else:
            raise ValueError('both paths to datafolder cannot be empty')

    @property
    def rediscred(self):
        """For the start and no open ports. Just put the default cred in your yaml."""
        rediscred = {
            'host': self.configs['Redis']['host'],
            'port': self.configs['Redis']['port']}
        redispw = self.configs['Redis']['password']
        if redispw:
            rediscred.update({'password': redispw})
        return rediscred