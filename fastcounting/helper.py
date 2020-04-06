# -*- coding: utf-8 -*-
"""Helps you to handle your local files."""
import pathlib
from ruamel.yaml import YAML

yaml = YAML()
yaml.default_flow_style = False

homedir = pathlib.Path.home()
path_to_config = pathlib.Path(f'{homedir}/.config/fastcounting.yaml')

if path_to_config is None:
    raise ValueError('path_to_config canot be empty')

# we read the main config file
with open(path_to_config, 'r') as f:
    configs = yaml.load(f)

# datafolder = setup.configs['datafolder']

rediscred = {
    'host': configs['Redis']['host'],
    'port': configs['Redis']['port']}
redispw = configs['Redis']['password']
if redispw:
    rediscred.update({'password': redispw})
