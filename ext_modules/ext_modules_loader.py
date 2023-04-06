import json
import sys
import os


def load_ext_modules():
    # Get the absolute path to the config file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'ext_modules.json')

    # Open the config file and load the JSON data
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Get the list of module directories from the config file
    module_dirs = config.get('module_dirs', [])

    # Add the module directories to sys.path
    for directory in module_dirs:
        abs_dir = os.path.abspath(os.path.join(script_dir, directory))
        sys.path.append(abs_dir)

