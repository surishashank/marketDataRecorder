import json
import sys


def load_ext_modules():
    # Open the config file and load the JSON data
    with open('ext_modules/ext_modules.json', 'r') as f:
        config = json.load(f)

    # Get the list of module directories from the config file
    module_dirs = config.get('module_dirs', [])

    # Add the module directories to sys.path
    for directory in module_dirs:
        sys.path.insert(0, directory)
