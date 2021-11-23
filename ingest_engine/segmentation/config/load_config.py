import yaml
import os

directory_path = os.path.dirname(os.path.abspath(__file__))
new_path = os.path.join(directory_path, "config.yml")


# Config
def load_config():
    config = yaml.safe_load(open(new_path))
    return config
