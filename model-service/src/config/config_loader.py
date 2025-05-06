import yaml
from typing import Dict

class ConfigLoader:
    @staticmethod
    def load_yaml_config(path: str) -> Dict:
        """Load YAML configuration from file."""
        with open(path, 'r') as f:
            return yaml.safe_load(f)