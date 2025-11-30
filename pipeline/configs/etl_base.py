import yaml
from pathlib import Path

class EtlBase:
    
    def __init__(self):
        # Get the configs directory relative to this file
        config_dir = Path(__file__).parent
        config_file = config_dir / 'config_local.yaml'
        
        # Load directly from filesystem (always reads latest version)
        with open(config_file, 'r') as f:
            self.appConfig = yaml.load(f, Loader=yaml.FullLoader)


