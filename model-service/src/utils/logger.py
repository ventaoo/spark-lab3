import logging
import os
from typing import Optional

class AppLogger:
    def __init__(self, name: str = "food_clustering", log_file: Optional[str] = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
        
        # Console handler
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        self.logger.addHandler(console)
        
        # File handler if log file is specified
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
    def get_logger(self) -> logging.Logger:
        return self.logger