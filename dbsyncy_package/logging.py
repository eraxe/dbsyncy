# dbsyncy_package/logging.py
import logging

def setup_logging():
    logging.basicConfig(filename='sync.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
