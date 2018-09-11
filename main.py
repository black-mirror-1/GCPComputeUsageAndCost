import logging
import os,sys
import init_logger

if __name__ == '__main__':
    init_logger.setup(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'conf', 'logging.yaml'))
logging.getLogger('sandeep')
logging.info("Sample Info Message")
logging.warning("Sample warn message")
logging.error("Sample error message")
logging.debug("Sample debug message")