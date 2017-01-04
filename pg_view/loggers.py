import logging

logger = logging.getLogger(__name__)


def setup_loggers(options):
    global logger

    _setup_basic_config(options)
    logger.setLevel((logging.INFO if options.verbose else logging.ERROR))
    log_stderr = logging.StreamHandler()
    logger.addHandler(log_stderr)
    return log_stderr


def _setup_basic_config(options):
    if options.log_file:
        LOG_FILE_NAME = options.log_file
        # truncate the former logs
        with open(LOG_FILE_NAME, 'w'):
            pass
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s', filename=LOG_FILE_NAME)
    else:
        logging.basicConfig(format='%(levelname)s: %(asctime)-15s %(message)s')
