import logging
import sys

root_logger = logging.getLogger()
root_logger.setLevel("INFO")

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
stream_handler = logging.StreamHandler(stream=sys.stderr)
stream_handler.setFormatter(formatter)
root_logger.addHandler(stream_handler)

logger = logging.getLogger(__name__)
logger.info("Configured logger")
