import os

from dotenv import load_dotenv
from loguru import logger

DEBUG = int(os.getenv("DEBUG"))
load_dotenv(override=DEBUG)  # take environment variables from .env.

# PATH #
DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))

# CONSUMER #
CONSUMER_NAME = "LiveTracking"

# AWS SETTINGS #
AWS_ARN_ROLE_CONSUMER = os.getenv("AWS_ARN_ROLE_CONSUMER", None)
SESSION_NAME = os.getenv("SESSION_NAME")
QUEUE_NAME = os.getenv("QUEUE_NAME")

# AWS QUEUE SETTINGS #
POLLING_INTERVAL = 1
MAX_NUMBER_OF_MESSAGES = 10
VISIBILITY_TIMEOUT = 30
WAIT_TIME_SECONDS = 20

# ENV & DEBUG #
ENV = os.getenv("ENV")
DEBUG = int(os.getenv("DEBUG"))

# LOGGER SETTINGS #
logger.add(
    "logs/aws_connection.log",
    rotation="10 MB",
    retention="10 days",
    level="DEBUG",
    enqueue=True,
)

# POSTGRES SETTINGS #
POSTGRES_URI = os.getenv("POSTGRES_URI")
