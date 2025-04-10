import os

from dotenv import load_dotenv
from loguru import logger

DEBUG = int(os.getenv("DEBUG"))
load_dotenv(override=DEBUG)  # take environment variables from .env.

# AWS Role
AWS_ARN_ROLE_CONSUMER = os.getenv("AWS_ARN_ROLE_CONSUMER", None)
SESSION_NAME = os.getenv("SESSION_NAME")
TOPIC_NAME = os.getenv("TOPIC_NAME")
TOPIC_ARN = os.getenv("TOPIC_ARN")
QUEUE_NAME = os.getenv("QUEUE_NAME")

# ENV & DEBUG
ENV=os.getenv("ENV")
DEBUG=int(os.getenv("DEBUG"))

# Configure loguru loggers
logger.add("logs/aws_connection.log", rotation="10 MB", retention="10 days", level="DEBUG", enqueue=True)

# Global Postgres connection string
POSTGRES_URI = os.getenv("POSTGRES_URI")

# Global SQS polling interval
POLLING_INTERVAL = 1  # seconds
