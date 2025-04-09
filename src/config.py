import os

from dotenv import load_dotenv
from loguru import logger

DEBUG = int(os.getenv("DEBUG"))
load_dotenv(override=DEBUG)  # take environment variables from .env.

# AWS Role
AWS_ARN_ROLE_CONSUMER = os.getenv("AWS_ARN_ROLE_CONSUMER")
SESSION_NAME = os.getenv("SESSION_NAME")
TOPIC_NAME = os.getenv("TOPIC_NAME")
QUEUE_NAME = os.getenv("QUEUE_NAME")

# ENV & DEBUG
ENV=os.getenv("ENV")
DEBUG=int(os.getenv("DEBUG"))

# Configure loguru loggers
logger.add("logs/aws_connection.log", rotation="10 MB", retention="10 days", level="DEBUG", enqueue=True)

# Global Postgres connection string
POSTGRES_URI = "postgres://admin:password@localhost:5432/mydatabase"

# Global SQS polling interval
POLLING_INTERVAL = 1  # seconds
