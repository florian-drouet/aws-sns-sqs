from loguru import logger
import sys
import os

# Initialize SNS client
AWS_ARN_ROLE_CONSUMER_TC_LIVE = "arn:aws:iam::283956190505:role/iam-role-consumer-tc-live"
ENV=os.getenv("ENV")
DEBUG=os.getenv("DEBUG")

# Configure loguru loggers
logger.add("logs/aws_connection.log", rotation="10 MB", retention="10 days", level="DEBUG", enqueue=True)
#logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
