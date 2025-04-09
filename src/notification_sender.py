import random
import time

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    TOPIC_NAME,
    logger,
)
from scripts.message import Message
from setup import initialize_aws_setup, initialize_postgres_table
from utils import send_message_to_topic


def producer(sns_client, topic_arn):
    try:
        counter = 1
        while counter<20:
            message_body = f"Test message number: {counter}"
            subject = "Test Subject"
            message_attributes = {
                'AttributeKey': {
                    'DataType': 'String',
                    'StringValue': 'AttributeValue'
                }
            }
            send_message_to_topic(sns_client, topic_arn, message_body, subject, message_attributes)
            counter += 1
            # Sleep for a while to avoid sending too many messages in a short time
            time.sleep(random.randint(1, 20))
    except Exception as e:
        logger.error(f"Error sending message to topic: {e}")

if __name__ == "__main__":

    postgres_client = Message(db_uri=POSTGRES_URI)

    sns_client, _, topic_arn, _ = initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME, topic_name=TOPIC_NAME, queue_name=QUEUE_NAME)
    initialize_postgres_table(postgres_client=postgres_client)

    producer(sns_client=sns_client, topic_arn=topic_arn)
