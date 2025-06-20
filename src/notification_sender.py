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
from scripts.postgres import PostgresClient
from setup import (
    initialize_aws_setup,
    initialize_postgres_client,
)
from simple_message import SimpleMessage
from utils import send_message_to_topic


def initialize_producer(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    topic_name: str = TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
    postgres: PostgresClient = SimpleMessage,
    db_uri: str = POSTGRES_URI,
):
    """
    Initialize the producer by setting up the AWS connection and PostgreSQL table.
    """
    postgres_client = initialize_postgres_client(
        postgres=postgres,
        db_uri=db_uri,
    )

    sns_client, _, topic_arn, _ = initialize_aws_setup(
        role=role,
        session_name=session_name,
        topic_name=topic_name,
        queue_name=queue_name,
    )
    return postgres_client, sns_client, topic_arn


def producer(sns_client, topic_arn) -> None:
    try:
        counter = 1
        while counter < 100:
            message_body = f"Test message number: {counter}"
            subject = "Test Subject"
            message_attributes = {
                "AttributeKey": {"DataType": "String", "StringValue": "AttributeValue"}
            }
            send_message_to_topic(
                sns_client, topic_arn, message_body, subject, message_attributes
            )
            counter += 1

            if counter % 20 == 0:
                logger.info(f"Sent {counter} messages, sleeping for a while...")
                time.sleep(random.randint(1, 20))
    except Exception as e:
        logger.error(f"Error sending message to topic: {e}")


if __name__ == "__main__":
    _, sns_client, topic_arn = initialize_producer(postgres=SimpleMessage)
    producer(sns_client=sns_client, topic_arn=topic_arn)
