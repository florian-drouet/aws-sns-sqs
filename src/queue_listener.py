import time

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POLLING_INTERVAL,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    logger,
)
from config_producer import LIST_TOPIC_NAME
from scripts.aws_queue import Queue
from setup import (
    initialize_aws_setup,
)
from utils import receive_message_from_queue


def initialize_consumer(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    list_topic_name: list = LIST_TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
):
    """
    Initialize the consumer by setting up the AWS connection and PostgreSQL table.
    """

    _, sqs_client, _, queue_url = initialize_aws_setup(
        role=role,
        session_name=session_name,
        list_topic_name=list_topic_name,
        queue_name=queue_name,
    )
    return sqs_client, queue_url


def consumer(
    sqs_client: Queue,
    queue_url: str = None,
    db_uri: str = POSTGRES_URI,
) -> None:
    is_consumer_running = True

    while is_consumer_running:
        try:
            receive_message_from_queue(
                sqs_client=sqs_client,
                queue_url=queue_url,
                db_uri=db_uri,
            )
            time.sleep(POLLING_INTERVAL)  # polling interval
        except Exception as e:
            is_consumer_running = False
            logger.error(f"Error receiving messages: {e}")
            break


if __name__ == "__main__":
    sqs_client, queue_url = initialize_consumer()
    consumer(sqs_client=sqs_client, queue_url=queue_url, db_uri=POSTGRES_URI)
