import time

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POLLING_INTERVAL,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    TOPIC_NAME,
    logger,
)
from message import Message
from scripts.aws_queue import Queue
from setup import (
    initialize_aws_setup,
)
from utils import receive_message_from_queue


def initialize_consumer(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    topic_name: str = TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
):
    """
    Initialize the consumer by setting up the AWS connection and PostgreSQL table.
    """
    postgres_client = Message(db_uri=POSTGRES_URI)
    postgres_client.delete_table(
        schema_name=postgres_client.schema_name, table_name=postgres_client.table_name
    )  # Clean up the table if it exists
    postgres_client.create_table(
        schema_name=postgres_client.schema_name,
        table_name=postgres_client.table_name,
        columns=postgres_client.columns,
    )

    _, sqs_client, _, queue_url = initialize_aws_setup(
        role=role,
        session_name=session_name,
        topic_name=topic_name,
        queue_name=queue_name,
    )
    return postgres_client, sqs_client, queue_url


def consumer() -> None:
    queue = Queue(
        role=AWS_ARN_ROLE_CONSUMER,
        session_name=SESSION_NAME,
        queue_name=QUEUE_NAME,
        logger=logger,
    )
    queue_url = queue.get_queue_url()
    postgres_client = Message(db_uri=POSTGRES_URI)

    is_consumer_running = True

    while is_consumer_running:
        try:
            receive_message_from_queue(
                postgres_client=postgres_client,
                schema_name=postgres_client.schema_name,
                table_name=postgres_client.table_name,
                sqs_client=queue.sqs_client,
                queue_url=queue_url,
                columns=postgres_client.columns,
            )
            time.sleep(POLLING_INTERVAL)  # polling interval
        except Exception as e:
            is_consumer_running = False
            logger.error(f"Error receiving messages: {e}")
            break


if __name__ == "__main__":
    initialize_consumer()
    consumer()
