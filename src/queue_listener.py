import os
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
from scripts.aws_queue import Queue
from scripts.postgres import PostgresClient
from setup import (
    initialize_aws_setup,
    initialize_postgres_client,
)
from simple_message import SimpleMessage
from utils import receive_message_from_queue


def initialize_consumer(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    topic_name: str = TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
    postgres: PostgresClient = SimpleMessage,
    db_uri: str = POSTGRES_URI,
):
    """
    Initialize the consumer by setting up the AWS connection and PostgreSQL table.
    """
    postgres_client = initialize_postgres_client(
        postgres=postgres,
        db_uri=db_uri,
    )

    if os.getenv("LOCALSTACK") == "1":
        postgres_client.delete_data(
            schema_name=postgres_client.schema_name,
            table_name=postgres_client.table_name,
            delete_column=postgres_client.delete_column,
        )

    _, sqs_client, _, queue_url = initialize_aws_setup(
        role=role,
        session_name=session_name,
        topic_name=topic_name,
        queue_name=queue_name,
    )
    return postgres_client, sqs_client, queue_url


def consumer(
    postgres_client: PostgresClient,
    sqs_client: Queue,
    queue_url: str = None,
) -> None:
    is_consumer_running = True

    while is_consumer_running:
        try:
            receive_message_from_queue(
                postgres_client=postgres_client,
                schema_name=postgres_client.schema_name,
                table_name=postgres_client.table_name,
                sqs_client=sqs_client,
                queue_url=queue_url,
                columns=postgres_client.columns,
            )
            time.sleep(POLLING_INTERVAL)  # polling interval
        except Exception as e:
            is_consumer_running = False
            logger.error(f"Error receiving messages: {e}")
            break


if __name__ == "__main__":
    postgres_client, sqs_client, queue_url = initialize_consumer(postgres=SimpleMessage)
    consumer(
        postgres_client=postgres_client, sqs_client=sqs_client, queue_url=queue_url
    )
