import os

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    logger,
)
from config_producer import LIST_TOPIC_NAME
from scripts.aws_queue import Queue
from scripts.aws_topic import Topic
from scripts.postgres import PostgresClient


def initialize_aws_setup(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    list_topic_name: list = LIST_TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
):
    """
    Initialize the AWS setup by creating an SNS topic a SQS queue and a DLQ queue
    The, subscribe the queue to the topic.
    """
    list_topic_arn = []
    for topic_name in list_topic_name:
        topic = Topic(
            role=role,
            session_name=session_name,
            topic_name=topic_name,
            logger=logger,
        )
        topic_arn = topic.get_or_create_topic()
        list_topic_arn.append(topic_arn)

    # Create queue if it doesn't exist
    dead_letter_queue = Queue(
        role=role,
        session_name=session_name,
        queue_name=f"dead_letter_{queue_name}",
        logger=logger,
    )
    dead_letter_queue_url = dead_letter_queue.get_queue_url()
    if not dead_letter_queue_url:
        dead_letter_queue.create_queue()
        dead_letter_queue_arn = dead_letter_queue.get_queue_arn()
    else:
        dead_letter_queue_arn = dead_letter_queue.get_queue_arn()
    queue = Queue(
        role=role,
        session_name=session_name,
        queue_name=queue_name,
        logger=logger,
    )
    queue.initialize_queue(
        list_topic_arn=list_topic_arn,
        dead_letter_queue_arn=dead_letter_queue_arn,
    )
    queue_url = queue.get_queue_url()
    return topic.sns_client, queue.sqs_client, list_topic_arn, queue_url


def initialize_postgres_client(postgres: PostgresClient, db_uri: str = POSTGRES_URI):
    """
    Initialize the PostgreSQL backend.
    """
    postgres_client = postgres(db_uri=db_uri)
    postgres_client.get_or_create_table(
        schema_name=postgres_client.schema_name,
        table_name=postgres_client.table_name,
        columns=postgres_client.columns,
    )

    if os.getenv("LOCALSTACK") == "1":
        postgres_client.delete_data(
            schema_name=postgres_client.schema_name,
            table_name=postgres_client.table_name,
            delete_column=postgres_client.delete_column,
        )

    return postgres_client
