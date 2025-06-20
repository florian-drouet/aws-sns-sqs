from config import (
    AWS_ARN_ROLE_CONSUMER,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    TOPIC_NAME,
    logger,
)
from scripts.aws_queue import Queue
from scripts.aws_topic import Topic
from scripts.postgres import PostgresClient


def initialize_aws_setup(
    role: str = AWS_ARN_ROLE_CONSUMER,
    session_name: str = SESSION_NAME,
    topic_name: str = TOPIC_NAME,
    queue_name: str = QUEUE_NAME,
):
    """
    Initialize the AWS setup by creating an SNS topic a SQS queue and a DLQ queue
    The, subscribe the queue to the topic.
    """
    topic = Topic(
        role=role,
        session_name=session_name,
        topic_name=topic_name,
        logger=logger,
    )
    topic_arn = topic.get_or_create_topic()

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
    queue_url = queue.get_queue_url()
    if not queue_url:
        queue.initialize_queue(
            topic_arn=topic_arn,
            dead_letter_queue_arn=dead_letter_queue_arn,
        )
        queue_url = queue.get_queue_url()
    return topic.sns_client, queue.sqs_client, topic_arn, queue_url


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
    return postgres_client
