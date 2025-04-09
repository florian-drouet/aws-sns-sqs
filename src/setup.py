import time

from config import AWS_ARN_ROLE_CONSUMER, SESSION_NAME, TOPIC_NAME, TOPIC_ARN, QUEUE_NAME, ENV
from utils import (
    create_queue,
    create_topic,
    get_connection_aws,
    get_queue_arn,
    get_queue_url,
    subscribe_queue_to_topic,
    get_topic_arn,
)


def initialize_aws_setup(role:str, session_name:str, topic_name:str, queue_name:str, env:str=ENV):
    """
    Initialize the AWS setup by creating an SNS topic and an SQS queue, and subscribing the queue to the topic.
    """
    sns_client = get_connection_aws(client="sns", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    sqs_client = get_connection_aws(client="sqs", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)

    # Create SNS topic if it doesn't exist
    if ENV != "production":
        topic_arn = create_topic(sns_client=sns_client, topic_name=topic_name)
    else:
        topic_arn = TOPIC_ARN

    # Create queue if it doesn't exist
    if ENV != "production":
        queue_url = create_queue(sqs_client=sqs_client, queue_name=queue_name)
    else:
        queue_url = get_queue_url(sqs_client=sqs_client, queue_name=queue_name)
    queue_arn = get_queue_arn(sqs_client=sqs_client, queue_name=queue_name)

    # Subscribe the queue to the topic
    if topic_arn and queue_url:
        time.sleep(1)  # Wait for the queue to be created
        subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_arn)
    return sns_client, sqs_client, topic_arn, queue_url

def initialize_postgres_table(postgres_client):
    """
    Initialize the PostgreSQL table.
    """
    postgres_client.create_table(schema_name=postgres_client.schema_name, table_name=postgres_client.table_name, columns=postgres_client.columns)


def delete_postgres_table(postgres_client):
    """
    Delete the PostgreSQL table.
    """
    postgres_client.delete_table(schema_name=postgres_client.schema_name, table_name=postgres_client.table_name)
