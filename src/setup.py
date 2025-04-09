from aws_connection import AWSConnection
from utils import create_topic, create_queue, get_queue_arn, subscribe_queue_to_topic, send_message_to_topic, get_connection_aws
from config import logger, AWS_ARN_ROLE_CONSUMER, POSTGRES_URI, SESSION_NAME, TOPIC_NAME, QUEUE_NAME
import time

from scripts.postgres import PostgresClient
from scripts.message import Message


def initialize_aws_setup(role:str, session_name:str, topic_name:str, queue_name:str):
    """
    Initialize the AWS setup by creating an SNS topic and an SQS queue, and subscribing the queue to the topic.
    """
    sns_client = get_connection_aws(client="sns", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    sqs_client = get_connection_aws(client="sqs", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)

    # Create SNS topic if it doesn't exist
    topic_arn = create_topic(sns_client, topic_name)

    # Create queue if it doesn't exist
    queue_url = create_queue(sqs_client, queue_name)
    queue_arn = get_queue_arn(sqs_client, queue_name)

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

if __name__ == "__main__":
    # Initialize AWS setup
    initialize_aws_setup()
    
    # Initialize PostgreSQL table
    initialize_postgres_table()
