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
from scripts.message import Message
from setup import (
    get_connection_aws,
    get_queue_url,
    initialize_aws_setup,
)
from utils import receive_message_from_queue


def initalize_consumer():
    """
    Initialize the consumer by setting up the AWS connection and PostgreSQL table.
    """
    postgres_client = Message(db_uri=POSTGRES_URI)
    postgres_client.delete_table(schema_name=postgres_client.schema_name, table_name=postgres_client.table_name) # Clean up the table if it exists
    postgres_client.create_table(schema_name=postgres_client.schema_name, table_name=postgres_client.table_name, columns=postgres_client.columns)

    _, sqs_client, _, queue_url = initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME, topic_name=TOPIC_NAME, queue_name=QUEUE_NAME)
    return postgres_client, sqs_client, queue_url

def consumer():
    sqs_client = get_connection_aws(client="sqs", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    queue_url = get_queue_url(sqs_client=sqs_client, queue_name=QUEUE_NAME)
    postgres_client = Message(db_uri=POSTGRES_URI)

    is_consumer_running = True

    while is_consumer_running:
        try:
            messages = receive_message_from_queue(
                postgres_client=postgres_client,
                table_name=postgres_client.table_name,
                sqs_client=sqs_client,
                queue_url=queue_url,
                columns=postgres_client.columns
            )
            time.sleep(POLLING_INTERVAL)  # polling interval
        except Exception as e:
            is_consumer_running = False
            logger.error(f"Error receiving messages: {e}")
            break

if __name__ == "__main__":
    consumer()
