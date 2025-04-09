from aws_connection import AWSConnection
from utils import get_queue_url, receive_message_from_queue
from config import logger, AWS_ARN_ROLE_CONSUMER, POSTGRES_URI, SESSION_NAME, TOPIC_NAME, QUEUE_NAME, POLLING_INTERVAL
from setup import initialize_aws_setup, initialize_postgres_table
from scripts.message import Message
import time

def consumer(postgres_client, sqs_client, queue_url):
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

    session_name = "test_producer_consumer"
    topic_name = "topic_producer"
    queue_name = "topic_consumer"
    role=AWS_ARN_ROLE_CONSUMER
    
    postgres_client = Message(db_uri=POSTGRES_URI)

    _, sqs_client, _, queue_url = initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME, topic_name=TOPIC_NAME, queue_name=QUEUE_NAME)
    initialize_postgres_table(postgres_client=postgres_client)

    consumer(postgres_client=postgres_client, sqs_client=sqs_client, queue_url=queue_url)
