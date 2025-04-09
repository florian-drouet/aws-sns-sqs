import random
import threading
import time

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POSTGRES_URI,
    QUEUE_NAME,
    SESSION_NAME,
    TOPIC_NAME,
)
from scripts.message import Message
from setup import delete_postgres_table, initialize_aws_setup, initialize_postgres_table
from utils import (
    get_connection_aws,
    get_queue_url,
    get_topic_arn,
    receive_message_from_queue,
    send_message_to_topic,
)

NUM_MESSAGES = 20

def test_listen_sqs():

    initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME, topic_name=TOPIC_NAME, queue_name=QUEUE_NAME)

    sns_client = get_connection_aws(client="sns", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    topic_arn = get_topic_arn(sns_client=sns_client, topic_name=TOPIC_NAME)

    sqs_client = get_connection_aws(client="sqs", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    queue_url = get_queue_url(sqs_client=sqs_client, queue_name=QUEUE_NAME)

    postgres_client = Message(db_uri=POSTGRES_URI)
    delete_postgres_table(postgres_client=postgres_client)  # Clean up the table if it exists
    initialize_postgres_table(postgres_client=postgres_client)

    def producer():
        for i in range(NUM_MESSAGES):
            send_message_to_topic(
                sns_client=sns_client,
                topic_arn=topic_arn,
                message_body=f"Test message {i}",
                subject=f"Test Subject {i}",
                message_attributes={}
            )
            time.sleep(random.randint(1,5))  # simulate small delay between messages

    def consumer():
        received = 0
        attempts = 0

        while received < NUM_MESSAGES and attempts < 50:
            messages = receive_message_from_queue(
                postgres_client=postgres_client,
                table_name=postgres_client.table_name,
                sqs_client=sqs_client,
                queue_url=queue_url,
                columns=postgres_client.columns
            )
            for message in messages:
                received += 1
            attempts += 1
            time.sleep(1)  # polling interval

    # Run producer and consumer in parallel
    sender_thread = threading.Thread(target=producer)
    receiver_thread = threading.Thread(target=consumer)

    sender_thread.start()
    receiver_thread.start()

    sender_thread.join()
    receiver_thread.join()

    nb_elements_postgres = postgres_client.count_elements(table_name="messages")
    assert nb_elements_postgres == 20, f"Expected 20 elements in the PostgreSQL table, but found {nb_elements_postgres}."
