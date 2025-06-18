import random
import threading
import time

from config import (
    AWS_ARN_ROLE_CONSUMER,
    POSTGRES_URI,
)
from setup import initialize_aws_setup
from simple_message import SimpleMessage
from utils import (
    receive_message_from_queue,
    send_message_to_topic,
)

NUM_MESSAGES = 20
session_name = "test_session"
topic_name = "test_topic"
queue_name = "test_queue"


def test_listen_sqs() -> None:
    sns_client, sqs_client, topic_arn, queue_url = initialize_aws_setup(
        role=AWS_ARN_ROLE_CONSUMER,
        session_name=session_name,
        topic_name=topic_name,
        queue_name=queue_name,
    )

    postgres_client = SimpleMessage(db_uri=POSTGRES_URI)
    postgres_client.delete_table(
        schema_name=postgres_client.schema_name, table_name=postgres_client.table_name
    )  # Clean up the table if it exists
    postgres_client.get_or_create_table(
        schema_name=postgres_client.schema_name,
        table_name=postgres_client.table_name,
        columns=postgres_client.columns,
    )

    def producer() -> None:
        for i in range(NUM_MESSAGES):
            send_message_to_topic(
                sns_client=sns_client,
                topic_arn=topic_arn,
                message_body=f"Test message {i}",
                subject=f"Test Subject {i}",
                message_attributes={},
            )
            time.sleep(random.randint(1, 5))  # simulate small delay between messages

    def consumer() -> None:
        received = 0
        attempts = 0

        while received < NUM_MESSAGES and attempts < 50:
            messages = receive_message_from_queue(
                postgres_client=postgres_client,
                schema_name=postgres_client.schema_name,
                table_name=postgres_client.table_name,
                sqs_client=sqs_client,
                queue_url=queue_url,
                columns=postgres_client.columns,
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

    nb_elements = postgres_client.count_elements(
        schema_name=postgres_client.schema_name, table_name=postgres_client.table_name
    )
    assert nb_elements == NUM_MESSAGES, (
        f"Expected {NUM_MESSAGES} elements in the PostgreSQL table, but found {nb_elements}."
    )
