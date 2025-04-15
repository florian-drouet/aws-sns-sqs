import os
import time

from utils import (
    create_queue,
    create_topic,
    get_connection_aws,
    get_queue_arn,
    get_queue_url,
    subscribe_queue_to_topic,
)


def initialize_aws_setup(role:str, session_name:str, topic_name:str, topic_arn:str, queue_name:str):
    """
    Initialize the AWS setup by creating an SNS topic and an SQS queue, and subscribing the queue to the topic.
    """
    sns_client = get_connection_aws(client="sns", role=role, session_name=session_name)
    sqs_client = get_connection_aws(client="sqs", role=role, session_name=session_name)

    # Create SNS topic if it doesn't exist
    topic_arn = topic_arn if os.getenv("TOPIC_ARN") else create_topic(sns_client=sns_client, topic_name=topic_name)

    # Create queue if it doesn't exist
    if os.getenv("QUEUE_NAME"):
        queue_url = get_queue_url(sqs_client=sqs_client, queue_name=queue_name)
    else:
        queue_url = create_queue(sqs_client=sqs_client, queue_name=queue_name)
    queue_arn = get_queue_arn(sqs_client=sqs_client, queue_name=queue_name)

    # Subscribe the queue to the topic
    if topic_arn and queue_url:
        time.sleep(1)  # Wait for the queue to be created
        subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_arn)
    return sns_client, sqs_client, topic_arn, queue_url
