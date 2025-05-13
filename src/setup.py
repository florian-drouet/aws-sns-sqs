from aws_queue import Queue
from aws_topic import Topic
from config import logger


def initialize_aws_setup(
    role: str, session_name: str, topic_name: str, queue_name: str
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
