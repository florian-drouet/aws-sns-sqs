import json

from botocore.exceptions import ClientError

from aws_connection import AWSConnection
from config import logger


def get_connection_aws(client: str, role: str, session_name: str):
    """
    Establish a connection to AWS services.
    """
    con = AWSConnection(
        role=role,
        session_name=session_name,
    )
    return con.get_client(client)


def create_topic(sns_client, topic_name):
    """
    Create an SNS topic.
    """
    try:
        # Create the topic
        response = sns_client.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]
        logger.info(f"Created topic: {topic_name}, ARN: {topic_arn}")
        return topic_arn
    except ClientError as e:
        logger.error(f"Error creating topic: {e}")
        return None


def get_topic_arn(sns_client, topic_name):
    """
    Get the ARN of an SNS topic by its name.
    """
    try:
        # List all SNS topics
        response = sns_client.list_topics()
        topics = response.get("Topics", [])

        # Find the topic ARN by name
        for topic in topics:
            if topic["TopicArn"].endswith(topic_name):
                return topic["TopicArn"]

        logger.error(f"Topic '{topic_name}' not found.")
        return None
    except ClientError as e:
        logger.error(f"Error retrieving topic ARN: {e}")
        return None


def create_queue(sqs_client, queue_name):
    """
    Create an SQS queue.
    """
    try:
        response = sqs_client.create_queue(QueueName=queue_name)
        queue_url = response["QueueUrl"]
        logger.info(f"Created queue: {queue_name}, URL: {queue_url}")
        return queue_url
    except ClientError as e:
        logger.error(f"Error creating queue: {e}")
        return None


def get_queue_arn(sqs_client, queue_name):
    """
    Get the ARN of an SQS queue by its name.
    """
    try:
        # List all SQS queues
        response = sqs_client.list_queues()
        queues = response.get("QueueUrls", [])

        # Find the queue URL by name
        for queue in queues:
            if queue.endswith(queue_name):
                queue_arn = sqs_client.get_queue_attributes(
                    QueueUrl=queue, AttributeNames=["QueueArn"]
                )
                queue_arn = queue_arn["Attributes"]["QueueArn"]
                logger.info(
                    f"Queue '{queue_name}' found. URL: {queue}, ARN: {queue_arn}"
                )
                return queue_arn

        logger.error(f"Queue '{queue_name}' not found.")
        return None
    except ClientError as e:
        logger.error(f"Error retrieving queue URL: {e}")
        return None


def get_queue_url(sqs_client, queue_name):
    """
    Get the URL of an SQS queue by its name.
    """
    try:
        # List all SQS queues
        response = sqs_client.list_queues()
        queues = response.get("QueueUrls", [])

        # Find the queue URL by name
        for queue in queues:
            if queue.endswith(queue_name):
                logger.info(f"Queue '{queue_name}' found. URL: {queue}")
                return queue

        logger.error(f"Queue '{queue_name}' not found.")
        return None
    except ClientError as e:
        logger.error(f"Error retrieving queue URL: {e}")
        return None


def subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_arn):
    """
    Subscribe an SQS queue to an SNS topic.
    """
    try:
        # Subscribe the SQS queue to the SNS topic
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
        )

        subscription_arn = response["SubscriptionArn"]
        logger.info(f"Subscribed queue to topic. Subscription ARN: {subscription_arn}")
        return subscription_arn
    except ClientError as e:
        logger.error(f"Error subscribing queue to topic: {e}")
        return None


def send_message_to_topic(
    sns_client, topic_arn, message_body, subject=None, message_attributes=None
):
    try:
        # Publish message to SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message_body,
            Subject=subject,  # Optional subject
            MessageAttributes=message_attributes
            or {},  # If no attributes, use empty dict
        )

        logger.info(f"Message sent to Topic. Message ID: {response['MessageId']}")
        return response
    except ClientError as e:
        logger.error(f"Error sending message: {e}")
        return None


def receive_message_from_queue(
    postgres_client, table_name, sqs_client, queue_url, columns
):
    """
    Receive messages from an SQS queue.
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # Long polling
            VisibilityTimeout=30,  # Message visibility timeout
            MessageAttributeNames=["All"],  # Retrieve all message attributes
        )

        messages = response.get("Messages", [])
        if not messages:
            logger.info("No messages received.")
            return None

        for message in messages:
            message_body = json.loads(message["Body"])
            logger.info(f"Received message: {message_body.get('MessageId')}")
            postgres_client.handle_message(message_body)
            postgres_client.insert_data(
                table_name=table_name, data=postgres_client.data
            )
            delete_message_from_queue(sqs_client, queue_url, message)
        return messages
    except ClientError as e:
        logger.error(f"Error receiving message: {e}")
        return None


def delete_message_from_queue(sqs_client, queue_url, message):
    """
    Delete a message from an SQS queue.
    """
    try:
        # Delete the message from the SQS queue
        response = sqs_client.delete_message(
            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
        )

        logger.info(
            f"Deleted message from queue. Receipt Handle: {message['ReceiptHandle']}"
        )
        return response
    except ClientError as e:
        logger.error(f"Error deleting message: {e}")
        return None
