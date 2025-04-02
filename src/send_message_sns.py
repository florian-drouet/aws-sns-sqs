from botocore.exceptions import ClientError
from aws_connection import AWSConnection

from config import AWS_ARN_ROLE_CONSUMER_TC_LIVE, logger

def create_topic(sns_client, topic_name):
    """
    Create an SNS topic if it does not already exist.
    In CLI : 
        ```export AWS_ENDPOINT_URL="http://localhost:4566"
        aws --endpoint-url=$AWS_ENDPOINT_URL sns create-topic --name test-sns-sqs```
    """
    try:
        # Check if the topic already exists
        existing_topic_arn = get_topic_arn(sns_client, topic_name)
        if existing_topic_arn:
            logger.info(f"Topic '{topic_name}' already exists. ARN: {existing_topic_arn}")
            return existing_topic_arn
        
        # Create the topic if it does not exist
        response = sns_client.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
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
        topics = response.get('Topics', [])
        
        # Find the topic ARN by name
        for topic in topics:
            if topic['TopicArn'].endswith(topic_name):
                return topic['TopicArn']
        
        logger.error(f"Topic '{topic_name}' not found.")
        return None
    except ClientError as e:
        logger.error(f"Error retrieving topic ARN: {e}")
        return None


def create_queue(sqs_client, queue_name):
    """
    Create an SQS queue if it does not already exist.
    """
    try:
        # Check if the queue already exists
        existing_queue_url = get_queue_url(sqs_client, queue_name)
        if existing_queue_url:
            logger.info(f"Queue '{queue_name}' already exists. URL: {existing_queue_url}")
            return existing_queue_url
        
        # Create the queue if it does not exist
        response = sqs_client.create_queue(QueueName=queue_name)
        queue_url = response['QueueUrl']
        logger.info(f"Created queue: {queue_name}, URL: {queue_url}")
        return queue_url
    except ClientError as e:
        logger.error(f"Error creating queue: {e}")
        return None


def get_queue_url(sqs_client, queue_name):
    """
    Get the URL of an SQS queue by its name.
    """
    try:
        # List all SQS queues
        response = sqs_client.list_queues()
        queues = response.get('QueueUrls', [])
        
        # Find the queue URL by name
        for queue in queues:
            if queue.endswith(queue_name):
                queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])
                queue_arn = queue_arn['Attributes']['QueueArn']
                logger.info(f"Queue '{queue_name}' found. URL: {queue}, ARN: {queue_arn}")
                return queue_arn
        
        logger.error(f"Queue '{queue_name}' not found.")
        return None
    except ClientError as e:
        logger.error(f"Error retrieving queue URL: {e}")
        return None


def send_message_to_topic(sns_client, topic_arn, message_body, subject=None, message_attributes=None):
    try:
        # Publish message to SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message_body,
            Subject=subject,  # Optional subject
            MessageAttributes=message_attributes or {}  # If no attributes, use empty dict
        )
        
        logger.info(f"Message sent to Topic. Message ID: {response['MessageId']}")
        return response
    except ClientError as e:
        logger.error(f"Error sending message: {e}")
        return None


def subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_url):
    """
    Subscribe an SQS queue to an SNS topic.
    """
    try:
        # Subscribe the SQS queue to the SNS topic
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=queue_url
        )
        
        subscription_arn = response['SubscriptionArn']
        logger.info(f"Subscribed queue to topic. Subscription ARN: {subscription_arn}")
        return subscription_arn
    except ClientError as e:
        logger.error(f"Error subscribing queue to topic: {e}")
        return None
