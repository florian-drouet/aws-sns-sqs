import json

from botocore.exceptions import ClientError

from config import logger


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
            data = postgres_client.handle_message(message_body)
            postgres_client.insert_data(
                table_name=table_name, data=data, columns=columns, strategy="update"
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
