import json

from botocore.exceptions import ClientError

from config import MAX_NUMBER_OF_MESSAGES, VISIBILTY_TIMEOUT, WAIT_TIME_SECONDS, logger


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
    postgres_client, schema_name, table_name, sqs_client, queue_url, columns
):
    """
    Receive messages from an SQS queue.
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
            WaitTimeSeconds=WAIT_TIME_SECONDS,  # Long polling
            VisibilityTimeout=VISIBILTY_TIMEOUT,
            MessageAttributeNames=["All"],  # Retrieve all message attributes
        )

        messages = response.get("Messages", [])
        if not messages:
            logger.info("No messages received.")
            return None

        data_batch = []
        for message in messages:
            message_body = json.loads(message["Body"])
            logger.info(f"Received message: {message_body.get('MessageId')}")
            data = postgres_client.handle_message(message_body)
            data_batch = [*data_batch, *data]

        postgres_client.insert_data(
            schema_name=schema_name,
            table_name=table_name,
            data=data_batch,
            columns=columns,
            strategy="skip",
        )
        delete_batch_messages_from_queue(sqs_client, queue_url, messages)
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


def delete_batch_messages_from_queue(sqs_client, queue_url, messages):
    """
    Delete a batch of messages from an SQS queue.
    """
    try:
        entries = [
            {"Id": str(i), "ReceiptHandle": msg["ReceiptHandle"]}
            for i, msg in enumerate(messages)
        ]
        response = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        nb_successful = len(response.get("Successful", []))
        nb_failed = len(response.get("Failed", []))

        if nb_failed > 0:
            logger.error(f"Failed to delete some messages: {response.get('Failed')}")
        else:
            logger.info(f"{nb_successful} Batch messages deleted successfully.")
        return response
    except ClientError as e:
        logger.error(f"Error deleting batch messages: {e}")
        return None
