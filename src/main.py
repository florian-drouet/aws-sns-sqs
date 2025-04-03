from aws_connection import AWSConnection
from send_message_sns import create_topic, create_queue, subscribe_queue_to_topic, send_message_to_topic
from config import logger, AWS_ARN_ROLE_CONSUMER_TC_LIVE


if __name__ == "__main__":
    con = AWSConnection(
        #role=AWS_ARN_ROLE_CONSUMER_TC_LIVE,
        session_name="test_consumer_tc_live",
    )
    
    # Example usage
    sns_client = con.get_client("sns")
    sqs_client = con.get_client("sqs")

    topic_name = "test-sns-sqs"  # Replace with your topic name

    # Create SNS topic if it doesn't exist
    topic_arn = create_topic(sns_client, topic_name)

    # Create queue if it doesn't exist
    queue_name = "test-sqs-queue"  # Replace with your queue name
    queue_url = create_queue(sqs_client, queue_name)

    # Subscribe the queue to the topic
    if topic_arn and queue_url:
        subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_url)
    
    if topic_arn:
        message_body = "Hello, this is a test message!"
        subject = "Test Subject"
        message_attributes = {
            'AttributeKey': {
                'DataType': 'String',
                'StringValue': 'AttributeValue'
            }
        }
        
        send_message_to_topic(sns_client, topic_arn, message_body, subject, message_attributes)
    else:
        logger.error(f"Failed to retrieve topic ARN for topic name: {topic_name}")
