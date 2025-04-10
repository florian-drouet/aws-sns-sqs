import pytest

from config import AWS_ARN_ROLE_CONSUMER
from setup import initialize_aws_setup
from utils import get_connection_aws, get_topic_arn, send_message_to_topic


@pytest.mark.parametrize(
    "message_body, subject, message_attributes",
    [
        ("Test message 1", "Test Subject 1", {}),
        ("Test message 2", "Test Subject 2", {"Attribute1": {"DataType": "String", "StringValue": "Value1"}}),
        ("Test message 3", "Test Subject 3", {"Attribute2": {"DataType": "Number", "StringValue": "123"}}),
    ]
)
def test_sns_response(message_body, subject, message_attributes):
    session_name = "test_session_producer"
    topic_name = "test_topic_producer"
    queue_name = "test_queue_producer"

    initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=session_name, topic_name=topic_name, queue_name=queue_name)

    sns_client = get_connection_aws(client="sns", role=AWS_ARN_ROLE_CONSUMER, session_name=session_name)
    topic_arn = get_topic_arn(sns_client=sns_client, topic_name=topic_name)

    response = send_message_to_topic(
        sns_client=sns_client,
        topic_arn=topic_arn,
        message_body=message_body,
        subject=subject,
        message_attributes=message_attributes
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Failed to send message to SNS topic"
    assert response['MessageId'] is not None, "Message ID is missing in the response"
