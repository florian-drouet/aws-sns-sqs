import pytest

from config import AWS_ARN_ROLE_CONSUMER, QUEUE_NAME, SESSION_NAME, TOPIC_NAME
from setup import initialize_aws_setup
from utils import get_connection_aws, get_topic_arn, send_message_to_topic


@pytest.mark.parametrize(
    "topic_name, message_body, subject, message_attributes",
    [
        (
            "topic_test_1_1",
            "Test message",
            "Test Subject",
            {
                'AttributeKey': {
                    'DataType': 'String',
                    'StringValue': 'AttributeValue'
                }
            }
        ),
        (
            "topic_test_1_2",
            "Another message",
            "Another Subject",
            {
                'AnotherKey': {
                    'DataType': 'String',
                    'StringValue': 'AnotherValue'
                }
            }
        ),
        # Add more test cases here if needed
    ]
)
def test_sns_response(topic_name, message_body, subject, message_attributes):

    initialize_aws_setup(role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME, topic_name=TOPIC_NAME, queue_name=QUEUE_NAME)

    sns_client = get_connection_aws(client="sns", role=AWS_ARN_ROLE_CONSUMER, session_name=SESSION_NAME)
    topic_arn = get_topic_arn(sns_client=sns_client, topic_name=TOPIC_NAME)

    response = send_message_to_topic(
        sns_client=sns_client,
        topic_arn=topic_arn,
        message_body=message_body,
        subject=subject,
        message_attributes=message_attributes
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "Failed to send message to SNS topic"
    assert response['MessageId'] is not None, "Message ID is missing in the response"
