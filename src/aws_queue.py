import json
import time

from botocore.exceptions import ClientError

from aws_connection import AWSConnection


class Queue:
    """
    Class to manage AWS SQS queues.
    """

    def __init__(self, role: str, session_name: str, queue_name: str, logger=None):
        self.queue_name = queue_name
        self.connection = AWSConnection(
            role=role,
            session_name=session_name,
        )
        self.sqs_client = self.connection.get_client(service="sqs")
        self.sns_client = self.connection.get_client(service="sns")
        self.logger = logger

    def initialize_queue(self, topic_arn, dead_letter_queue_arn):
        try:
            self.create_queue()
            time.sleep(1)  # Wait for the queue to be created
            self.subscribe_queue_to_topic(topic_arn=topic_arn)
            self.add_subscription_policy_to_queue(topic_arn=topic_arn)
            self.add_redrive_policy_to_queue(
                dead_letter_queue_arn=dead_letter_queue_arn
            )
        except Exception:
            self.logger.error(f"Error initializing queue {self.queue_name}")
            raise

    def create_queue(self):
        try:
            response = self.sqs_client.create_queue(QueueName=self.queue_name)
            queue_url = response["QueueUrl"]
            self.logger.info(f"Created queue: {self.queue_name}, URL: {queue_url}")
            return queue_url
        except ClientError as e:
            self.logger.error(f"Error creating queue: {e}")
            return None

    def subscribe_queue_to_topic(self, topic_arn):
        try:
            queue_arn = self.get_queue_arn()
            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=queue_arn,
            )

            subscription_arn = response["SubscriptionArn"]
            self.logger.info(
                f"Subscribed queue to topic. Subscription ARN: {subscription_arn}"
            )
            return subscription_arn
        except ClientError as e:
            self.logger.error(f"Error subscribing queue to topic: {e}")
            return None

    def add_redrive_policy_to_queue(self, dead_letter_queue_arn, max_receive_count=5):
        queue_url = self.get_queue_url()
        queue_arn = self.get_queue_arn()

        existing_attrs = self.sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["RedrivePolicy"]
        )
        existing_policy_str = existing_attrs.get("Attributes", {}).get(
            "RedrivePolicy", ""
        )

        new_policy = {
            "deadLetterTargetArn": dead_letter_queue_arn,
            "maxReceiveCount": str(max_receive_count),
        }

        # Compare existing and desired redrive policy
        if existing_policy_str:
            try:
                existing_policy = json.loads(existing_policy_str)
                if existing_policy.get(
                    "deadLetterTargetArn"
                ) == dead_letter_queue_arn and existing_policy.get(
                    "maxReceiveCount"
                ) == str(max_receive_count):
                    self.logger.info(
                        "Redrive policy already set correctly. No update needed."
                    )
                    return
            except json.JSONDecodeError:
                self.logger.warning(
                    "Failed to parse existing RedrivePolicy, will overwrite."
                )

        self.sqs_client.set_queue_attributes(
            QueueUrl=queue_url, Attributes={"RedrivePolicy": json.dumps(new_policy)}
        )
        self.logger.info(
            f"""Redrive policy set on queue {queue_arn} to redirect to DLQ 
            {dead_letter_queue_arn} after {max_receive_count} receives."""
        )

    def add_subscription_policy_to_queue(self, topic_arn):
        queue_url = self.get_queue_url()
        queue_attrs = self.sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn", "Policy"]
        )
        queue_arn = self.get_queue_arn()
        existing_policy_str = queue_attrs["Attributes"].get("Policy", "")

        if existing_policy_str:
            policy = json.loads(existing_policy_str)
        else:
            policy = {"Version": "2012-10-17", "Id": "SQSPolicy", "Statement": []}

        statement_id = f"SNSPublish-{topic_arn.split(':')[-1]}"
        new_statement = {
            "Sid": statement_id,
            "Effect": "Allow",
            "Principal": "*",
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
        }

        if not any(st.get("Sid") == statement_id for st in policy["Statement"]):
            policy["Statement"].append(new_statement)
            self.sqs_client.set_queue_attributes(
                QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)}
            )
            self.logger.info("SQS policy updated to allow SNS topic to send messages.")
        else:
            self.logger.info("Policy already includes permission for this SNS topic.")

    def get_queue_arn(self):
        try:
            response = self.sqs_client.list_queues(QueueNamePrefix=self.queue_name)
            queue = response.get("QueueUrls", [])[0]
            queue_arn = self.sqs_client.get_queue_attributes(
                QueueUrl=queue, AttributeNames=["QueueArn"]
            )
            queue_arn = queue_arn["Attributes"]["QueueArn"]
            self.logger.info(
                f"Queue '{self.queue_name}' found. URL: {queue}, ARN: {queue_arn}"
            )
            return queue_arn
        except ClientError as e:
            self.logger.error(f"Error retrieving queue URL: {e}")
            return None

    def get_queue_url(self):
        try:
            response = self.sqs_client.get_queue_url(QueueName=self.queue_name)
            self.logger.info(f"Queue URL is: {response['QueueUrl']}")
            return response["QueueUrl"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                return None
            else:
                raise e
