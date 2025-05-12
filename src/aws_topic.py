from botocore.exceptions import ClientError

from aws_connection import AWSConnection


class Topic(AWSConnection):
    def __init__(self, role: str, session_name: str, topic_name: str, logger=None):
        super().__init__(role=role, session_name=session_name)
        self.topic_name = topic_name
        self.sns_client = self.get_client(service="sns")
        self.sts_client = self.get_client(service="sts")
        self.logger = logger

    def get_account_id(self):
        try:
            account_id = self.sts_client.get_caller_identity()["Account"]
            return account_id
        except ClientError as e:
            self.logger.error(f"Error getting account ID: {e}")
            return None

    def get_region(self):
        try:
            response = self.sns_client.meta.region_name
            return response
        except ClientError as e:
            self.logger.error(f"Error getting region: {e}")
            return None

    def topic_exists(self) -> bool:
        region = self.get_region()
        account_id = self.get_account_id()
        topic_arn = f"arn:aws:sns:{region}:{account_id}:{self.topic_name}"
        try:
            self.sns_client.get_topic_attributes(TopicArn=topic_arn)
            return True, topic_arn
        except self.sns_client.exceptions.NotFoundException:
            return False, None
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotFound":
                return False, None
            raise e

    def get_or_create_topic(self):
        try:
            is_topic, topic_arn = self.topic_exists()
            if is_topic:
                self.logger.info(f"Topic '{self.topic_name}' already exists.")
                return topic_arn
            response = self.sns_client.create_topic(Name=self.topic_name)
            topic_arn = response["TopicArn"]
            self.logger.info(f"Topic created: {self.topic_name}, ARN: {topic_arn}")
            return topic_arn
        except ClientError as e:
            self.logger.error(f"Error creating topic: {e}")
            return None
