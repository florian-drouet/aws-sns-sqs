import datetime
import os

import boto3
from boto3 import Session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from config import logger


class AWSConnectionMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class AWSConnection(metaclass=AWSConnectionMeta):
    def __init__(
        self,
        role: str = None,
        session_name: str = None,
        assume_role_max_duration: int = 3600,
    ) -> None:

        self.credentials = self._init_credentials(
            role, session_name, assume_role_max_duration
        )
        # if no role assumed or in local dev / testing environment return None
        if self.credentials == {} or os.getenv("LOCALSTACK") == "1":
            self.expired_date = None
        else:
            self.expired_date = self.credentials._expiry_time.isoformat()
        self.utc = datetime.timezone.utc

    def _init_credentials(self, role, session_name, assume_role_max_duration):

        self.role = role
        self.session_name = session_name
        self.region = "eu-west-3"
        self.assume_role_max_duration = assume_role_max_duration

        if (
            self.assume_role_max_duration is None
            or not 900 <= self.assume_role_max_duration <= 28800
        ):
            raise Exception(
                "Assume role max duration is None or not between 15 mins and 8 hours"
            )

        if os.getenv("LOCALSTACK") == "1":
            if os.getenv("REDSHIFT") == "1":
                return {}
            host = "localstack" if os.getenv("CI_DOCKER") == "1" else "localhost"
            logger.info(f"Host is: {host}")
            credentials = {
                "endpoint_url": f"http://{host}:4566",
            }
            return credentials

        # If no role, try connection without assume role
        if self.role is None:
            logger.warning("No role given : using aws connection without assume role")
            return {}

        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=self._refresh(),
            refresh_using=self._refresh,
            method="sts-assume-role",
        )

        return session_credentials

    def _refresh(self):
        """
        Refresh tokens by calling assume_role again
        Documentation: https://dev.to/li_chastina/auto-refresh-aws-tokens-using-iam-role-and-boto3-2cjf
        """

        sts_client = boto3.client("sts", region_name=self.region)

        params = {
            "RoleArn": self.role,
            "RoleSessionName": self.session_name,
            "DurationSeconds": self.assume_role_max_duration,
        }

        response = sts_client.assume_role(**params).get("Credentials")

        credentials = {
            "access_key": response.get("AccessKeyId"),
            "secret_key": response.get("SecretAccessKey"),
            "token": response.get("SessionToken"),
            "expiry_time": response.get("Expiration").isoformat(),
        }

        self.expired_date = credentials.get("expiry_time")

        return credentials

    def get_session(self) -> boto3.Session:
        """
        Get s3 session object
        """
        session = get_session()
        session._credentials = self.credentials
        session.set_config_variable(logical_name="region", value=self.region)
        return Session(botocore_session=session)

    def get_resource(self, service) -> boto3.resource:
        """
        Get s3 resource object
        Args:
            service: aws service to connect to
        """

        if (self.expired_date is not None) and (
            datetime.datetime.now(self.utc)
            > datetime.datetime.fromisoformat(self.expired_date)
        ):
            self.__init__(role=self.role, session_name=self.session_name)

        # if no role assumed or in local dev / testing environment return None
        if self.credentials == {} or os.getenv("LOCALSTACK") == "1":
            logger.info(
                f"No role assumed or in local dev / testing environment, credentials: {self.credentials}"
            )
            return boto3.resource(service, region_name=self.region, **self.credentials)
        else:
            autorefresh_session = self.get_session()
            return autorefresh_session.resource(service_name=service)

    def get_client(self, service) -> boto3.client:
        """
        Get s3 client object
        Args:
            service: aws service to connect to
        """

        if (self.expired_date is not None) and (
            datetime.datetime.now(self.utc)
            > datetime.datetime.fromisoformat(self.expired_date)
        ):
            self.__init__(role=self.role, session_name=self.session_name)

        # if no role assumed or in local dev / testing environment return None
        if self.credentials == {} or os.getenv("LOCALSTACK") == "1":
            logger.info(
                f"No role assumed or in local dev / testing environment, credentials: {self.credentials}"
            )
            return boto3.client(service, region_name=self.region,  **self.credentials)
        else:
            autorefresh_session = self.get_session()
            return autorefresh_session.client(service)
