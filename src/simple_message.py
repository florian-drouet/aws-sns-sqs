import datetime

from config import DIRECTORY_PATH
from scripts.postgres import PostgresClient


class SimpleMessage(PostgresClient):
    def __init__(self, db_uri):
        super().__init__(db_uri=db_uri)
        self.schema_name = "schema_name"
        self.table_name = "table_name"
        self.delete_column = "created_at"
        self.columns = {
            "id": "VARCHAR PRIMARY KEY",
            "created_at": "TIMESTAMP",
            "message": "VARCHAR",
        }

    @staticmethod
    def handle_message(message_body):
        """
        Handle the message received from SQS.
        """
        data = [
            (
                message_body.get("MessageId"),
                datetime.datetime.now().isoformat(),
                message_body.get("Message"),
            )
        ]
        return data

    def aggregate(self):
        with open(
            f"{DIRECTORY_PATH}/queries/aggregate_simple_message.sql", "r"
        ) as file:
            query = file.read()
        return self.execute_query(query=query)
