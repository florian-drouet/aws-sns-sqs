import datetime
import json

from config import DIRECTORY_PATH
from scripts.postgres import PostgresClient


class Message(PostgresClient):
    def __init__(self, db_uri) -> None:
        super().__init__(db_uri=db_uri)
        self.schema_name = "public"
        self.table_name = "messages"
        self.primary_key = "consultation_id"
        self.delete_column = "inserted_utc_at"
        self.columns = {
            "consultation_id": "VARCHAR PRIMARY KEY",
            "inserted_utc_at": "TIMESTAMP",
            "estimated_start_utc_at": "TIMESTAMP",
            "closed_utc_at": "TIMESTAMP",
        }

    def handle_message(self, message_body) -> None:
        """
        Handle the message received from SQS.
        """
        dict_message = json.loads(message_body.get("Message"))

        data = [
            (
                dict_message.get("id"),
                datetime.datetime.now().isoformat(),
                dict_message.get("estimatedStartDate"),
                dict_message.get("closedAt"),
            )
        ]
        return data

    def aggregate(self):
        with open(f"{DIRECTORY_PATH}/queries/aggregate_message.sql", "r") as file:
            query = file.read()
        return self.execute_query(query=query)
