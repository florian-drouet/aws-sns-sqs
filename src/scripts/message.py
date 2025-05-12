import datetime

from scripts.postgres import PostgresClient


class Message(PostgresClient):
    def __init__(self, db_uri) -> None:
        super().__init__(db_uri=db_uri)
        self.schema_name = "public"
        self.table_name = "messages"
        self.columns = {
            "id": "VARCHAR PRIMARY KEY",
            "created_at": "TIMESTAMP",
            "message": "VARCHAR",
        }

    def handle_message(self, message_body) -> None:
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
