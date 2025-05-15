import threading

from flask import Flask

from config import POSTGRES_URI
from queue_listener import consumer, initialize_consumer
from scripts.postgres import PostgresClient


def start_consumer():
    consumer()


def create_app():
    app = Flask(__name__)

    @app.route("/")
    def health_check():
        return {"status": "ok", "message": "Flask app is running."}

    @app.route("/aggregate")
    def aggregate():
        try:
            query = """
                SELECT
                    DATE_TRUNC('MINUTE', m.closed_utc_at) AS minute,
                    COUNT(*)
                FROM
                    "public".messages AS m
                GROUP BY
                    1
                ORDER BY
                    1 ASC
            """
            client = PostgresClient(db_uri=POSTGRES_URI)
            data = client.execute_query(query=query)
            return data
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    @app.route("/initialize")
    def initialize():
        try:
            initialize_consumer()
            return [
                {"status": "ok", "message": "Schema and table created successfully."}
            ]
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    @app.route("/cleanup")
    def cleanup():
        try:
            client = PostgresClient(db_uri=POSTGRES_URI)
            client.delete_data(
                schema_name="public",
                table_name="messages",
                delete_column="inserted_utc_at",
            )
            return [{"status": "ok", "message": "Data deleted successfully."}]
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    # Start the consumer thread when the app starts
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    return app


app = create_app()

# Optional: for local dev with `python main.py`
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
