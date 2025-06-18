import threading

from flask import Flask

from config import CONSUMER_NAME, POSTGRES_URI, logger
from queue_listener import consumer, initialize_consumer
from src import dict_consumers


def start_consumer(consumer_class):
    logger.info(f"Starting consumer for {consumer_class.__name__}...")
    try:
        postgres_client, sqs_client, queue_url = initialize_consumer(
            postgres=consumer_class
        )
        consumer(
            postgres_client=postgres_client, sqs_client=sqs_client, queue_url=queue_url
        )
        return {"status": "ok", "message": "Consumer initialized and running."}
    except Exception as e:
        return [{"server-status": f"error: {e}"}]


def create_app():
    app = Flask(__name__)

    consumer_class = dict_consumers.get(CONSUMER_NAME)

    @app.route("/_healthz")
    def health_check():
        return [{"status": "ok", "message": "Flask app is running."}]

    @app.route("/aggregate")
    def aggregate():
        try:
            if hasattr(consumer_class, "aggregate"):
                client = consumer_class(db_uri=POSTGRES_URI)
                data = client.aggregate()
                return data
            else:
                return [{"server-status": "error: aggregate method not implemented"}]
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    @app.route("/cleanup")
    def cleanup():
        try:
            client = consumer_class(db_uri=POSTGRES_URI)
            client.delete_data(
                schema_name=client.schema_name,
                table_name=client.table_name,
                delete_column=client.delete_column,
            )
            return [{"status": "ok", "message": "Data deleted successfully."}]
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    # Start the consumer thread when the app starts
    consumer_thread = threading.Thread(
        target=start_consumer, args=(consumer_class,), daemon=True
    )
    consumer_thread.start()

    return app


app = create_app()

# Optional: for local dev with `python main.py`
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
