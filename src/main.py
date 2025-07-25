import threading

from flask import Flask

from config import CONSUMER_NAME, POSTGRES_URI, logger
from config_producer import DICT_CONSUMERS
from queue_listener import consumer, initialize_consumer


def start_consumer():
    logger.info(f"Starting consumer for {CONSUMER_NAME}...")
    try:
        sqs_client, queue_url = initialize_consumer()
        consumer(sqs_client=sqs_client, queue_url=queue_url)
        return {"status": "ok", "message": "Consumer initialized and running."}
    except Exception as e:
        return [{"server-status": f"error: {e}"}]


def create_app():
    app = Flask(__name__)

    @app.route("/")
    def index():
        return [{"status": "ok", "message": f"Welcome to {CONSUMER_NAME} consumer."}]

    @app.route("/_healthz")
    def health_check():
        return [{"status": "ok", "message": "Flask app is running."}]

    @app.route("/aggregate/<consumer>")
    def aggregate(consumer):
        try:
            # Dynamically get the consumer class based on the argument
            consumer_cls = DICT_CONSUMERS.get(consumer)
            if hasattr(consumer_cls, "aggregate"):
                client = consumer_cls(db_uri=POSTGRES_URI)
                data = client.aggregate()
                return data
            else:
                return [{"server-status": "error: aggregate method not implemented"}]
        except Exception as e:
            return [{"server-status": f"error: {e}"}]

    @app.route("/cleanup/<consumer>")
    def cleanup(consumer):
        try:
            consumer_cls = DICT_CONSUMERS.get(consumer)
            if consumer_cls:
                client = consumer_cls(db_uri=POSTGRES_URI)
                client.delete_data(
                    schema_name=client.schema_name,
                    table_name=client.table_name,
                    delete_column=client.delete_column,
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
