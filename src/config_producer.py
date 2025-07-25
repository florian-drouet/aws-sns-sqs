# DICTIONARY OF CONSUMERS #
from simple_message import SimpleMessage

# from message import Message

DICT_CONSUMERS = {
    "test-topic": SimpleMessage,
    "sns_on-consultation-close_api_dev_eu-west-3": SimpleMessage,
}
LIST_TOPIC_NAME = list(DICT_CONSUMERS.keys())
