# aws-sns-sqs
Sandbox repository for SNS/SQS communication protocols with AWS

To launch the app [install uv](https://github.com/astral-sh/uv).  
Then run `uv sync`  
Then run `uv run src/queue_listener.py` 

To test the app in local env (with docker-compose) :

Add .env :
```
ENV=local
DEBUG=1

LOCALSTACK=1

POSTGRES_URI = "postgres://admin:password@localhost:5432/mydatabase"
AWS_ENDPOINT_URL="http://localhost:4566"

SESSION_NAME="test-live-tracking"
QUEUE_NAME="test-queue"
TOPIC_NAME="test-topic"
```

Then run :
```
make rebuild
make test
```

This repository works well with localstack but in a production AWS environment you will need to
add some rights :
- SQS:CreateQueue
- SQS:GetQueueAttributes
- SQS:ListQueue
- SQS:GetTopicAttributes
- SNS:CreateTopic


```
docker build --no-cache --build-arg GITHUB_TOKEN_ARG="$GITHUB_TOKEN" -t consumer-live --file Dockerfile .
docker run -p 8000:8000 --rm=false --env-file .env consumer-live
```

Lauching Flask App for production :
```
PYTHONPATH=src uv run gunicorn src.main:app --bind 0.0.0.0:8000 --threads 4 --reload
```