# aws-sns-sqs
Sandbox repository for SNS/SQS communication protocols with AWS

To launch the app [install uv](https://github.com/astral-sh/uv).  
Then run `uv sync`  
Then run `uv run src/queue_listener.py` 

docker-compose build  
docker-compose up -d  
docker-compose exec testrunner uv run pytest

OR in local env :

Add .env :
```
ENV=local
DEBUG=1

LOCALSTACK=1

POSTGRES_URI = "postgres://admin:password@localhost:5432/mydatabase"
AWS_ENDPOINT_URL="http://localhost:4566"
```

uv run env AWS_ENDPOINT_URL='http://localhost:4566' env ENV=local env LOCALSTACK=1 pytest