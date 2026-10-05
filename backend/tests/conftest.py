import json
import os
import sys

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import common  # noqa: E402

REGION = "eu-west-1"
USER_ID = "11111111-aaaa-bbbb-cccc-000000000001"
OTHER_USER_ID = "22222222-aaaa-bbbb-cccc-000000000002"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("TABLE_NAME", "tasks")
    monkeypatch.setenv("SCHEDULE_GROUP", "task-expiry")
    monkeypatch.setenv("DEFAULT_DEADLINE_MINUTES", "5")
    common.reset_clients()
    yield
    common.reset_clients()


@pytest.fixture
def aws(monkeypatch):
    """Mocked AWS with the same resources the SAM template creates."""
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName="tasks",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "UserId", "AttributeType": "S"},
                {"AttributeName": "TaskId", "AttributeType": "S"},
                {"AttributeName": "Status", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "UserId", "KeyType": "HASH"},
                {"AttributeName": "TaskId", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "UserStatusIndex",
                    "KeySchema": [
                        {"AttributeName": "UserId", "KeyType": "HASH"},
                        {"AttributeName": "Status", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        sqs = boto3.client("sqs")
        expiry_q = sqs.create_queue(
            QueueName="task-expiry.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
        )["QueueUrl"]
        cancel_q = sqs.create_queue(QueueName="task-cancellation.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
        dlq = sqs.create_queue(QueueName="schedule-dlq")["QueueUrl"]

        def arn(url):
            return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

        topic_arn = boto3.client("sns").create_topic(Name="task-notifications")["TopicArn"]
        boto3.client("scheduler").create_schedule_group(Name="task-expiry")

        monkeypatch.setenv("EXPIRY_QUEUE_ARN", arn(expiry_q))
        monkeypatch.setenv("SCHEDULE_DLQ_ARN", arn(dlq))
        monkeypatch.setenv("SCHEDULER_ROLE_ARN", "arn:aws:iam::123456789012:role/scheduler")
        monkeypatch.setenv("CANCELLATION_QUEUE_URL", cancel_q)
        monkeypatch.setenv("TOPIC_ARN", topic_arn)
        common.reset_clients()
        yield {"cancel_queue": cancel_q, "topic_arn": topic_arn}


def api_event(body=None, path=None, query=None, user_id=USER_ID):
    return {
        "body": json.dumps(body) if isinstance(body, dict) else body,
        "pathParameters": path,
        "queryStringParameters": query,
        "requestContext": {"authorizer": {"claims": {"sub": user_id, "email": "user@example.com"}}},
    }
