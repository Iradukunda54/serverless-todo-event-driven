"""Shared helpers: AWS clients, configuration, HTTP responses and time handling."""

import functools
import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

STATUS_PENDING = "Pending"
STATUS_COMPLETED = "Completed"
STATUS_EXPIRED = "Expired"
VALID_STATUSES = (STATUS_PENDING, STATUS_COMPLETED, STATUS_EXPIRED)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


# Clients are created lazily (and cached) so tests can swap in mocked AWS.
@functools.cache
def table():
    return boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])


@functools.cache
def scheduler():
    return boto3.client("scheduler")


@functools.cache
def sns():
    return boto3.client("sns")


@functools.cache
def sqs():
    return boto3.client("sqs")


def reset_clients():
    for getter in (table, scheduler, sns, sqs):
        getter.cache_clear()


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value):
    """Parse an ISO-8601 timestamp; naive values are treated as UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def schedule_name(task_id):
    return f"task-{task_id}"


def response(status_code, body=None):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json", **CORS_HEADERS},
        "body": "" if body is None else json.dumps(body),
    }


def error(status_code, message):
    return response(status_code, {"message": message})


def user_id_from(event):
    """The Cognito `sub` claim, injected by the API Gateway Cognito authorizer."""
    return event["requestContext"]["authorizer"]["claims"]["sub"]
