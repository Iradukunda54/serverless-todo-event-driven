"""CRUD handlers behind API Gateway (REST) for the tasks table.

Table keys: UserId (partition) + TaskId (sort). Every request is scoped to the
caller's Cognito `sub`, so users can only ever reach their own tasks.
"""

import json
import os
import re
import uuid
from datetime import timedelta

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from common import (
    STATUS_COMPLETED,
    STATUS_PENDING,
    VALID_STATUSES,
    error,
    iso,
    logger,
    now_utc,
    parse_iso,
    response,
    schedule_name,
    scheduler,
    table,
    user_id_from,
)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_DESCRIPTION = 500
MIN_DEADLINE_SECONDS = 30
MAX_MINUTES = 365 * 24 * 60


def _body(event):
    try:
        data = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _validate_description(value):
    if not isinstance(value, str) or not value.strip():
        return "Description is required"
    if len(value) > MAX_DESCRIPTION:
        return f"Description must be at most {MAX_DESCRIPTION} characters"
    return None


def _validate_date(value):
    if not isinstance(value, str) or not DATE_RE.match(value):
        return "Date must be in YYYY-MM-DD format"
    return None


def _create_expiry_schedule(task_id, user_id, deadline):
    """One-time EventBridge Scheduler schedule that drops an expiry message on the FIFO queue."""
    scheduler().create_schedule(
        Name=schedule_name(task_id),
        GroupName=os.environ["SCHEDULE_GROUP"],
        ScheduleExpression=f"at({deadline.strftime('%Y-%m-%dT%H:%M:%S')})",
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        ActionAfterCompletion="DELETE",
        Target={
            "Arn": os.environ["EXPIRY_QUEUE_ARN"],
            "RoleArn": os.environ["SCHEDULER_ROLE_ARN"],
            "Input": json.dumps({"taskId": task_id, "userId": user_id}),
            "SqsParameters": {"MessageGroupId": task_id},
            "DeadLetterConfig": {"Arn": os.environ["SCHEDULE_DLQ_ARN"]},
            "RetryPolicy": {"MaximumRetryAttempts": 10, "MaximumEventAgeInSeconds": 3600},
        },
    )


def create_handler(event, _context):
    user_id = user_id_from(event)
    data = _body(event)
    if data is None:
        return error(400, "Request body must be a JSON object")

    problem = _validate_description(data.get("Description"))
    if problem:
        return error(400, problem)

    now = now_utc()
    date = data.get("Date") or now.strftime("%Y-%m-%d")
    problem = _validate_date(date)
    if problem:
        return error(400, problem)

    default_minutes = int(os.environ.get("DEFAULT_DEADLINE_MINUTES", "5"))
    if data.get("ExpiresInMinutes") is not None:
        # Relative deadline, resolved with the server clock (immune to client clock skew).
        minutes = data["ExpiresInMinutes"]
        if isinstance(minutes, bool) or not isinstance(minutes, int) or not 1 <= minutes <= MAX_MINUTES:
            return error(400, f"ExpiresInMinutes must be an integer between 1 and {MAX_MINUTES}")
        deadline = now + timedelta(minutes=minutes)
    elif data.get("Deadline"):
        try:
            deadline = parse_iso(str(data["Deadline"]))
        except ValueError:
            return error(400, "Deadline must be an ISO-8601 timestamp")
        if deadline < now + timedelta(seconds=MIN_DEADLINE_SECONDS):
            return error(400, f"Deadline must be at least {MIN_DEADLINE_SECONDS} seconds in the future")
        if deadline > now + timedelta(days=365):
            return error(400, "Deadline must be within one year")
    else:
        deadline = now + timedelta(minutes=default_minutes)

    task_id = str(uuid.uuid4())
    item = {
        "UserId": user_id,
        "TaskId": task_id,
        "Description": data["Description"].strip(),
        "Date": date,
        "Status": STATUS_PENDING,
        "Deadline": iso(deadline),
        "ScheduleName": schedule_name(task_id),
        "CreatedAt": iso(now),
        "UpdatedAt": iso(now),
    }
    table().put_item(Item=item, ConditionExpression="attribute_not_exists(TaskId)")

    try:
        _create_expiry_schedule(task_id, user_id, deadline)
    except ClientError:
        logger.exception("Failed to create expiry schedule; rolling back task %s", task_id)
        table().delete_item(Key={"UserId": user_id, "TaskId": task_id})
        return error(500, "Could not schedule task expiry")

    logger.info("Task created", extra={"taskId": task_id, "deadline": item["Deadline"]})
    return response(201, item)


def list_handler(event, _context):
    user_id = user_id_from(event)
    status = (event.get("queryStringParameters") or {}).get("status")

    if status:
        if status not in VALID_STATUSES:
            return error(400, f"status must be one of {', '.join(VALID_STATUSES)}")
        query = {
            "IndexName": "UserStatusIndex",
            "KeyConditionExpression": Key("UserId").eq(user_id) & Key("Status").eq(status),
        }
    else:
        query = {"KeyConditionExpression": Key("UserId").eq(user_id)}

    items = []
    while True:
        page = table().query(**query)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            break
        query["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    items.sort(key=lambda t: t.get("CreatedAt", ""), reverse=True)
    return response(200, {"tasks": items})


def get_handler(event, _context):
    user_id = user_id_from(event)
    task_id = event["pathParameters"]["taskId"]
    item = table().get_item(Key={"UserId": user_id, "TaskId": task_id}).get("Item")
    if not item:
        return error(404, "Task not found")
    return response(200, item)


def update_handler(event, _context):
    """Edit Description/Date; the only allowed status change is Pending -> Completed.

    Completing a task produces a DynamoDB Stream MODIFY record, which drives the
    cancellation workflow (Stream -> Lambda -> SQS FIFO -> Lambda -> DeleteSchedule).
    """
    user_id = user_id_from(event)
    task_id = event["pathParameters"]["taskId"]
    data = _body(event)
    if data is None:
        return error(400, "Request body must be a JSON object")

    names = {"#u": "UpdatedAt"}
    values = {":now": iso(now_utc())}
    sets = ["#u = :now"]
    condition = "attribute_exists(TaskId)"

    if "Description" in data:
        problem = _validate_description(data["Description"])
        if problem:
            return error(400, problem)
        names["#d"] = "Description"
        values[":d"] = data["Description"].strip()
        sets.append("#d = :d")

    if "Date" in data:
        problem = _validate_date(data["Date"])
        if problem:
            return error(400, problem)
        names["#dt"] = "Date"
        values[":dt"] = data["Date"]
        sets.append("#dt = :dt")

    if "Status" in data:
        if data["Status"] != STATUS_COMPLETED:
            return error(400, "Status can only be changed to Completed")
        names["#s"] = "Status"
        values[":completed"] = STATUS_COMPLETED
        values[":pending"] = STATUS_PENDING
        sets.append("#s = :completed")
        names["#ca"] = "CompletedAt"
        sets.append("#ca = :now")
        condition += " AND #s = :pending"

    if len(sets) == 1:
        return error(400, "Nothing to update: provide Description, Date or Status")

    try:
        result = table().update_item(
            Key={"UserId": user_id, "TaskId": task_id},
            UpdateExpression="SET " + ", ".join(sets),
            ConditionExpression=condition,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        existing = table().get_item(Key={"UserId": user_id, "TaskId": task_id}).get("Item")
        if not existing:
            return error(404, "Task not found")
        return error(409, f"Only Pending tasks can be completed (current status: {existing['Status']})")

    return response(200, result["Attributes"])


def delete_handler(event, _context):
    """Deleting a Pending task produces a Stream REMOVE record that cancels its expiry schedule."""
    user_id = user_id_from(event)
    task_id = event["pathParameters"]["taskId"]
    try:
        table().delete_item(
            Key={"UserId": user_id, "TaskId": task_id},
            ConditionExpression="attribute_exists(TaskId)",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return error(404, "Task not found")
        raise
    return response(204)
