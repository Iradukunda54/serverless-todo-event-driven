"""SQS FIFO consumer for scheduled expiry events (EventBridge Scheduler -> task-expiry.fifo).

For each event:
  1. Conditionally set Status = Expired, only if the task is still Pending.
  2. Publish an email notification to SNS with a `userId` message attribute,
     which the owner's subscription filter policy matches.
  3. Record NotifiedAt so a retried message does not send a second email.
"""

import json
import os

from botocore.exceptions import ClientError

from common import STATUS_EXPIRED, STATUS_PENDING, iso, logger, now_utc, sns, table


def _expire(key):
    """Return the task to notify about, or None if there is nothing to do."""
    try:
        return table().update_item(
            Key=key,
            UpdateExpression="SET #s = :expired, ExpiredAt = :now, UpdatedAt = :now",
            ConditionExpression="#s = :pending",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={
                ":expired": STATUS_EXPIRED,
                ":pending": STATUS_PENDING,
                ":now": iso(now_utc()),
            },
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

    # Not Pending: deleted, completed, or expired by an earlier attempt.
    item = table().get_item(Key=key, ConsistentRead=True).get("Item")
    if item and item.get("Status") == STATUS_EXPIRED and "NotifiedAt" not in item:
        return item  # expired earlier but the email was never sent
    return None


def _notify(task):
    subject = f"Task expired: {task['Description']}"
    if len(subject) > 100:
        subject = subject[:97] + "..."
    message = (
        "Your task has expired because it was not completed before its deadline.\n\n"
        f"Task:        {task['Description']}\n"
        f"Date:        {task['Date']}\n"
        f"Deadline:    {task['Deadline']}\n"
        f"Task ID:     {task['TaskId']}\n"
    )
    sns().publish(
        TopicArn=os.environ["TOPIC_ARN"],
        Subject=subject,
        Message=message,
        MessageAttributes={"userId": {"DataType": "String", "StringValue": task["UserId"]}},
    )


def handler(event, _context):
    records = event.get("Records", [])
    for index, record in enumerate(records):
        try:
            body = json.loads(record["body"])
            key = {"UserId": body["userId"], "TaskId": body["taskId"]}
            task = _expire(key)
            if task is None:
                logger.info("Task no longer pending; nothing to expire", extra=key)
                continue
            _notify(task)
            table().update_item(
                Key=key,
                UpdateExpression="SET NotifiedAt = :now",
                ConditionExpression="attribute_exists(TaskId)",
                ExpressionAttributeValues={":now": iso(now_utc())},
            )
            logger.info("Task expired and owner notified", extra=key)
        except Exception:
            logger.exception("Expiry processing failed for message %s", record.get("messageId"))
            # FIFO: fail this message and every later one to preserve ordering.
            return {"batchItemFailures": [{"itemIdentifier": r["messageId"]} for r in records[index:]]}
    return {"batchItemFailures": []}
