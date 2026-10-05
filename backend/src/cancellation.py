"""SQS FIFO consumer: cancels the scheduled expiry event of a completed or deleted task.

Idempotent: a schedule that no longer exists (already cancelled, or already fired
and auto-deleted) counts as success.
"""

import json
import os

from botocore.exceptions import ClientError

from common import logger, scheduler


def handler(event, _context):
    group = os.environ["SCHEDULE_GROUP"]
    records = event.get("Records", [])
    for index, record in enumerate(records):
        try:
            message = json.loads(record["body"])
            try:
                scheduler().delete_schedule(Name=message["scheduleName"], GroupName=group)
                logger.info("Expiry schedule cancelled", extra=message)
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
                logger.info("Expiry schedule already gone", extra=message)
        except Exception:
            logger.exception("Cancellation failed for message %s", record.get("messageId"))
            # FIFO: fail this message and every later one to preserve per-task ordering.
            return {"batchItemFailures": [{"itemIdentifier": r["messageId"]} for r in records[index:]]}
    return {"batchItemFailures": []}
