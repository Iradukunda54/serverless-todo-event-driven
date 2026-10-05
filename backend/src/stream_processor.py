"""DynamoDB Stream consumer: turns "task finished early" changes into cancellation messages.

The event source mapping filter only delivers:
  * MODIFY records where the status went Pending -> Completed
  * REMOVE records where the deleted task was still Pending
Each one becomes a message on the cancellation SQS FIFO queue, grouped by TaskId
(ordered per task) and de-duplicated by the stream record's eventID (stable across retries).
"""

import json
import os

from common import STATUS_COMPLETED, STATUS_PENDING, logger, sqs


def _status(image):
    return (image or {}).get("Status", {}).get("S")


def _cancellation_reason(record):
    ddb = record["dynamodb"]
    old, new = _status(ddb.get("OldImage")), _status(ddb.get("NewImage"))
    if record["eventName"] == "MODIFY" and old == STATUS_PENDING and new == STATUS_COMPLETED:
        return "COMPLETED"
    if record["eventName"] == "REMOVE" and old == STATUS_PENDING:
        return "DELETED"
    return None


def handler(event, _context):
    queue_url = os.environ["CANCELLATION_QUEUE_URL"]
    for record in event.get("Records", []):
        try:
            reason = _cancellation_reason(record)
            if reason is None:
                continue
            image = record["dynamodb"].get("OldImage") or {}
            task_id = image["TaskId"]["S"]
            message = {
                "taskId": task_id,
                "userId": image["UserId"]["S"],
                "scheduleName": image.get("ScheduleName", {}).get("S", f"task-{task_id}"),
                "reason": reason,
            }
            sqs().send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
                MessageGroupId=task_id,
                MessageDeduplicationId=record["eventID"],
            )
            logger.info("Queued expiry cancellation", extra=message)
        except Exception:
            logger.exception("Failed to queue cancellation for stream record %s", record.get("eventID"))
            # Checkpoint here: this record and everything after it are retried.
            return {"batchItemFailures": [{"itemIdentifier": record["dynamodb"]["SequenceNumber"]}]}
    return {"batchItemFailures": []}
