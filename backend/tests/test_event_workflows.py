import json
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from conftest import USER_ID, api_event

import auth_triggers
import cancellation
import common
import expiry
import stream_processor
import tasks_api


def _new_task(description="Task"):
    resp = tasks_api.create_handler(api_event({"Description": description}), None)
    return json.loads(resp["body"])


def _sqs_event(*bodies):
    return {"Records": [{"messageId": f"m{i}", "body": json.dumps(b)} for i, b in enumerate(bodies)]}


def _stream_record(event_name, old=None, new=None, event_id="evt-1", seq="100"):
    def image(status, task_id="t-1"):
        return {"TaskId": {"S": task_id}, "UserId": {"S": USER_ID}, "Status": {"S": status},
                "ScheduleName": {"S": f"task-{task_id}"}}

    ddb = {"SequenceNumber": seq}
    if old:
        ddb["OldImage"] = image(old)
    if new:
        ddb["NewImage"] = image(new)
    return {"eventID": event_id, "eventName": event_name, "dynamodb": ddb}


# ----------------------------------------------------------------- expiry
def test_expiry_marks_pending_task_expired_and_notifies_owner(aws):
    task = _new_task("Pay bills")
    with mock.patch.object(common.sns(), "publish", wraps=common.sns().publish) as publish:
        result = expiry.handler(_sqs_event({"taskId": task["TaskId"], "userId": USER_ID}), None)

    assert result == {"batchItemFailures": []}
    item = common.table().get_item(Key={"UserId": USER_ID, "TaskId": task["TaskId"]})["Item"]
    assert item["Status"] == "Expired"
    assert "ExpiredAt" in item and "NotifiedAt" in item

    publish.assert_called_once()
    kwargs = publish.call_args.kwargs
    # The userId attribute is what the owner's subscription filter policy matches on.
    assert kwargs["MessageAttributes"]["userId"] == {"DataType": "String", "StringValue": USER_ID}
    assert kwargs["TopicArn"] == aws["topic_arn"]
    assert "Pay bills" in kwargs["Subject"]


def test_expiry_is_idempotent_and_skips_non_pending(aws):
    task = _new_task()
    key = {"taskId": task["TaskId"], "userId": USER_ID}
    expiry.handler(_sqs_event(key), None)

    completed = _new_task("done")
    tasks_api.update_handler(api_event({"Status": "Completed"}, path={"taskId": completed["TaskId"]}), None)

    with mock.patch.object(common.sns(), "publish") as publish:
        # Redelivery of an already-processed event, a completed task, and a deleted task.
        result = expiry.handler(
            _sqs_event(key, {"taskId": completed["TaskId"], "userId": USER_ID}, {"taskId": "gone", "userId": USER_ID}),
            None,
        )
    assert result == {"batchItemFailures": []}
    publish.assert_not_called()
    item = common.table().get_item(Key={"UserId": USER_ID, "TaskId": completed["TaskId"]})["Item"]
    assert item["Status"] == "Completed"


def test_expiry_retry_sends_email_if_previous_attempt_failed_after_update(aws):
    task = _new_task()
    key = {"taskId": task["TaskId"], "userId": USER_ID}
    boom = ClientError({"Error": {"Code": "InternalError", "Message": "x"}}, "Publish")
    with mock.patch.object(common.sns(), "publish", side_effect=boom):
        failed = expiry.handler(_sqs_event(key), None)
    assert failed == {"batchItemFailures": [{"itemIdentifier": "m0"}]}

    with mock.patch.object(common.sns(), "publish") as publish:
        assert expiry.handler(_sqs_event(key), None) == {"batchItemFailures": []}
    publish.assert_called_once()


def test_expiry_failure_reports_remaining_fifo_messages(aws):
    a, b = _new_task("a"), _new_task("b")
    event = _sqs_event({"taskId": a["TaskId"], "userId": USER_ID}, {"taskId": b["TaskId"], "userId": USER_ID})
    boom = ClientError({"Error": {"Code": "InternalError", "Message": "x"}}, "Publish")
    with mock.patch.object(common.sns(), "publish", side_effect=boom):
        result = expiry.handler(event, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m0"}, {"itemIdentifier": "m1"}]}


# ----------------------------------------------------------------- stream -> FIFO
@pytest.mark.parametrize(
    "record, reason",
    [
        (_stream_record("MODIFY", old="Pending", new="Completed"), "COMPLETED"),
        (_stream_record("REMOVE", old="Pending"), "DELETED"),
    ],
)
def test_stream_processor_queues_cancellation(aws, record, reason):
    assert stream_processor.handler({"Records": [record]}, None) == {"batchItemFailures": []}
    messages = boto3.client("sqs").receive_message(
        QueueUrl=aws["cancel_queue"], AttributeNames=["All"], MaxNumberOfMessages=10
    )["Messages"]
    assert len(messages) == 1
    body = json.loads(messages[0]["Body"])
    assert body == {"taskId": "t-1", "userId": USER_ID, "scheduleName": "task-t-1", "reason": reason}
    assert messages[0]["Attributes"]["MessageGroupId"] == "t-1"
    assert messages[0]["Attributes"]["MessageDeduplicationId"] == "evt-1"


@pytest.mark.parametrize(
    "record",
    [
        _stream_record("INSERT", new="Pending"),
        _stream_record("MODIFY", old="Pending", new="Expired"),
        _stream_record("MODIFY", old="Pending", new="Pending"),
        _stream_record("REMOVE", old="Completed"),
        _stream_record("REMOVE", old="Expired"),
    ],
)
def test_stream_processor_ignores_other_changes(aws, record):
    stream_processor.handler({"Records": [record]}, None)
    resp = boto3.client("sqs").receive_message(QueueUrl=aws["cancel_queue"])
    assert "Messages" not in resp


def test_stream_processor_reports_failed_sequence_number(aws):
    records = [
        _stream_record("REMOVE", old="Pending", event_id="e1", seq="1"),
        _stream_record("REMOVE", old="Pending", event_id="e2", seq="2"),
    ]
    boom = ClientError({"Error": {"Code": "InternalError", "Message": "x"}}, "SendMessage")
    with mock.patch.object(common.sqs(), "send_message", side_effect=[{}, boom]):
        result = stream_processor.handler({"Records": records}, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "2"}]}


# ----------------------------------------------------------------- FIFO -> cancel schedule
def test_cancellation_deletes_schedule_and_is_idempotent(aws):
    task = _new_task()
    scheduler = boto3.client("scheduler")
    name = f"task-{task['TaskId']}"
    assert scheduler.get_schedule(Name=name, GroupName="task-expiry")

    msg = {"taskId": task["TaskId"], "userId": USER_ID, "scheduleName": name, "reason": "COMPLETED"}
    assert cancellation.handler(_sqs_event(msg), None) == {"batchItemFailures": []}
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule(Name=name, GroupName="task-expiry")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # Duplicate delivery: schedule already gone -> still success.
    assert cancellation.handler(_sqs_event(msg), None) == {"batchItemFailures": []}


def test_cancellation_failure_reports_remaining_messages(aws):
    msgs = [{"scheduleName": f"task-{i}"} for i in range(3)]
    boom = ClientError({"Error": {"Code": "ThrottlingException", "Message": "x"}}, "DeleteSchedule")
    not_found = ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": "x"}}, "DeleteSchedule")
    with mock.patch.object(common.scheduler(), "delete_schedule", side_effect=[not_found, boom]):
        result = cancellation.handler(_sqs_event(*msgs), None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}]}


def test_end_to_end_complete_then_cancel(aws):
    """Create -> complete -> (stream record) -> FIFO message -> schedule deleted."""
    task = _new_task()
    tasks_api.update_handler(api_event({"Status": "Completed"}, path={"taskId": task["TaskId"]}), None)

    record = {
        "eventID": "abc",
        "eventName": "MODIFY",
        "dynamodb": {
            "SequenceNumber": "1",
            "OldImage": {"TaskId": {"S": task["TaskId"]}, "UserId": {"S": USER_ID}, "Status": {"S": "Pending"},
                         "ScheduleName": {"S": task["ScheduleName"]}},
            "NewImage": {"TaskId": {"S": task["TaskId"]}, "UserId": {"S": USER_ID}, "Status": {"S": "Completed"}},
        },
    }
    stream_processor.handler({"Records": [record]}, None)
    msg = boto3.client("sqs").receive_message(QueueUrl=aws["cancel_queue"])["Messages"][0]
    cancellation.handler({"Records": [{"messageId": msg["MessageId"], "body": msg["Body"]}]}, None)

    with pytest.raises(ClientError):
        boto3.client("scheduler").get_schedule(Name=task["ScheduleName"], GroupName="task-expiry")


# ----------------------------------------------------------------- Cognito triggers
def test_pre_signup_auto_confirms():
    event = {"request": {"userAttributes": {"email": "a@example.com"}}, "response": {}}
    out = auth_triggers.pre_signup_handler(event, None)
    assert out["response"] == {"autoConfirmUser": True, "autoVerifyEmail": True}


def _post_auth_event(email="a@example.com", sub=USER_ID):
    return {"request": {"userAttributes": {"email": email, "sub": sub}}, "response": {}}


def test_post_authentication_subscribes_once_with_filter_policy(aws):
    sns = boto3.client("sns")
    for _ in range(3):  # every sign-in triggers it
        out = auth_triggers.post_authentication_handler(_post_auth_event(), None)
        assert out["request"]["userAttributes"]["email"] == "a@example.com"

    subs = sns.list_subscriptions_by_topic(TopicArn=aws["topic_arn"])["Subscriptions"]
    assert len(subs) == 1
    assert subs[0]["Endpoint"] == "a@example.com" and subs[0]["Protocol"] == "email"
    attrs = sns.get_subscription_attributes(SubscriptionArn=subs[0]["SubscriptionArn"])["Attributes"]
    assert json.loads(attrs["FilterPolicy"]) == {"userId": [USER_ID]}


def test_post_authentication_never_blocks_sign_in(aws):
    boom = ClientError({"Error": {"Code": "Throttled", "Message": "x"}}, "Subscribe")
    with mock.patch.object(common.sns(), "subscribe", side_effect=boom):
        event = _post_auth_event("b@example.com")
        assert auth_triggers.post_authentication_handler(event, None) is event
