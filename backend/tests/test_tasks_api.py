import json
from datetime import timedelta

import boto3
from conftest import OTHER_USER_ID, USER_ID, api_event

import common
import tasks_api


def _create(description="Write report", **extra):
    resp = tasks_api.create_handler(api_event({"Description": description, **extra}), None)
    return resp, json.loads(resp["body"])


def test_create_task_defaults_and_schedule(aws):
    before = common.now_utc()
    resp, task = _create()

    assert resp["statusCode"] == 201
    assert resp["headers"]["Access-Control-Allow-Origin"] == "*"
    assert task["UserId"] == USER_ID
    assert task["Status"] == "Pending"
    assert task["Date"] == before.strftime("%Y-%m-%d")
    deadline = common.parse_iso(task["Deadline"])
    assert timedelta(minutes=4, seconds=55) <= deadline - before <= timedelta(minutes=5, seconds=5)

    schedule = boto3.client("scheduler").get_schedule(Name=f"task-{task['TaskId']}", GroupName="task-expiry")
    assert schedule["ScheduleExpression"] == f"at({deadline.strftime('%Y-%m-%dT%H:%M:%S')})"
    assert schedule["ScheduleExpressionTimezone"] == "UTC"
    assert schedule["ActionAfterCompletion"] == "DELETE"
    assert schedule["Target"]["SqsParameters"]["MessageGroupId"] == task["TaskId"]
    assert json.loads(schedule["Target"]["Input"]) == {"taskId": task["TaskId"], "userId": USER_ID}


def test_create_with_custom_deadline(aws):
    deadline = common.iso(common.now_utc() + timedelta(minutes=2))
    resp, task = _create(Deadline=deadline, Date="2026-12-01")
    assert resp["statusCode"] == 201
    assert task["Deadline"] == deadline
    assert task["Date"] == "2026-12-01"


def test_create_with_relative_deadline(aws):
    before = common.now_utc()
    resp, task = _create(ExpiresInMinutes=2)
    assert resp["statusCode"] == 201
    delta = common.parse_iso(task["Deadline"]) - before
    assert timedelta(minutes=1, seconds=55) <= delta <= timedelta(minutes=2, seconds=5)
    for bad in (0, -1, "5", 1.5, True, 10**7):
        assert _create(ExpiresInMinutes=bad)[0]["statusCode"] == 400


def test_create_validation(aws):
    assert _create(description="  ")[0]["statusCode"] == 400
    assert _create(Date="12/01/2026")[0]["statusCode"] == 400
    soon = common.iso(common.now_utc() + timedelta(seconds=10))
    assert _create(Deadline=soon)[0]["statusCode"] == 400
    assert _create(Deadline="not-a-date")[0]["statusCode"] == 400
    assert tasks_api.create_handler(api_event("not json"), None)["statusCode"] == 400


def test_list_get_and_isolation(aws):
    _, first = _create("first")
    _, second = _create("second")
    tasks_api.create_handler(api_event({"Description": "other user"}, user_id=OTHER_USER_ID), None)

    listed = json.loads(tasks_api.list_handler(api_event(), None)["body"])["tasks"]
    assert {t["TaskId"] for t in listed} == {first["TaskId"], second["TaskId"]}

    pending = json.loads(tasks_api.list_handler(api_event(query={"status": "Pending"}), None)["body"])["tasks"]
    assert len(pending) == 2
    assert tasks_api.list_handler(api_event(query={"status": "Bogus"}), None)["statusCode"] == 400

    # Another user cannot see these tasks.
    other = json.loads(tasks_api.list_handler(api_event(user_id=OTHER_USER_ID), None)["body"])["tasks"]
    assert [t["Description"] for t in other] == ["other user"]


def test_update_description_and_complete(aws):
    _, task = _create()
    path = {"taskId": task["TaskId"]}

    edited = tasks_api.update_handler(api_event({"Description": "Edited", "Date": "2026-11-11"}, path=path), None)
    assert edited["statusCode"] == 200
    assert json.loads(edited["body"])["Description"] == "Edited"

    done = tasks_api.update_handler(api_event({"Status": "Completed"}, path=path), None)
    body = json.loads(done["body"])
    assert done["statusCode"] == 200
    assert body["Status"] == "Completed"
    assert "CompletedAt" in body

    # Completing twice, or reopening, is rejected.
    assert tasks_api.update_handler(api_event({"Status": "Completed"}, path=path), None)["statusCode"] == 409
    assert tasks_api.update_handler(api_event({"Status": "Pending"}, path=path), None)["statusCode"] == 400


def test_update_missing_task_does_not_create_item(aws):
    path = {"taskId": "does-not-exist"}
    resp = tasks_api.update_handler(api_event({"Description": "x"}, path=path), None)
    assert resp["statusCode"] == 404
    assert common.table().get_item(Key={"UserId": USER_ID, "TaskId": "does-not-exist"}).get("Item") is None


def test_update_requires_fields(aws):
    _, task = _create()
    resp = tasks_api.update_handler(api_event({}, path={"taskId": task["TaskId"]}), None)
    assert resp["statusCode"] == 400


def test_delete(aws):
    _, task = _create()
    path = {"taskId": task["TaskId"]}
    assert tasks_api.delete_handler(api_event(path=path), None)["statusCode"] == 204
    assert tasks_api.delete_handler(api_event(path=path), None)["statusCode"] == 404
