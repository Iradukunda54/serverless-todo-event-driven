"""End-to-end smoke test against a deployed stack.

Checks:
  * sign-up is auto-confirmed
  * sign-in triggers the PostAuthentication SNS subscription
  * CRUD through API Gateway with the Cognito ID token
  * expiry (task -> Expired + SNS publish) and cancellation (schedule deleted)
  * CORS on preflight and on authorizer errors

Usage:
  python scripts/e2e_smoke.py --email you@example.com [--stack serverless-todo-dev] [--keep-user]

With an example.com address nothing is delivered. With a real address you receive the
SNS confirmation email, and the expiry email once you confirm it.
"""

import argparse
import json
import secrets
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

import boto3

REGION = "eu-west-1"


def outputs(stack):
    cfn = boto3.client("cloudformation", region_name=REGION)
    return {o["OutputKey"]: o["OutputValue"] for o in cfn.describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]}


def http(method, url, token=None, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None)
    if token:
        req.add_header("Authorization", token)
    if body:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as res:
            raw = res.read().decode()
            return res.status, dict(res.headers), json.loads(raw) if raw else None
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        return err.code, dict(err.headers), json.loads(raw) if raw else None


def check(condition, message):
    print(("PASS  " if condition else "FAIL  ") + message)
    if not condition:
        sys.exit(1)


def wait_for(predicate, timeout, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def schedule_exists(scheduler, group, name):
    try:
        scheduler.get_schedule(Name=name, GroupName=group)
        return True
    except scheduler.exceptions.ResourceNotFoundException:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--stack", default="serverless-todo-dev")
    parser.add_argument("--keep-user", action="store_true")
    args = parser.parse_args()

    out = outputs(args.stack)
    api, pool, client_id = out["ApiUrl"], out["UserPoolId"], out["UserPoolClientId"]
    group, topic = out["ScheduleGroupName"], out["NotificationsTopicArn"]
    idp = boto3.client("cognito-idp", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)
    scheduler = boto3.client("scheduler", region_name=REGION)
    password = "Aa1" + secrets.token_urlsafe(12)

    # ------------------------------------------------------------ auth
    idp.sign_up(ClientId=client_id, Username=args.email, Password=password,
                UserAttributes=[{"Name": "email", "Value": args.email}])
    user = idp.admin_get_user(UserPoolId=pool, Username=args.email)
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    check(user["UserStatus"] == "CONFIRMED", "sign-up auto-confirmed (PreSignUp)")
    check(attrs.get("email_verified") == "true", "email auto-verified")

    auth = idp.initiate_auth(ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
                             AuthParameters={"USERNAME": args.email, "PASSWORD": password})
    token = auth["AuthenticationResult"]["IdToken"]
    check(bool(token), "sign-in returned an ID token")

    def my_subscription():
        for page in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=topic):
            for s in page["Subscriptions"]:
                if s["Endpoint"] == args.email:
                    return s
        return None

    sub = wait_for(my_subscription, 20, 2)
    check(sub is not None, f"PostAuthentication subscribed email to SNS ({sub and sub['SubscriptionArn']})")
    if sub and sub["SubscriptionArn"] not in ("PendingConfirmation", "Deleted"):
        policy = json.loads(sns.get_subscription_attributes(SubscriptionArn=sub["SubscriptionArn"])
                            ["Attributes"].get("FilterPolicy", "{}"))
        check(policy == {"userId": [attrs["sub"]]}, "subscription filter policy is the user's sub")

    # ------------------------------------------------------------ CORS + auth errors
    status, headers, _ = http("OPTIONS", f"{api}/tasks", headers={
        "Origin": "https://example.com", "Access-Control-Request-Method": "POST"})
    check(status == 200 and headers.get("Access-Control-Allow-Origin") == "*", "CORS preflight OK")
    status, headers, _ = http("GET", f"{api}/tasks")
    check(status == 401 and headers.get("Access-Control-Allow-Origin") == "*", "unauthenticated -> 401 with CORS")

    def find(task_id):
        _, _, body = http("GET", f"{api}/tasks", token)
        return next((t for t in body["tasks"] if t["TaskId"] == task_id), None)

    # ------------------------------------------------------------ CRUD
    status, _, expiring = http("POST", f"{api}/tasks", token, {"Description": "e2e: should expire", "ExpiresInMinutes": 1})
    check(status == 201 and expiring["Status"] == "Pending", f"create task expiring in 1 minute -> {status} {expiring if status != 201 else ''}")
    soon = expiring["Deadline"]
    status, _, default = http("POST", f"{api}/tasks", token, {"Description": "e2e: default deadline"})
    created = datetime.fromisoformat(default["CreatedAt"].replace("Z", "+00:00"))
    deadline = datetime.fromisoformat(default["Deadline"].replace("Z", "+00:00"))
    check(status == 201 and deadline - created == timedelta(minutes=5), "default deadline = creation + 5 min")
    _, _, to_complete = http("POST", f"{api}/tasks", token, {"Description": "e2e: complete me"})
    _, _, to_delete = http("POST", f"{api}/tasks", token, {"Description": "e2e: delete me"})
    for t in (expiring, default, to_complete, to_delete):
        check(schedule_exists(scheduler, group, t["ScheduleName"]), f"schedule {t['ScheduleName']} exists")

    status, _, listed = http("GET", f"{api}/tasks", token)
    check(status == 200 and len(listed["tasks"]) == 4, "list returns 4 tasks")
    check(default["TaskId"] in {t["TaskId"] for t in listed["tasks"]}, "listed tasks include the new task")
    status, _, edited = http("PUT", f"{api}/tasks/{default['TaskId']}", token, {"Description": "e2e: edited", "Date": "2026-12-31"})
    check(status == 200 and edited["Description"] == "e2e: edited", "update description/date")

    # ------------------------------------------------------------ cancellation
    status, _, completed = http("PUT", f"{api}/tasks/{to_complete['TaskId']}", token, {"Status": "Completed"})
    check(status == 200 and completed["Status"] == "Completed", "complete task")
    status, _, _ = http("DELETE", f"{api}/tasks/{to_delete['TaskId']}", token)
    check(status == 204, "delete task")
    for t, why in ((to_complete, "completed"), (to_delete, "deleted")):
        gone = wait_for(lambda t=t: not schedule_exists(scheduler, group, t["ScheduleName"]), 90)
        check(gone, f"expiry schedule cancelled for {why} task (Streams -> FIFO -> Lambda)")
    check(schedule_exists(scheduler, group, default["ScheduleName"]), "untouched pending task keeps its schedule")

    # ------------------------------------------------------------ expiry
    print(f"...waiting for the 1-minute task to expire (deadline {soon})")

    def expired():
        t = find(expiring["TaskId"])
        return t if t and t["Status"] == "Expired" and t.get("NotifiedAt") else None

    task = wait_for(expired, 180, 10)
    check(task is not None, "task expired at deadline and SNS notification published (NotifiedAt set)")
    done = find(to_complete["TaskId"])
    check(done["Status"] == "Completed", "completed task was not expired")
    status, _, pending = http("GET", f"{api}/tasks?status=Expired", token)
    check([t["TaskId"] for t in pending["tasks"]] == [expiring["TaskId"]], "status filter (GSI) returns the expired task")

    # ------------------------------------------------------------ cleanup
    for t in (expiring, default, to_complete):
        http("DELETE", f"{api}/tasks/{t['TaskId']}", token)
    if not args.keep_user:
        idp.admin_delete_user(UserPoolId=pool, Username=args.email)
        sub = my_subscription()
        if sub and sub["SubscriptionArn"].startswith("arn:"):
            sns.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
    print("All checks passed.")


if __name__ == "__main__":
    main()
