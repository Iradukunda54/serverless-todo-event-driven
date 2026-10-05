"""Cognito User Pool Lambda triggers."""

import json
import os

from common import logger, sns


def pre_signup_handler(event, _context):
    """Auto-confirm every sign-up and mark the email as verified (no verification code)."""
    event["response"]["autoConfirmUser"] = True
    if "email" in event["request"].get("userAttributes", {}):
        event["response"]["autoVerifyEmail"] = True
    return event


def _already_subscribed(topic_arn, email):
    paginator = sns().get_paginator("list_subscriptions_by_topic")
    for page in paginator.paginate(TopicArn=topic_arn):
        for sub in page.get("Subscriptions", []):
            if sub.get("Protocol") == "email" and sub.get("Endpoint", "").lower() == email.lower():
                return True
    return False


def post_authentication_handler(event, _context):
    """Subscribe the signed-in user's email to the notifications topic.

    The subscription carries a filter policy on the `userId` message attribute,
    so each user only receives expiry emails for their own tasks. This runs on
    every sign-in, so it is idempotent, and it never raises: a failure here must
    not block the user from signing in.
    """
    try:
        attributes = event["request"]["userAttributes"]
        email = attributes["email"]
        user_id = attributes["sub"]
        topic_arn = os.environ["TOPIC_ARN"]

        if _already_subscribed(topic_arn, email):
            logger.info("Email already subscribed", extra={"userId": user_id})
            return event

        sns().subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint=email,
            Attributes={"FilterPolicy": json.dumps({"userId": [user_id]})},
            ReturnSubscriptionArn=True,
        )
        logger.info("Subscribed email to task notifications", extra={"userId": user_id})
    except Exception:  # noqa: BLE001 - never fail the sign-in
        logger.exception("PostAuthentication subscription failed")
    return event
