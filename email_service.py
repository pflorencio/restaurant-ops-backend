import os
import smtplib
import socket
from email.message import EmailMessage

# -----------------------------------------------------------
# 📧 Email Configuration (from Render env vars)
# -----------------------------------------------------------
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME")

# For MVP: send everything to one test inbox
TEST_EMAIL_RECIPIENT = os.getenv("TEST_EMAIL_RECIPIENT", EMAIL_USER)

# ⏱️ Global socket timeout (prevents hanging requests)
socket.setdefaulttimeout(5)


def send_closing_submission_email(
    store_name: str,
    business_date: str,
    submitted_by: str,
    reason: str,
):
    """
    Sends a notification email when a closing is submitted.

    reason:
    - "first_submission"
    - "resubmission_after_update"

    IMPORTANT:
    - This function is NON-BLOCKING for the API
    - Any failure here must NOT break the save flow
    """

    try:
        # ---------------------------------------------------
        # Subject
        # ---------------------------------------------------
        subject_prefix = "🧾 Closing Submitted"
        if reason == "resubmission_after_update":
            subject_prefix = "🔄 Closing Re-Submitted"

        subject = f"{subject_prefix} — {store_name} ({business_date})"

        # ---------------------------------------------------
        # Body
        # ---------------------------------------------------
        body = f"""
Closing Report Notification

Store: {store_name}
Business Date: {business_date}
Submitted By: {submitted_by}

Submission Type:
{'First Submission' if reason == 'first_submission' else 'Re-Submission After Needs Update'}

This is an automated message.
"""

        # ---------------------------------------------------
        # Email Message
        # ---------------------------------------------------
        msg = EmailMessage()
        msg["From"] = f"{EMAIL_FROM_NAME} <{EMAIL_USER}>"
        msg["To"] = TEST_EMAIL_RECIPIENT
        msg["Subject"] = subject
        msg.set_content(body)

        # ---------------------------------------------------
        # SMTP Send (with timeout)
        # ---------------------------------------------------
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=5) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)

        print(
            f"📧 Email sent | {store_name} | {business_date} | {reason}"
        )

    except Exception as e:
        # 🚨 CRITICAL: Never block or fail the request
        print("⚠️ Email send failed (non-blocking):", e)
