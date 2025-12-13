import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -----------------------------------------------------------
# 🔐 Environment Variables
# -----------------------------------------------------------
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

EMAIL_FROM = os.getenv("EMAIL_USER")  # sender email
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Closing Report App")

TEST_EMAIL_RECIPIENT = os.getenv("TEST_EMAIL_RECIPIENT", EMAIL_FROM)


def send_closing_submission_email(
    store_name: str,
    business_date: str,
    submitted_by: str,
    reason: str,
):
    """
    Sends email via SendGrid (non-blocking caller).

    reason:
    - first_submission
    - resubmission_after_update
    """

    try:
        if not SENDGRID_API_KEY:
            raise ValueError("SENDGRID_API_KEY not configured")

        # ---------------------------------------------------
        # Subject
        # ---------------------------------------------------
        subject_prefix = "🧾 Closing Submitted"
        if reason == "resubmission_after_update":
            subject_prefix = "🔄 Closing Re-Submitted"

        subject = f"{subject_prefix} — {store_name} ({business_date})"

        # ---------------------------------------------------
        # Email Body (plain text for MVP)
        # ---------------------------------------------------
        body = f"""
Closing Report Notification

Store: {store_name}
Business Date: {business_date}
Submitted By: {submitted_by}

Submission Type:
{"First Submission" if reason == "first_submission" else "Re-Submission After Needs Update"}

This is an automated message.
"""

        # ---------------------------------------------------
        # Build SendGrid Mail
        # ---------------------------------------------------
        message = Mail(
            from_email=(EMAIL_FROM, EMAIL_FROM_NAME),
            to_emails=TEST_EMAIL_RECIPIENT,
            subject=subject,
            plain_text_content=body,
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        print(
            f"📧 Email sent via SendGrid | status={response.status_code} | reason={reason}"
        )

    except Exception as e:
        # ⚠️ Never block saving if email fails
        print("⚠️ SendGrid email failed (non-blocking):", str(e))
