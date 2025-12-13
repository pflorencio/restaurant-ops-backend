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


# -----------------------------------------------------------
# 💰 Helpers
# -----------------------------------------------------------
def peso(value):
    try:
        if value is None:
            return "—"
        return f"₱{float(value):,.2f}"
    except Exception:
        return "—"


def send_closing_submission_email(
    store_name: str,
    business_date: str,
    submitted_by: str,
    reason: str,
    closing_fields: dict,
):
    """
    Sends cashier submission snapshot via SendGrid (non-blocking).

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
        # Extract values safely
        # ---------------------------------------------------
        f = closing_fields or {}

        # Sales
        total_sales = peso(f.get("Total Sales"))
        net_sales = peso(f.get("Net Sales"))

        # Payments
        cash = peso(f.get("Cash Payments"))
        card = peso(f.get("Card Payments"))
        digital = peso(f.get("Digital Payments"))
        grab = peso(f.get("Grab Payments"))
        voucher = peso(f.get("Voucher Payments"))
        bank = peso(f.get("Bank Transfer Payments"))

        # Cash handling
        actual_cash = peso(f.get("Actual Cash Counted"))
        cash_float = peso(f.get("Cash Float"))

        # ---------------------------------------------------
        # Email Body (plain text MVP)
        # ---------------------------------------------------
        body = f"""
Closing Report Notification

Store: {store_name}
Business Date: {business_date}
Submitted By: {submitted_by}

Submission Type:
{"First Submission" if reason == "first_submission" else "Re-Submission After Needs Update"}

----------------------------------
SALES SUMMARY
----------------------------------
Total Sales: {total_sales}
Net Sales:   {net_sales}

----------------------------------
PAYMENTS
----------------------------------
Cash:           {cash}
Card:           {card}
Digital:        {digital}
Grab:           {grab}
Voucher:        {voucher}
Bank Transfer:  {bank}

----------------------------------
CASH HANDLING
----------------------------------
Actual Cash Counted: {actual_cash}
Cash Float:          {cash_float}

----------------------------------
This is an automated message.
"""

        # ---------------------------------------------------
        # SendGrid Mail
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
            f"📧 Submission email sent | status={response.status_code} | reason={reason}"
        )

    except Exception as e:
        # ⚠️ Never block saving if email fails
        print("⚠️ Submission email failed (non-blocking):", str(e))
