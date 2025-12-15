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


def percent(value, base):
    try:
        if value is None or base in (None, 0):
            return "—"
        return f"{(float(value) / float(base) * 100):.2f}%"
    except Exception:
        return "—"


# -----------------------------------------------------------
# 📧 CASHIER SUBMISSION EMAIL
# -----------------------------------------------------------
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
        # Email Body
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
        print("⚠️ Submission email failed (non-blocking):", str(e))


# -----------------------------------------------------------
# ✅ VERIFICATION SUMMARY EMAIL
# -----------------------------------------------------------
def send_closing_verification_email(
    store_name: str,
    business_date: str,
    cashier_name: str,
    verified_by: str,
    manager_notes: str,
    closing_fields: dict,
):
    """
    Sends final verification summary after manager approval (non-blocking).
    """

    try:
        if not SENDGRID_API_KEY:
            raise ValueError("SENDGRID_API_KEY not configured")

        f = closing_fields or {}

        net_sales_value = f.get("Net Sales") or 0

        # Budgets
        kitchen_budget = f.get("Kitchen Budget")
        bar_budget = f.get("Bar Budget")
        non_food_budget = f.get("Non Food Budget")
        staff_meal_budget = f.get("Staff Meal Budget")

        total_budgets = sum(
            v or 0
            for v in [
                kitchen_budget,
                bar_budget,
                non_food_budget,
                staff_meal_budget,
            ]
        )

        # Variance / deposits
        variance = f.get("Variance")
        cash_for_deposit = f.get("Cash for Deposit")
        bank_transfer = f.get("Bank Transfer Payments")

        # ---------------------------------------------------
        # Email Body
        # ---------------------------------------------------
        body = f"""
CLOSING VERIFIED ✅

Store: {store_name}
Business Date: {business_date}
Cashier in Charge: {cashier_name}
Verified By: {verified_by}

----------------------------------
MANAGER NOTES
----------------------------------
{manager_notes or "— None —"}

----------------------------------
SALES OVERVIEW
----------------------------------
Total Sales: {peso(f.get("Total Sales"))}
Net Sales:   {peso(net_sales_value)}

----------------------------------
BUDGET UTILIZATION
----------------------------------
Kitchen Budget:     {peso(kitchen_budget)} ({percent(kitchen_budget, net_sales_value)})
Bar Budget:         {peso(bar_budget)} ({percent(bar_budget, net_sales_value)})
Non-Food Budget:    {peso(non_food_budget)} ({percent(non_food_budget, net_sales_value)})
Staff Meal Budget:  {peso(staff_meal_budget)} ({percent(staff_meal_budget, net_sales_value)})

Total Budgets:      {peso(total_budgets)} ({percent(total_budgets, net_sales_value)})

----------------------------------
VARIANCE
----------------------------------
Variance: {peso(variance)}

----------------------------------
DEPOSITS & TRANSFERS
----------------------------------
Cash for Deposit: {peso(cash_for_deposit)}
Bank Transfers:   {peso(bank_transfer)}

----------------------------------
This closing has been verified and locked.
This is an automated message.
"""

        message = Mail(
            from_email=(EMAIL_FROM, EMAIL_FROM_NAME),
            to_emails=TEST_EMAIL_RECIPIENT,
            subject=f"✅ Closing Verified — {store_name} ({business_date})",
            plain_text_content=body,
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        print(f"📧 Verification email sent | status={response.status_code}")

    except Exception as e:
        print("⚠️ Verification email failed (non-blocking):", str(e))