import os
import smtplib
from email.message import EmailMessage
from typing import Optional
from datetime import date as dt_date, datetime
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pyairtable import Table

# --- Load environment early ---
load_dotenv()

# ---------- App Initialization ----------
app = FastAPI(title="Daily Sales & Cash Management API", version="0.3.0")

# ✅ Universal CORS (must come first to affect all responses)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins (safe for internal Replit dev)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ Middleware to enforce HTTPS (after CORS)
@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers[
        "Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers[
        "Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# ✅ OPTIONS fallback for browsers (covers Replit preflight)
@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers[
        "Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers[
        "Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# ---------- Airtable helpers ----------
def _airtable_table(table_name: str) -> Table:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")
    return Table(api_key, base_id, table_name)


DAILY_CLOSINGS_TABLE = os.getenv("AIRTABLE_DAILY_CLOSINGS_TABLE",
                                 "Daily Closings")


# ---------- Models ----------
class ClosingCreate(BaseModel):
    business_date: dt_date = Field(...,
                                   description="Business date (YYYY-MM-DD)")
    store: str
    total_sales: Optional[float] = 0.0
    cash_payments: Optional[float] = 0.0
    card_payments: Optional[float] = 0.0
    digital_payments: Optional[float] = 0.0
    grab_payments: Optional[float] = 0.0
    bank_transfer_payments: Optional[float] = 0.0
    voucher_payments: Optional[float] = 0.0
    marketing_expenses: Optional[float] = 0.0
    actual_cash_counted: Optional[float] = 0.0
    cash_float: Optional[float] = 0.0
    kitchen_budget: Optional[float] = 0.0
    bar_budget: Optional[float] = 0.0
    non_food_budget: Optional[float] = 0.0
    staff_meal_budget: Optional[float] = 0.0
    attachments: Optional[str] = None
    submitted_by: Optional[str] = None


# ---------- Basic Routes ----------
@app.get("/")
def root():
    return {"status": "ok", "service": "daily-sales-api"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/airtable/test")
def airtable_test():
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_name = os.getenv("AIRTABLE_TABLE_NAME")
    if not (base_id and api_key and table_name):
        return {
            "error":
            "Missing one or more env vars: AIRTABLE_BASE_ID, AIRTABLE_API_KEY, AIRTABLE_TABLE_NAME"
        }

    try:
        table = Table(api_key, base_id, table_name)
        records = table.all(max_records=3)
        return {"records": [r.get("fields", {}) for r in records]}
    except Exception as e:
        return {"error": str(e)}


# ---------- Create & List ----------
@app.post("/closings")
def create_closing(payload: ClosingCreate):
    """Create a new daily closing record in Airtable."""
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        fields = {
            "Date": payload.business_date.isoformat(),
            "Store": payload.store,
            "Total Sales": payload.total_sales,
            "Cash Payments": payload.cash_payments,
            "Card Payments": payload.card_payments,
            "Digital Payments": payload.digital_payments,
            "Grab Payments": payload.grab_payments,
            "Bank Transfer Payments": payload.bank_transfer_payments,
            "Voucher Payments": payload.voucher_payments,
            "Marketing Expenses": payload.marketing_expenses,
            "Actual Cash Counted": payload.actual_cash_counted,
            "Cash Float": payload.cash_float,
            "Kitchen Budget": payload.kitchen_budget,
            "Bar Budget": payload.bar_budget,
            "Non Food Budget": payload.non_food_budget,
            "Staff Meal Budget": payload.staff_meal_budget,
            "Submitted By": payload.submitted_by,
        }
        fields = {k: v for k, v in fields.items() if v is not None}
        created = table.create(fields)
        return {"id": created.get("id"), "fields": created.get("fields", {})}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _airtable_filter_formula(business_date: Optional[str],
                             store: Optional[str]) -> Optional[str]:
    clauses = []
    if business_date:
        clauses.append(
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
        )
    if store:
        safe_store = store.replace("'", "\\'")
        clauses.append(f"{{Store}} = '{safe_store}'")
    if not clauses:
        return None
    return "AND(" + ",".join(clauses) + ")"


@app.get("/closings")
def list_closings(
        business_date: Optional[str] = Query(None),
        store: Optional[str] = Query(None),
        limit: int = Query(50, ge=1, le=200),
):
    """Fetch daily closings filtered by date and/or store."""
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        formula = _airtable_filter_formula(business_date, store)
        records = table.all(max_records=limit, formula=formula)
        return {
            "count":
            len(records),
            "records": [{
                "id": r.get("id"),
                "fields": r.get("fields", {})
            } for r in records],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Summary Report ----------
def _format_php(amount: float | None) -> str:
    try:
        return f"₱{amount:,.0f}"
    except Exception:
        return "₱0"


def _build_daily_summary_text(business_date: str, records: list[dict]) -> str:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        store = r.get("Store", "Unknown Store")
        grouped[store].append(r)

    lines = [f"Daily Sales Summary — {business_date}\n"]
    for store, rows in grouped.items():
        total_sales = sum([(r.get("Total Sales") or 0) for r in rows])
        cash_payments = sum([(r.get("Cash Payments") or 0) for r in rows])
        actual_cash_counted = sum([(r.get("Actual Cash Counted") or 0)
                                   for r in rows])
        cash_float = sum([(r.get("Cash Float") or 0) for r in rows])
        variance = actual_cash_counted - cash_payments - cash_float

        lines.append(f"📍 {store}")
        lines.append(
            f"- Total Sales: {_format_php(total_sales)} | Variance: {_format_php(variance)}"
        )
        lines.append("───────────────────────────────\n")
    return "\n".join(lines)


@app.get("/reports/daily-summary")
def report_daily_summary(
        business_date: Optional[str] = Query(None),
        store: Optional[str] = Query(None),
        send: bool = Query(False),
):
    """Generate or email daily summary."""
    try:
        if not business_date:
            raise HTTPException(
                status_code=400,
                detail="business_date query param required (YYYY-MM-DD)",
            )

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        formula = _airtable_filter_formula(business_date, store)
        records = table.all(max_records=200, formula=formula)
        fields_only = [r.get("fields", {}) for r in records]

        body = _build_daily_summary_text(business_date, fields_only)
        if send:
            _send_email(f"Daily Sales Summary — {business_date}", body)
            return {"sent": True}
        return {"preview": body}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Verification ----------
@app.post("/verify")
def verify_closing(payload: dict):
    """Update verification fields for a Daily Closing record."""
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        record_id = payload.get("record_id")
        status = payload.get("status")
        verified_by = payload.get("verified_by")

        if status not in ["Verified", "Flagged"]:
            raise HTTPException(
                status_code=400,
                detail="status must be 'Verified' or 'Flagged'")

        fields = {
            "Verified Status": status,
            "Verified By": verified_by,
            "Verified At": datetime.now().isoformat(),
        }

        updated = table.update(record_id, fields)
        return {"id": updated.get("id"), "fields": updated.get("fields", {})}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/unverified")
def list_unverified(limit: int = Query(100, ge=1, le=500)):
    """Return all unverified or flagged Daily Closings."""
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        formula = "OR({Verified Status}='Pending',{Verified Status}='Flagged',NOT({Verified Status}))"
        records = table.all(max_records=limit, formula=formula)
        data = []
        for r in records:
            f = r.get("fields", {})
            data.append({
                "id": r.get("id"),
                "Date": f.get("Date"),
                "Store": f.get("Store"),
                "Total Sales": f.get("Total Sales"),
                "Variance": f.get("Variance (Cash Payments vs Actual)"),
                "Cash for Deposit": f.get("Cash for Deposit"),
                "Transfer Needed": f.get("Transfer Needed"),
                "Verified Status": f.get("Verified Status", "Pending"),
                "Verified By": f.get("Verified By"),
            })
        return {"count": len(data), "records": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Entrypoint ----------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))  # use 8080, not 8000
    print(f"✅ Server starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
