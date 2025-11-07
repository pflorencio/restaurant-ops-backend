import os
import smtplib
from email.message import EmailMessage
from typing import Optional
from datetime import date as dt_date, datetime
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pyairtable import Table

# --- Load environment early ---
load_dotenv()

# ---------- App Initialization ----------
app = FastAPI(title="Daily Sales & Cash Management API", version="0.3.9")

origins = [
    "http://localhost:5000",  # ✅ local dev via Vite
    "http://127.0.0.1:5000",  # ✅ fallback local
    "https://restaurant-ops-dashboard.onrender.com",  # ✅ future deployed frontend
    "https://restaurant-ops-frontend.vercel.app",     # ✅ optional Vercel deploy
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

)

# ✅ OPTIONS fallback for browsers (preflight support)
@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers[
        "Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers[
        "Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# ---------- Airtable Helpers ----------
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
    net_sales: Optional[float] = 0.0
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
    variance_cash: Optional[float] = 0.0
    total_budgets: Optional[float] = 0.0
    cash_for_deposit: Optional[float] = 0.0
    transfer_needed: Optional[float] = 0.0
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


# ---------- UPSERT (Create or Update) ----------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    """Create or update a daily closing record in Airtable (upsert by store + date) with lock check."""
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        safe_store = payload.store.replace("'", "''")
        date_iso = payload.business_date.isoformat()
        formula = f"AND({{Store}}='{safe_store}', {{Date}}='{date_iso}')"
        print("🔍 UPSERT formula:", formula)

        existing = table.all(formula=formula, max_records=1)

        fields = {
            "Date": date_iso,
            "Store": payload.store,
            "Total Sales": payload.total_sales,
            "Net Sales": payload.net_sales,
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
            "Last Updated At": datetime.now().isoformat(),
        }
        fields = {k: v for k, v in fields.items() if v is not None}

        if existing:
            record = existing[0]
            record_id = record["id"]
            current_lock = record["fields"].get("Lock Status", "Unlocked")

            if current_lock in ["Locked", "Verified"]:
                raise HTTPException(
                    status_code=403,
                    detail=
                    f"Record for {payload.store} on {payload.business_date} is locked and cannot be modified.",
                )

            fields["Lock Status"] = "Locked"
            updated = table.update(record_id, fields)
            print(
                f"🔁 Updated and locked record for {payload.store} on {payload.business_date}"
            )
            return {
                "status":
                "updated_locked",
                "id":
                record_id,
                "lock_status":
                updated.get("fields", {}).get("Lock Status", "Locked"),
                "fields":
                updated.get("fields", {}),
            }

        fields["Lock Status"] = "Locked"
        created = table.create(fields)
        print(
            f"🆕 Created and locked new record for {payload.store} on {payload.business_date}"
        )
        return {
            "status": "created_locked",
            "id": created.get("id"),
            "lock_status": created.get("fields",
                                       {}).get("Lock Status", "Locked"),
            "fields": created.get("fields", {}),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error during upsert:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Secure Unlock (Manager PIN Protected) ----------
class UnlockPayload(BaseModel):
    pin: str


def _constant_time_equal(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= ord(x) ^ ord(y)
    return result == 0


@app.post("/closings/{record_id}/unlock")
def unlock_closing(record_id: str, payload: UnlockPayload):
    try:
        manager_pin = os.getenv("MANAGER_PIN")
        if not manager_pin:
            raise HTTPException(status_code=500,
                                detail="MANAGER_PIN not configured on server")

        if not _constant_time_equal(payload.pin or "", manager_pin):
            raise HTTPException(status_code=401, detail="Invalid manager PIN")

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        updated = table.update(
            record_id,
            {
                "Lock Status": "Unlocked",
                "Unlocked At": datetime.now().isoformat()
            },
        )
        print(f"🔓 Record {record_id} unlocked by manager.")
        return {
            "status": "unlocked",
            "id": record_id,
            "lock_status": updated.get("fields",
                                       {}).get("Lock Status", "Unlocked"),
            "fields": updated.get("fields", {}),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Utility: Airtable Filter ----------
def _airtable_filter_formula(business_date: Optional[str],
                             store: Optional[str]) -> Optional[str]:
    clauses = []
    if business_date:
        clauses.append(
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
        )
    if store:
        safe_store = store.replace("'", "''")
        clauses.append(f"{{Store}}='{safe_store}'")
    if not clauses:
        return None
    return "AND(" + ",".join(clauses) + ")"


# ---------- Unique Record Fetch (Prefill) ----------
@app.get("/closings/unique")
def get_unique_closing(business_date: str = Query(...),
                       store: str = Query(...)):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        safe_store = store.replace("'", "''")
        formula = f"AND({{Store}}='{safe_store}', {{Date}}='{business_date}')"
        print("🔍 /closings/unique formula:", formula)
        records = table.all(formula=formula, max_records=1)
        if not records:
            raise HTTPException(status_code=404,
                                detail="No record for given store and date.")
        r = records[0]
        fields = r.get("fields", {})
        return {
            "id": r.get("id"),
            "lock_status": fields.get("Lock Status", "Unlocked"),
            "fields": fields
        }
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in /closings/unique:", e)
        raise HTTPException(status_code=500,
                            detail=f"Airtable query error: {str(e)}")


# ---------- Listing ----------
@app.get("/closings")
def list_closings(business_date: Optional[str] = Query(None),
                  store: Optional[str] = Query(None),
                  limit: int = Query(50)):
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
            } for r in records]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/closings/{record_id}")
def get_closing_by_id(record_id: str):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        record = table.get(record_id)
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        return {"id": record.get("id"), "fields": record.get("fields", {})}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Inline Editing (PATCH) ----------
@app.patch("/closings/{record_id}")
async def update_closing_field(record_id: str, request: Request):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        payload = await request.json()
        if not payload or not isinstance(payload, dict):
            raise HTTPException(status_code=400,
                                detail="Empty or invalid JSON payload")

        payload = {k: v for k, v in payload.items() if v is not None}
        payload["Last Updated At"] = datetime.now().isoformat()

        print(f"🔹 PATCH payload received for {record_id}: {payload}")
        updated = table.update(record_id, payload)
        print(f"✅ Airtable update response: {updated}")

        return {
            "id": updated.get("id"),
            "fields": updated.get("fields", {}),
            "updated_fields": list(payload.keys()),
            "status": "success",
        }

    except Exception as e:
        import traceback
        print("❌ ERROR during update:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")


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
    try:
        if not business_date:
            raise HTTPException(
                status_code=400,
                detail="business_date query param required (YYYY-MM-DD)")

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        formula = _airtable_filter_formula(business_date, store)
        records = table.all(max_records=200, formula=formula)
        fields_only = [r.get("fields", {}) for r in records]

        body = _build_daily_summary_text(business_date, fields_only)
        return {"preview": body}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Verification ----------
@app.post("/verify")
def verify_closing(payload: dict):
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
    port = int(os.environ.get("PORT", 8080))
    print(f"✅ Server starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
