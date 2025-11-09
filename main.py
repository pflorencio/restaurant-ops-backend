import os
import smtplib
import json
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
app = FastAPI(title="Daily Sales & Cash Management API", version="0.5.1")

origins = [
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "https://restaurant-ops-dashboard-pflorencio.replit.app",
    "https://restaurant-ops-backend.onrender.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# ---------- Airtable Helpers ----------
def _airtable_table(table_name: str) -> Table:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")
    return Table(api_key, base_id, table_name)


DAILY_CLOSINGS_TABLE = os.getenv("AIRTABLE_DAILY_CLOSINGS_TABLE", "Daily Closings")
HISTORY_TABLE = os.getenv("AIRTABLE_HISTORY_TABLE", "Daily Closing History")


# ---------- Models ----------
class ClosingCreate(BaseModel):
    business_date: dt_date = Field(..., description="Business date (YYYY-MM-DD)")
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
        return {"error": "Missing one or more env vars: AIRTABLE_BASE_ID, AIRTABLE_API_KEY, AIRTABLE_TABLE_NAME"}

    try:
        table = Table(api_key, base_id, table_name)
        records = table.all(max_records=3)
        return {"records": [r.get("fields", {}) for r in records]}
    except Exception as e:
        return {"error": str(e)}


# ---------- Hybrid History Logger ----------
def _safe_serialize(obj):
    """Recursively convert datetime/date objects and nested dicts/lists to JSON-safe formats."""
    if isinstance(obj, (datetime, dt_date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_safe_serialize(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(_safe_serialize(v) for v in obj)
    else:
        return obj

def _log_history(
    *,
    action: str,
    store: str,
    business_date: str,
    fields_snapshot: dict,
    submitted_by: str = None,
    record_id: str = None,
    lock_status: str = None,
    changed_fields: Optional[list[str]] = None,
):
    """Logs both summary + full JSON snapshot of record changes."""
    try:
        table = _airtable_table(HISTORY_TABLE)
        changed_csv = ", ".join(changed_fields) if changed_fields else None

        safe_snapshot = _safe_serialize(fields_snapshot)
        print(f"🧩 Serialized snapshot type: {type(safe_snapshot)}")

        table.create({
            "Date": str(business_date),
            "Store": store,
            "Action": action,
            "Changed By": submitted_by,
            "Timestamp": datetime.now().isoformat(),
            "Record ID": record_id,
            "Lock Status": lock_status,
            "Changed Fields": changed_csv,
            "Snapshot": json.dumps(safe_snapshot, ensure_ascii=False),
        })

        print(f"🧾 Logged {action} for {store} on {business_date}")
    except Exception as e:
        print(f"⚠️ Failed to log history: {e}")

# ---------- UPSERT (Create or Update, Prevent Duplicates) ----------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    """Create or update a daily closing record in Airtable (upsert by store + date) with validation and history logging."""
    try:
        print("📩 Incoming payload received:", json.dumps(payload.dict(), default=str, indent=2))
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        store = payload.store.strip()
        business_date = payload.business_date.isoformat()
        payload_dict = json.loads(json.dumps(payload.dict(), default=str))
        print(f"🧾 Processing upsert for Store={store}, Date={business_date}")

        # --- Validation ---
        if payload.total_sales < 0 or payload.net_sales < 0:
            raise HTTPException(status_code=400, detail="Sales values cannot be negative.")
        if payload.total_sales < payload.net_sales:
            raise HTTPException(status_code=400, detail="Net sales cannot exceed total sales.")

        clean_store = store.replace("’", "'").replace("‘", "'").replace("'", "''")
        formula = (
            f"AND("
            f'{{Store}}="{clean_store}", '
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
            f")"
        )

        print(f"🔍 Airtable query formula: {formula}")
        existing = table.all(formula=formula, max_records=1)
        print(f"📊 Existing records found: {len(existing)}")

        fields = {
            "Date": business_date,
            "Store": store,
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
            "Last Updated By": payload.submitted_by,
            "Last Updated At": datetime.now().isoformat(),
        }

        fields = {k: v for k, v in fields.items() if v is not None}
        print(f"🧮 Fields prepared for upsert ({len(fields)} keys): {list(fields.keys())}")

        # 🔧 Universal safeguard for JSON serialization
        fields = json.loads(json.dumps(fields, default=str))

        # --- Existing Record (Update) ---
        if existing:
            print("🔁 Found existing record — proceeding with update.")
            record = existing[0]
            record_id = record["id"]
            current_lock = record["fields"].get("Lock Status", "Unlocked")

            print(f"🔒 Current lock status: {current_lock}")
            if current_lock in ["Locked", "Verified"]:
                print("⚠️ Record is locked or verified — cannot modify.")
                raise HTTPException(
                    status_code=403,
                    detail=f"Record for {store} on {business_date} is locked and cannot be modified.",
                )

            fields["Lock Status"] = "Locked"

            # 🔧 Re-sanitize before sending to Airtable
            fields = json.loads(json.dumps(fields, default=str))

            updated = table.update(record_id, fields)
            print(f"✅ Airtable record updated successfully: {record_id}")

            print("🗒 Logging update action to history...")
            _log_history(
                action="Updated",
                store=store,
                business_date=business_date,
                fields_snapshot=updated.get("fields", {}),
                submitted_by=payload.submitted_by,
                record_id=record_id,
                lock_status=updated.get("fields", {}).get("Lock Status"),
                changed_fields=list(fields.keys()),
            )
            print("✅ History log completed.")

            return {
                "status": "updated_locked",
                "id": record_id,
                "lock_status": updated.get("fields", {}).get("Lock Status", "Locked"),
                "fields": updated.get("fields", {}),
            }

        # --- New Record (Create) ---
        print("🆕 No existing record — creating a new one.")
        fields["Lock Status"] = "Locked"

        # 🔧 Re-sanitize before sending to Airtable
        fields = json.loads(json.dumps(fields, default=str))

        created = table.create(fields)
        print(f"✅ Airtable record created successfully: {created.get('id')}")

        print("🗒 Logging create action to history...")
        _log_history(
            action="Created",
            store=store,
            business_date=business_date,
            fields_snapshot=created.get("fields", {}),
            submitted_by=payload.submitted_by,
            record_id=created.get("id"),
            lock_status=created.get("fields", {}).get("Lock Status"),
            changed_fields=list(fields.keys()),
        )
        print("✅ History log completed.")

        return {
            "status": "created_locked",
            "id": created.get("id"),
            "lock_status": created.get("fields", {}).get("Lock Status", "Locked"),
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
            raise HTTPException(status_code=500, detail="MANAGER_PIN not configured on server")

        if not _constant_time_equal(payload.pin or "", manager_pin):
            raise HTTPException(status_code=401, detail="Invalid manager PIN")

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        updated = table.update(
            record_id,
            {"Lock Status": "Unlocked", "Unlocked At": datetime.now().isoformat()},
        )

        _log_history(
            action="Unlocked",
            store=updated.get("fields", {}).get("Store", "Unknown"),
            business_date=updated.get("fields", {}).get("Date", ""),
            fields_snapshot=updated.get("fields", {}),
            submitted_by="Manager PIN",
            record_id=record_id,
            lock_status="Unlocked",
            changed_fields=["Lock Status", "Unlocked At"],
        )

        print(f"🔓 Record {record_id} unlocked by manager.")
        return {
            "status": "unlocked",
            "id": record_id,
            "lock_status": updated.get("fields", {}).get("Lock Status", "Unlocked"),
            "fields": updated.get("fields", {}),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Utility: Airtable Filter ----------
def _airtable_filter_formula(business_date: Optional[str], store: Optional[str]) -> Optional[str]:
    clauses = []
    if business_date:
        clauses.append(f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')")
    if store:
        safe_store = store.replace("'", "''")
        clauses.append(f"{{Store}}='{safe_store}'")
    if not clauses:
        return None
    return "AND(" + ",".join(clauses) + ")"


# ---------- Unique Record Fetch (Prefill) ----------
@app.get("/closings/unique")
def get_unique_closing(business_date: str = Query(...), store: str = Query(...)):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        clean_store = (
            store.replace("’", "'")
            .replace("‘", "'")
            .replace("'", "''")
            .strip()
        )
        formula = (
            f"AND("
            f'{{Store}}="{clean_store}", '
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
            f")"
        )
        records = table.all(formula=formula, max_records=1)

        if not records:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "empty",
                    "message": f"No record found for {store} on {business_date}",
                    "fields": {},
                    "lock_status": "Unlocked",
                },
            )

        r = records[0]
        fields = r.get("fields", {})
        return JSONResponse(
            status_code=200,
            content={
                "status": "found",
                "id": r.get("id"),
                "lock_status": fields.get("Lock Status", "Unlocked"),
                "fields": fields,
            },
        )

    except Exception as e:
        print("❌ Error in /closings/unique:", e)
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


# ---------- History Read (Admin View) ----------
@app.get("/history")
def get_history(
    business_date: Optional[str] = Query(None),
    store: Optional[str] = Query(None),
    limit: int = Query(100),
):
    """Fetch history entries by date and/or store for admin view."""
    try:
        table = _airtable_table(HISTORY_TABLE)

        # Build formula using double-quoted strings (Airtable-safe)
        clauses = []
        if business_date:
            clauses.append(f'{{Date}}="{business_date}"')
        if store:
            safe_store = store.replace('"', '\\"')
            clauses.append(f'{{Store}}="{safe_store}"')

        formula = "AND(" + ", ".join(clauses) + ")" if clauses else None

        records = table.all(max_records=limit, formula=formula)
        return {
            "count": len(records),
            "records": [{"id": r.get("id"), "fields": r.get("fields", {})} for r in records],
        }
    except Exception as e:
        print("❌ Error fetching history:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Entrypoint ----------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    print(f"✅ Server starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")