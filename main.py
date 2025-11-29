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
from pydantic import RootModel

# --- Load environment early ---
load_dotenv()

print("🔎 ENV CHECK:")
print("  HISTORY TABLE ID:", os.getenv("AIRTABLE_HISTORY_TABLE_ID"))
print("  HISTORY TABLE NAME:", os.getenv("AIRTABLE_HISTORY_TABLE"))
print("  DAILY ID:", os.getenv("AIRTABLE_DAILY_CLOSINGS_TABLE_ID"))
print("  DAILY NAME:", os.getenv("AIRTABLE_DAILY_CLOSINGS_TABLE"))

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
    response.headers[
        "Access-Control-Allow-Methods"
    ] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# ---------- Airtable Helpers (STRICT mode: IDs required) ----------
def _airtable_table(table_key: str) -> Table:
    """
    Create an Airtable Table instance using STRICT ID-based resolution.

    table_key is the logical key, e.g. "daily_closing" or "history".

    In STRICT MODE:
    - We REQUIRE the table ID env var to be present.
    - We still read the human-readable name env var for logging only.
    """
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")

    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")

    # Map logical keys to ENV vars
    table_configs = {
        "daily_closing": {
            "id_env": "AIRTABLE_DAILY_CLOSINGS_TABLE_ID",
            "name_env": "AIRTABLE_DAILY_CLOSINGS_TABLE",
            "default_name": "Daily Closing",
        },
        "history": {
            "id_env": "AIRTABLE_HISTORY_TABLE_ID",
            "name_env": "AIRTABLE_HISTORY_TABLE",
            "default_name": "Daily Closing History",
        },
    }

    if table_key not in table_configs:
        raise RuntimeError(f"Unknown table key: {table_key}")

    cfg = table_configs[table_key]
    table_id = os.getenv(cfg["id_env"])
    table_name = os.getenv(cfg["name_env"], cfg["default_name"])

    if not table_id:
        # STRICT MODE: do NOT fall back to table name
        raise RuntimeError(
            f"STRICT MODE: {cfg['id_env']} is not set. "
            f"Please configure it in Render. Intended table name is '{table_name}'."
        )

    print(f"🔗 Using Airtable table '{table_name}' with ID {table_id} for key '{table_key}'")
    return Table(api_key, base_id, table_id)


# Logical keys (NOT table names — helper resolves IDs via env)
DAILY_CLOSINGS_TABLE = "daily_closing"
HISTORY_TABLE = "history"

# ---------- Multi-tenant Defaults (Option A for now) ----------
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")


def resolve_tenant_id(explicit: Optional[str]) -> str:
    """
    Return a concrete tenant id for every request.

    Today this just falls back to DEFAULT_TENANT_ID so existing clients do not
    need to send anything. Later we can derive tenant_id from auth/session.
    """
    return explicit or DEFAULT_TENANT_ID


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
    tenant_id: Optional[str] = None
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
    """
    Simple connectivity check for a generic table (using AIRTABLE_TABLE_NAME).
    This is separate from our strict-table-key helper.
    """
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_name = os.getenv("AIRTABLE_TABLE_NAME")
    if not (base_id and api_key and table_name):
        return {
            "error": "Missing one or more env vars: AIRTABLE_BASE_ID, AIRTABLE_API_KEY, AIRTABLE_TABLE_NAME"
        }

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
    submitted_by: Optional[str] = None,
    record_id: Optional[str] = None,
    lock_status: Optional[str] = None,
    changed_fields: Optional[list[str]] = None,
    tenant_id: Optional[str] = None,
):
    """
    Log a compact summary + full JSON snapshot of record changes.

    NOTE: `tenant_id` is optional for now. In the current single-tenant setup
    we default it in the caller using `resolve_tenant_id(...)`.
    """
    try:
        print(f"🗒 Attempting to log history for {store} on {business_date}...")
        table = _airtable_table(HISTORY_TABLE)
        changed_csv = ", ".join(changed_fields) if changed_fields else None

        safe_snapshot = _safe_serialize(fields_snapshot)

        # Normalize store name for filters
        normalized_store = (
            (store or "")
            .lower()
            .strip()
            .replace("’", "")
            .replace("‘", "")
            .replace("'", "")
        )

        payload = {
            "Date": str(business_date),
            "Store": store,
            "Store Normalized": normalized_store,
            "Tenant ID": tenant_id or DEFAULT_TENANT_ID,
            "Action": action,
            "Changed By": submitted_by,
            "Timestamp": datetime.now().isoformat(),
            "Record ID": record_id,
            "Lock Status": lock_status,
            "Changed Fields": changed_csv,
            "Snapshot": json.dumps(safe_snapshot, ensure_ascii=False),
        }

        print("🧾 History payload:", json.dumps(payload, indent=2))
        created = table.create(payload)
        print(
            f"✅ History logged (id={created.get('id')}) for {store} on {business_date} "
            f"(tenant={tenant_id or DEFAULT_TENANT_ID}, action={action})"
        )
    except Exception as e:
        # We don't crash the main flow, but we log clearly
        print(f"⚠️ Failed to log history: {e}")


# ---------- UPSERT (Create or Update, Prevent Duplicates) ----------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    """Create or update a daily closing record in Airtable (upsert by store + date) with validation and history logging."""
    try:
        print(
            "📩 Incoming payload received:",
            json.dumps(payload.dict(), default=str, indent=2),
        )
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        store = payload.store.strip()
        business_date = payload.business_date.isoformat()

        # Resolve tenant
        tenant_id = resolve_tenant_id(getattr(payload, "tenant_id", None))
        print(f"🧾 Processing upsert for Store={store}, Date={business_date}, Tenant={tenant_id}")

        # --- Validation ---
        if payload.total_sales < 0 or payload.net_sales < 0:
            raise HTTPException(
                status_code=400, detail="Sales values cannot be negative."
            )
        if payload.total_sales < payload.net_sales:
            raise HTTPException(
                status_code=400, detail="Net sales cannot exceed total sales."
            )

        normalized_store = (
            store.lower().strip().replace("’", "").replace("‘", "").replace("'", "")
        )
        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized_store}', "
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
            f")"
        )

        print(f"🔍 Airtable query formula: {formula}")
        existing = table.all(formula=formula, max_records=1)
        print(f"📊 Existing records found: {len(existing)}")

        fields = {
            "Date": business_date,
            "Store": store,
            "Tenant ID": tenant_id,
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

        # Safeguard for JSON serialization
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
                tenant_id=tenant_id,
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
            tenant_id=tenant_id,
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
            raise HTTPException(
                status_code=500, detail="MANAGER_PIN not configured on server"
            )

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
            tenant_id=updated.get("fields", {}).get("Tenant ID") or DEFAULT_TENANT_ID,
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
def _airtable_filter_formula(
    business_date: Optional[str], store: Optional[str]
) -> Optional[str]:
    clauses = []

    # Date filter
    if business_date:
        clauses.append(
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
        )

    # Store filter (normalized)
    if store:
        normalized_store = (
            store.lower().strip().replace("’", "").replace("‘", "").replace("'", "")
        )
        clauses.append(f"{{Store Normalized}}='{normalized_store}'")

    if not clauses:
        return None

    return "AND(" + ",".join(clauses) + ")"

# ---------- Unique Record Fetch (Prefill) ----------
@app.get("/closings/unique")
def get_unique_closing(business_date: str = Query(...), store: str = Query(...)):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        normalized_store = (
            store.lower().strip().replace("’", "").replace("‘", "").replace("'", "")
        )
        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized_store}', "
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
        return JSONResponse(
            status_code=500, content={"status": "error", "message": str(e)}
        )

# --- Single record fetch by ID (used by Cashier auto-refresh) ---
@app.get("/closings/{record_id}")
def get_closing_by_id(
    record_id: str = Path(..., description="Airtable record ID for the closing")
):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        record = table.get(record_id)
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        return {
            "id": record.get("id"),
            "fields": record.get("fields", {}),
        }
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in get_closing_by_id:", e)
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Admin List: /closings (with filters) ----------
@app.get("/closings")
def list_closings(
    business_date: Optional[str] = Query(
        None, description="Filter by business date YYYY-MM-DD"
    ),
    store: Optional[str] = Query(None, description="Filter by store name"),
    limit: int = Query(50, description="Maximum records to return"),
):
    """
    Lightweight admin endpoint used by the React dashboard to list closings
    for a specific date (and optionally a store).
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        formula = _airtable_filter_formula(business_date, store)
        records = table.all(max_records=limit, formula=formula)

        return {
            "count": len(records),
            "records": [
                {"id": r.get("id"), "fields": r.get("fields", {})} for r in records
            ],
        }
    except Exception as e:
        print("❌ Error listing closings:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Inline Update (PATCH /closings/{record_id}) ----------
class ClosingUpdate(RootModel[dict]):
    pass


@app.patch("/closings/{record_id}")
def patch_closing(record_id: str, payload: ClosingUpdate):
    """
    Update one or more fields on a closing record.
    """
    try:
        updates = payload.root or {}
        if not isinstance(updates, dict) or not updates:
            raise HTTPException(
                status_code=400, detail="Payload must be a non-empty object"
            )

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        existing = table.get(record_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Record not found")

        # Merge fields (for logging only)
        fields = existing.get("fields", {})
        for k, v in updates.items():
            fields[k] = v

        changed_keys = list(updates.keys())

        updated = table.update(record_id, updates)

        try:
            _log_history(
                action="Patched",
                store=fields.get("Store", "Unknown"),
                business_date=fields.get("Date", ""),
                fields_snapshot=updated.get("fields", {}),
                submitted_by=fields.get("Last Updated By"),
                record_id=record_id,
                lock_status=updated.get("fields", {}).get("Lock Status"),
                changed_fields=changed_keys,
                tenant_id=updated.get("fields", {}).get("Tenant ID")
                or DEFAULT_TENANT_ID,
            )
        except Exception as e:
            print("⚠️ Failed to log patch history:", e)

        return {
            "status": "patched",
            "id": record_id,
            "fields": updated.get("fields", {}),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in patch_closing:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- History Read (Admin View) ----------
@app.get("/history")
def get_history(
    business_date: Optional[str] = Query(None),
    store: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
    limit: int = Query(100),
):
    """
    Fetch history entries for admin view.
    """
    try:
        table = _airtable_table(HISTORY_TABLE)

        clauses: list[str] = []

        if business_date:
            clauses.append(
                f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
            )

        if store:
            normalized_store = (
                store.lower()
                .strip()
                .replace("’", "")
                .replace("‘", "")
                .replace("'", "")
            )
            clauses.append(f"{{Store Normalized}}='{normalized_store}'")

        if tenant_id:
            clauses.append(f'{{Tenant ID}}="{tenant_id}"')

        formula = "AND(" + ", ".join(clauses) + ")" if clauses else None
        print(f"🔍 History query formula: {formula}")

        records = table.all(max_records=limit, formula=formula)
        print(f"📜 History records fetched: {len(records)}")
        return {
            "count": len(records),
            "records": [
                {"id": r.get("id"), "fields": r.get("fields", {})} for r in records
            ],
        }
    except Exception as e:
        print("❌ Error fetching history:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Management Summary / Daily Report ----------
class VerifyPayload(BaseModel):
    record_id: str
    status: str
    verified_by: str


@app.post("/verify")
def verify_closing(payload: VerifyPayload):
    """
    Mark a closing record as Verified or Flagged, log history, and
    leave the row otherwise unchanged.
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        record_id = payload.record_id
        status = payload.status.strip().capitalize()  # "Verified" / "Flagged"

        record = table.get(record_id)
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        fields = record.get("fields", {})

        update_fields = {
            "Verified Status": status,
            "Verified By": payload.verified_by,
            "Verified At": datetime.now().isoformat(),
        }

        updated = table.update(record_id, update_fields)

        try:
            _log_history(
                action=f"Verification - {status}",
                store=fields.get("Store", "Unknown"),
                business_date=fields.get("Date", ""),
                fields_snapshot=updated.get("fields", {}),
                submitted_by=payload.verified_by,
                record_id=record_id,
                lock_status=updated.get("fields", {}).get("Lock Status"),
                changed_fields=list(update_fields.keys()),
                tenant_id=updated.get("fields", {}).get("Tenant ID")
                or DEFAULT_TENANT_ID,
            )
        except Exception as e:
            print("⚠️ Failed to log verification history:", e)

        return {
            "status": "ok",
            "record_id": record_id,
            "verified_status": status,
        }
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error verifying record:", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/reports/daily-summary")
def daily_summary(
    business_date: str = Query(..., description="Business date YYYY-MM-DD"),
    store: Optional[str] = Query(
        None, description="Optional store filter, e.g. `Nonie's`"
    ),
):
    """
    Very simple daily summary for management.
    """
    try:
        closings_table = _airtable_table(DAILY_CLOSINGS_TABLE)

        clauses = [
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
        ]

        if store:
            normalized_store = (
                store.lower()
                .strip()
                .replace("’", "")
                .replace("‘", "")
                .replace("'", "")
            )
            clauses.append(f"{{Store Normalized}}='{normalized_store}'")

        formula = "AND(" + ", ".join(clauses) + ")"

        records = closings_table.all(formula=formula, max_records=100)
        if not records:
            return {
                "business_date": business_date,
                "store": store,
                "preview": f"No closings found for {business_date}"
                + (f" at {store}" if store else ""),
            }

        agg = defaultdict(float)
        stores_seen = set()

        for r in records:
            f = r.get("fields", {})
            stores_seen.add(f.get("Store", "Unknown"))
            for key in [
                "Total Sales",
                "Net Sales",
                "Cash Payments",
                "Card Payments",
                "Digital Payments",
                "Grab Payments",
                "Voucher Payments",
                "Bank Transfer Payments",
                "Marketing Expenses",
                "Actual Cash Counted",
                "Cash Float",
                "Kitchen Budget",
                "Bar Budget",
                "Non Food Budget",
                "Staff Meal Budget",
                "Cash for Deposit",
                "Transfer Needed",
            ]:
                val = f.get(key)
                if isinstance(val, (int, float)):
                    agg[key] += float(val)

        def peso(n: float) -> str:
            return f"₱{n:,.0f}"

        lines = []
        lines.append(f"Management Summary for {business_date}")
        if store:
            lines.append(f"Store: {store}")
        else:
            joined = (
                ", ".join(sorted(s for s in stores_seen if s != "Unknown")) or "N/A"
            )
            lines.append(f"Stores included: {joined}")

        lines.append("")
        lines.append(f"Total Sales: {peso(agg['Total Sales'])}")
        lines.append(f"Net Sales: {peso(agg['Net Sales'])}")
        lines.append(
            "Cash + Digital + Card: "
            f"{peso(agg['Cash Payments'] + agg['Card Payments'] + agg['Digital Payments'])}"
        )
        lines.append(f"Marketing Expenses: {peso(agg['Marketing Expenses'])}")
        lines.append(
            f"Cash for Deposit: {peso(agg['Cash for Deposit'])}, "
            f"Transfer Needed: {peso(agg['Transfer Needed'])}"
        )
        lines.append("")
        lines.append("AI-generated summary is not enabled yet.")
        lines.append("Once configured, this section will show:")
        lines.append("- Total sales and cash across all stores")
        lines.append("- Variances and flagged records")
        lines.append("- Key notes for management review")

        return {
            "business_date": business_date,
            "store": store,
            "preview": "\n".join(lines),
        }

    except Exception as e:
        print("❌ Error in daily_summary:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Entrypoint ----------
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    print(f"✅ Server starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
