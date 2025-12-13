import os
import json
from datetime import date as dt_date, datetime
from typing import Optional, List, Dict
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, RootModel
from dotenv import load_dotenv
from pyairtable import Table
from email import send_closing_submission_email

# -----------------------------------------------------------
# 🔧 Load environment
# -----------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------
# 🔐 Load Airtable Credentials
# -----------------------------------------------------------
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AIRTABLE_DAILY_CLOSINGS_TABLE = os.getenv("AIRTABLE_DAILY_CLOSINGS_TABLE")
AIRTABLE_DAILY_CLOSINGS_TABLE_ID = os.getenv(
    "AIRTABLE_DAILY_CLOSINGS_TABLE_ID")
AIRTABLE_HISTORY_TABLE = os.getenv("AIRTABLE_HISTORY_TABLE")
AIRTABLE_HISTORY_TABLE_ID = os.getenv("AIRTABLE_HISTORY_TABLE_ID")

if not AIRTABLE_BASE_ID or not AIRTABLE_API_KEY:
    raise Exception(
        "❌ Missing Airtable credentials — check Render Environment settings.")

# 👉 NEW: Users table (by name, that's fine here)
AIRTABLE_USERS_TABLE_ID = os.getenv("AIRTABLE_USERS_TABLE_ID")

AIRTABLE_USERS = Table(
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AIRTABLE_USERS_TABLE_ID
)

# 👉 Daily Closing table (main table for closings)
DAILY_CLOSINGS = Table(
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AIRTABLE_DAILY_CLOSINGS_TABLE_ID
)

STORES_TABLE = "Stores"

# Airtable formula fields (must never be updated via API)
FORMULA_FIELDS = [
    "Total Budgets",
    "Variance",
    "Cash for Deposit",
]

# -----------------------------------------------------------
# 🚀 FastAPI App Init
# -----------------------------------------------------------
app = FastAPI(title="Daily Sales & Cash Management API", version="1.1.0")

FRONTEND_URL = "https://restaurant-ops-dashboard-pflorencio.replit.app"
BACKEND_URL = "https://restaurant-ops-backend.onrender.com"

origins = [
    FRONTEND_URL,
    "http://localhost:5000",
    "http://127.0.0.1:5000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Generic OPTIONS for preflight (kept, but GET routes override for normal traffic)
@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = FRONTEND_URL
    response.headers[
        "Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

# -----------------------------------------------------------
# 🔗 Airtable Helpers (STRICT mode using IDs)
# -----------------------------------------------------------

def _airtable_table(table_key: str) -> Table:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")

    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")

    table_configs = {
        "daily_closing": {
            "id_env": "AIRTABLE_DAILY_CLOSINGS_TABLE_ID",
            "default_name": "Daily Closing",
        },
        "history": {
            "id_env": "AIRTABLE_HISTORY_TABLE_ID",
            "default_name": "Daily Closing History",
        },
        "stores": {
            "id_env": "AIRTABLE_STORES_TABLE_ID",
            "default_name": "Stores",
        },
        "users": {
            "id_env": "AIRTABLE_USERS_TABLE_ID",
            "default_name": "Users",
        },
    }

    if table_key not in table_configs:
        raise RuntimeError(f"Unknown table key: {table_key}")

    cfg = table_configs[table_key]
    table_id = os.getenv(cfg["id_env"])

    if not table_id:
        raise RuntimeError(f"Missing table ID for {table_key} → {cfg['id_env']}")

    return Table(api_key, base_id, table_id)


# -----------------------------------------------------------
# TABLE KEYS
# -----------------------------------------------------------
DAILY_CLOSINGS_TABLE = "daily_closing"
HISTORY_TABLE = "history"
STORES_TABLE = "stores"
USERS_TABLE = "users"

DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")


def resolve_tenant_id(explicit: Optional[str]) -> str:
    return explicit or DEFAULT_TENANT_ID


def normalize_store_value(store: Optional[str]) -> str:
    return ((store or "")
            .lower()
            .strip()
            .replace("’", "")
            .replace("‘", "")
            .replace("'", ""))


# -----------------------------------------------------------
# USERS TABLE OBJECT
# -----------------------------------------------------------
AIRTABLE_USERS_TABLE_ID = os.getenv("AIRTABLE_USERS_TABLE_ID")
if not AIRTABLE_USERS_TABLE_ID:
    raise RuntimeError("Missing AIRTABLE_USERS_TABLE_ID")

AIRTABLE_USERS = Table(
    os.getenv("AIRTABLE_API_KEY"),
    os.getenv("AIRTABLE_BASE_ID"),
    AIRTABLE_USERS_TABLE_ID
)

# -----------------------------------------------------------
# STORES TABLE OBJECT
# -----------------------------------------------------------
AIRTABLE_STORES_TABLE_ID = os.getenv("AIRTABLE_STORES_TABLE_ID")
if not AIRTABLE_STORES_TABLE_ID:
    raise RuntimeError("Missing AIRTABLE_STORES_TABLE_ID")

AIRTABLE_STORES = Table(
    os.getenv("AIRTABLE_API_KEY"),
    os.getenv("AIRTABLE_BASE_ID"),
    AIRTABLE_STORES_TABLE_ID
)

# -----------------------------------------------------------
# 🧩 Models
# -----------------------------------------------------------
class ClosingCreate(BaseModel):
    business_date: dt_date = Field(...,
                                   description="Business date (YYYY-MM-DD)")
    # ⭐ NEW — preferred: linked Store record ID
    store_id: Optional[str] = Field(
        None, description="Linked Store record ID (preferred)")
    # Legacy / display store name (kept for backwards compatibility and history)
    store: Optional[str] = Field(
        None, description="Store name (legacy; used for display/back-compat)")
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


class UnlockPayload(BaseModel):
    pin: str


class VerifyPayload(BaseModel):
    record_id: str
    status: str
    verified_by: str


class ClosingUpdate(RootModel[Dict]):
    """
    Simple wrapper to accept an arbitrary JSON object for PATCH.
    """
    pass


# -----------------------------------------------------------
# 🔍 Basic routes
# -----------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "daily-sales-api", "version": "1.1.0"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/airtable/test")
def airtable_test():
    """
    Quick connectivity test using AIRTABLE_TABLE_NAME (optional).
    """
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_name = os.getenv("AIRTABLE_TABLE_NAME")
    if not (base_id and api_key and table_name):
        return {
            "error":
            "Missing AIRTABLE_BASE_ID, AIRTABLE_API_KEY, or AIRTABLE_TABLE_NAME"
        }

    try:
        table = Table(api_key, base_id, table_name)
        records = table.all(max_records=3)
        return {"records": [r.get("fields", {}) for r in records]}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------
# GET /stores  →  List all active stores
# ---------------------------------------------------------
@app.get("/stores")
async def list_stores():
    """
    Returns:
    [
      { "id": "recXXXX", "name": "Nonie's" },
      { "id": "recYYYY", "name": "Muchos" },
      ...
    ]
    """
    import requests

    if not AIRTABLE_BASE_ID or not AIRTABLE_API_KEY:
        raise HTTPException(status_code=500,
                            detail="Airtable credentials missing")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Stores"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("🔥 ERROR FETCHING STORES:", e)
        raise HTTPException(status_code=500, detail="Failed to fetch stores")

    stores = []
    for rec in data.get("records", []):
        fields = rec.get("fields", {})
        status = fields.get("Status", "")

        if isinstance(status, list):
            status = status[0]

        if str(status).lower() == "active":
            stores.append({
                "id": rec.get("id"),
                "name": fields.get("Store", "")
            })

    return stores

# -----------------------------------------------------------
# 🔐 AUTH — Users List + Login (using Airtable record ID)
# -----------------------------------------------------------

class UserLoginRequest(BaseModel):
    user_id: str
    pin: str


@app.get("/auth/users")
def list_users():
    """
    Returns all ACTIVE users from Airtable Users table with normalized store data.
    """

    try:
        records = AIRTABLE_USERS.all(formula="{Active}=TRUE()", max_records=200)
        result = []

        for r in records:
            fields = r.get("fields", {})

            # ---------------------------------------
            # Build Store Access (multi-store)
            # ---------------------------------------
            store_access_list = []
            access_ids = fields.get("Store Access") or []
            access_names = fields.get("Store (from Store Access)") or []

            for i, sid in enumerate(access_ids):
                store_access_list.append({
                    "id": sid,
                    "name": access_names[i] if i < len(access_names) else ""
                })

            # ---------------------------------------
            # Determine Primary Store
            # ---------------------------------------
            store_obj = None

            # Case 1: "Stores" field contains a linked store
            if isinstance(fields.get("Stores"), list) and fields["Stores"]:
                store_obj = {
                    "id": fields["Stores"][0],
                    "name": fields.get("Store (from Stores)")
                }

            # Case 2: fallback — first Store Access
            if not store_obj and store_access_list:
                store_obj = store_access_list[0]

            result.append({
                "user_id": r.get("id"),
                "name": fields.get("Name"),
                "pin": str(fields.get("PIN", "")),
                "role": str(fields.get("Role", "cashier")).lower(),
                "active": bool(fields.get("Active")),
                "store": store_obj,
                "store_access": store_access_list
            })

        return result

    except Exception as e:
        print("❌ Error in /auth/users:", e)
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/auth/user-login")
def user_login(payload: UserLoginRequest):
    """
    Validates login using Airtable record ID and returns normalized user object.
    """

    try:
        record = AIRTABLE_USERS.get(payload.user_id)
        if not record:
            raise HTTPException(status_code=401, detail="Invalid user selection")

        fields = record.get("fields", {})

        # Must be active
        if not bool(fields.get("Active")):
            raise HTTPException(status_code=401, detail="User is inactive")

        # PIN must match
        stored_pin = str(fields.get("PIN", ""))
        if payload.pin != stored_pin:
            raise HTTPException(status_code=401, detail="Invalid PIN")

        # ---------------------------------------
        # Build Store Access
        # ---------------------------------------
        store_access_list = []
        access_ids = fields.get("Store Access") or []
        access_names = fields.get("Store (from Store Access)") or []

        for i, sid in enumerate(access_ids):
            store_access_list.append({
                "id": sid,
                "name": access_names[i] if i < len(access_names) else ""
            })

        # ---------------------------------------
        # Determine Primary Store (same logic)
        # ---------------------------------------
        store_obj = None

        if isinstance(fields.get("Stores"), list) and fields["Stores"]:
            store_obj = {
                "id": fields["Stores"][0],
                "name": fields.get("Store (from Stores)")
            }

        if not store_obj and store_access_list:
            store_obj = store_access_list[0]

        # ---------------------------------------
        # Return login payload
        # ---------------------------------------
        return {
            "user_id": record.get("id"),
            "name": fields.get("Name"),
            "role": str(fields.get("Role", "cashier")).lower(),
            "store": store_obj,
            "store_access": store_access_list
        }

    except HTTPException:
        raise

    except Exception as e:
        print("❌ Error in /auth/user-login:", e)
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------
# ⭐ ADMIN USER MANAGEMENT (Update Role, Active, Store, Access)
# ------------------------------------------------------------
@app.patch("/admin/users/{user_id}")
async def update_user(user_id: str, payload: dict):
    """
    Admin update endpoint:
    Allows editing:
    - Role
    - Active (true/false)
    - Store (linked single record)
    - Store Access (multi-linked)
    """

    update_fields: Dict[str, object] = {}

    # Role change
    if "role" in payload:
        update_fields["Role"] = payload["role"]

    # Activate / deactivate user
    if "active" in payload:
        update_fields["Active"] = bool(payload["active"])

    # Change primary store (linked field is "Stores")
    if "store_id" in payload:
        update_fields["Stores"] = ([payload["store_id"]]
                                   if payload["store_id"] else [])

    # Update store access (multi-linked "Store Access")
    if "store_access_ids" in payload:
        update_fields["Store Access"] = payload["store_access_ids"] or []

    try:
        updated = AIRTABLE_USERS.update(user_id, update_fields)
        return {"status": "ok", "fields": updated.get("fields", {})}
    except Exception as e:
        print("❌ Error updating user:", e)
        raise HTTPException(status_code=500, detail="Failed to update user")


# ---------------------------------------------------------
# POST /admin/users  →  Create a new user
# ---------------------------------------------------------
@app.post("/admin/users")
async def create_user(payload: dict):
    """
    Expected payload example:
    {
        "name": "New Cashier",
        "pin": "1504",
        "role": "cashier",
        "active": true,
        "store_id": "recXXXX",           # optional
        "store_access_ids": ["recYYY"]   # optional array
    }
    """
    try:
        fields = {
            "Name": payload.get("name"),
            "PIN": payload.get("pin"),
            "Role": payload.get("role", "cashier"),
            "Active": payload.get("active", True),
        }

        # Assigned Store (single linked field "Stores")
        if payload.get("store_id"):
            fields["Stores"] = [payload["store_id"]]

        # Store Access (multi linked "Store Access")
        if payload.get("store_access_ids"):
            fields["Store Access"] = payload["store_access_ids"]

        created = AIRTABLE_USERS.create(fields)

        return {
            "status": "success",
            "id": created.get("id"),
            "fields": created.get("fields"),
        }

    except Exception as e:
        print("❌ Error creating user:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# 📝 History logger
# -----------------------------------------------------------
def _safe_serialize(obj):
    if isinstance(obj, (datetime, dt_date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_serialize(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_safe_serialize(v) for v in obj)
    return obj

def _log_history(
    *,
    action: str,
    store: Optional[str],
    business_date: str,
    fields_snapshot: dict,
    submitted_by: Optional[str] = None,
    record_id: Optional[str] = None,
    lock_status: Optional[str] = None,
    changed_fields: Optional[List[str]] = None,
    tenant_id: Optional[str] = None,
):
    try:
        history_table = _airtable_table(HISTORY_TABLE)

        # Resolve store name safely (linked or text)
        store_name = store or ""
        snap = fields_snapshot or {}

        # If store is a linked-record list, resolve name
        store_ids = snap.get("Store")
        if isinstance(store_ids, list) and len(store_ids) > 0:
            try:
                store_rec = AIRTABLE_STORES.get(store_ids[0])
                store_name = store_rec.get("fields", {}).get("Store") or store_name
            except Exception:
                pass

        if not store_name:
            store_name = (
                snap.get("Store Name")
                or snap.get("Store Display")
                or snap.get("Store Normalized")
                or snap.get("Store")
                or "Unknown"
            )

        normalized = normalize_store_value(store_name)

        payload = {
            "Date": business_date,
            "Store": store_name,
            "Store Normalized": normalized,
            "Tenant ID": tenant_id or DEFAULT_TENANT_ID,
            "Action": action,
            "Changed By": submitted_by,
            "Timestamp": datetime.now().isoformat(),
            "Record ID": record_id,
            "Lock Status": lock_status,
            "Changed Fields": ", ".join(changed_fields) if changed_fields else None,
            "Snapshot": json.dumps(_safe_serialize(snap), ensure_ascii=False),
        }

        history_table.create(payload)

    except Exception as e:
        print("⚠️ Failed to log history:", e)

# -----------------------------------------------------------
# 📌 UPSERT — Create or Update + Lock
# -----------------------------------------------------------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    """
    Create or update a daily closing record in Airtable.
    Prefers store_id (linked Store) but still accepts store name for compatibility.
    """

    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        # -----------------------------------------
        # Extract incoming values
        # -----------------------------------------
        store_id = (payload.store_id or "").strip()
        store_name = (payload.store or "").strip()
        business_date = payload.business_date.isoformat()

        # -----------------------------------------
        # Resolve tenant
        # -----------------------------------------
        tenant_id = resolve_tenant_id(getattr(payload, "tenant_id", None))

        # -----------------------------------------
        # Resolve store name from linked table if missing
        # -----------------------------------------
        if not store_name and store_id:
            try:
                rec = AIRTABLE_STORES.get(store_id)
                store_name = rec.get("fields", {}).get("Store", "")
                print(f"Resolved store_name → {store_name}")
            except Exception as e:
                print("⚠️ Could not resolve linked store name:", e)

        if not store_id and not store_name:
            raise HTTPException(
                400, "Either store_id or store name is required."
            )

        # -----------------------------------------
        # VALIDATION RULES
        # -----------------------------------------
        import math

        numeric_fields = {
            "Total Sales": payload.total_sales,
            "Net Sales": payload.net_sales,
            "Cash Payments": payload.cash_payments,
            "Card Payments": payload.card_payments,
            "Digital Payments": payload.digital_payments,
            "Grab Payments": payload.grab_payments,
            "Voucher Payments": payload.voucher_payments,
            "Bank Transfer Payments": payload.bank_transfer_payments,
            "Marketing Expenses": payload.marketing_expenses,
            "Actual Cash Counted": payload.actual_cash_counted,
            "Cash Float": payload.cash_float,
            "Kitchen Budget": payload.kitchen_budget,
            "Bar Budget": payload.bar_budget,
            "Non Food Budget": payload.non_food_budget,
            "Staff Meal Budget": payload.staff_meal_budget,
        }

        for field_name, value in numeric_fields.items():
            if value is not None:
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    raise HTTPException(400, f"{field_name} contains an invalid number.")
                if value < 0:
                    raise HTTPException(400, f"{field_name} cannot be negative.")

        if payload.total_sales is not None and payload.net_sales is not None:
            if payload.net_sales > payload.total_sales:
                raise HTTPException(
                    400, "Net sales cannot exceed total sales."
                )

        payments_sum = (
            (payload.cash_payments or 0)
            + (payload.card_payments or 0)
            + (payload.digital_payments or 0)
            + (payload.grab_payments or 0)
            + (payload.voucher_payments or 0)
            + (payload.bank_transfer_payments or 0)
            + (payload.marketing_expenses or 0)
        )

        if payload.total_sales is not None:
            if abs(payments_sum - payload.total_sales) > 1:
                raise HTTPException(
                    400,
                    f"Sum of payments ({payments_sum}) must equal Total Sales ({payload.total_sales}).",
                )

        budget_total = (
            (payload.kitchen_budget or 0)
            + (payload.bar_budget or 0)
            + (payload.non_food_budget or 0)
            + (payload.staff_meal_budget or 0)
        )

        if payload.net_sales is not None and budget_total > payload.net_sales:
            raise HTTPException(
                400,
                f"Total budget allocation ({budget_total}) cannot exceed Net Sales ({payload.net_sales}).",
            )

        # -----------------------------------------
        # FIND EXISTING RECORD (store + date)
        # -----------------------------------------
        date_formula = (
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
        )
        candidates = table.all(formula=date_formula, max_records=50)

        existing = None
        normalized_target = normalize_store_value(store_name)

        for rec in candidates:
            fields = rec.get("fields", {})

            linked_ids = fields.get("Store") or []
            if store_id and isinstance(linked_ids, list) and store_id in linked_ids:
                existing = rec
                break

            rec_norm = normalize_store_value(fields.get("Store Normalized", ""))
            if rec_norm == normalized_target:
                existing = rec
                break

        # ===========================================================
        # 📧 EMAIL TRIGGER LOGIC (SAFE — READ ONLY)
        # ===========================================================
        previous_status = None
        is_first_submission = False

        if not existing:
            is_first_submission = True
        else:
            previous_status = existing.get("fields", {}).get("Verified Status")

        # -----------------------------------------
        # PREPARE PAYLOAD FOR AIRTABLE
        # -----------------------------------------
        fields = {
            "Date": business_date,
            "Tenant ID": tenant_id,
            "Submitted By": payload.submitted_by,
            "Last Updated By": payload.submitted_by,
            "Last Updated At": datetime.now().isoformat(),
            "Total Sales": payload.total_sales,
            "Net Sales": payload.net_sales,
            "Cash Payments": payload.cash_payments,
            "Card Payments": payload.card_payments,
            "Digital Payments": payload.digital_payments,
            "Grab Payments": payload.grab_payments,
            "Voucher Payments": payload.voucher_payments,
            "Bank Transfer Payments": payload.bank_transfer_payments,
            "Marketing Expenses": payload.marketing_expenses,
            "Actual Cash Counted": payload.actual_cash_counted,
            "Cash Float": payload.cash_float,
            "Kitchen Budget": payload.kitchen_budget,
            "Bar Budget": payload.bar_budget,
            "Non Food Budget": payload.non_food_budget,
            "Staff Meal Budget": payload.staff_meal_budget,
        }

        if store_id:
            fields["Store"] = [store_id]
        else:
            fields["Store"] = store_name

        for f in ["Variance", "Cash for Deposit", "Total Budgets"]:
            fields.pop(f, None)

        fields = json.loads(json.dumps(fields, default=str))

        # ===========================================================
        # UPDATE EXISTING
        # ===========================================================
        if existing:
            rec_id = existing["id"]
            lock_status = existing["fields"].get("Lock Status", "Unlocked")

            if lock_status in ["Locked", "Verified"]:
                raise HTTPException(
                    403, f"Record for {store_name} on {business_date} is locked."
                )

            fields["Lock Status"] = "Locked"

            table.update(rec_id, fields)
            fresh = table.get(rec_id)

            _log_history(
                action="Updated",
                store=store_name,
                business_date=business_date,
                fields_snapshot=fresh.get("fields", {}),
                submitted_by=payload.submitted_by,
                record_id=rec_id,
                lock_status=fresh["fields"].get("Lock Status"),
                changed_fields=list(fields.keys()),
                tenant_id=tenant_id,
            )

            # 📧 Email only if resubmitting after Needs Update
            if previous_status == "Needs Update":
                send_closing_submission_email(
                    store_name=store_name,
                    business_date=business_date,
                    submitted_by=payload.submitted_by,
                    reason="resubmission_after_update",
                )

            return {
                "status": "updated_locked",
                "id": rec_id,
                "lock_status": fresh["fields"].get("Lock Status", "Locked"),
                "fields": fresh["fields"],
            }

        # ===========================================================
        # CREATE NEW
        # ===========================================================
        fields["Lock Status"] = "Locked"
        created = table.create(fields)
        fresh = table.get(created["id"])

        _log_history(
            action="Created",
            store=store_name,
            business_date=business_date,
            fields_snapshot=fresh.get("fields", {}),
            submitted_by=payload.submitted_by,
            record_id=fresh.get("id"),
            lock_status=fresh["fields"].get("Lock Status"),
            changed_fields=list(fields.keys()),
            tenant_id=tenant_id,
        )

        # 📧 Email on first submission
        send_closing_submission_email(
            store_name=store_name,
            business_date=business_date,
            submitted_by=payload.submitted_by,
            reason="first_submission",
        )

        return {
            "status": "created_locked",
            "id": fresh.get("id"),
            "lock_status": fresh["fields"].get("Lock Status", "Locked"),
            "fields": fresh["fields"],
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error during upsert:", e)
        raise HTTPException(500, str(e))

# -----------------------------------------------------------
# 🔓 UNLOCK — Manager PIN
# -----------------------------------------------------------
def _constant_time_equal(a: str, b: str) -> bool:
    """Prevents timing attacks — safe string comparison."""
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= ord(x) ^ ord(y)
    return result == 0


@app.post("/closings/{record_id}/unlock")
def unlock_closing(record_id: str, payload: UnlockPayload):
    """
    Unlock a closing record using Manager PIN.
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        record = table.get(record_id)

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        # ------------------------------------------------------------------
        # ⭐ FIXED: Proper PIN loading + sanitization
        # ------------------------------------------------------------------
        manager_pin = (os.getenv("MANAGER_PIN") or "").strip()
        incoming_pin = str(payload.pin).strip()

        if not _constant_time_equal(incoming_pin, manager_pin):
            raise HTTPException(status_code=401, detail="Invalid PIN")

        # Prepare updates
        updates = {
            "Lock Status": "Unlocked",
            "Unlocked At": datetime.now().isoformat(),
            "Unlocked By": "Manager PIN",
        }

        updated = table.update(record_id, updates)

        # Refresh to include formula fields
        fresh = table.get(record_id)
        fields = fresh.get("fields", {})

        # Resolve store name in a safe, guaranteed way
        store_value = fields.get("Store Normalized") or fields.get("Store") or ""

        # Log history (best effort)
        try:
            _log_history(
                action="Unlocked",
                store=store_value,
                business_date=fields.get("Date"),
                fields_snapshot=fields,
                submitted_by="Manager PIN",
                record_id=record_id,
                lock_status=fields.get("Lock Status"),
                changed_fields=list(updates.keys()),
                tenant_id=fields.get("Tenant ID") or DEFAULT_TENANT_ID,
            )
        except Exception as e:
            print("⚠️ Unlock history failed:", e)

        return fresh

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Unlock error:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# 🔍 Filter helper for listing closings (legacy name-based)
# -----------------------------------------------------------
def _airtable_filter_formula(business_date: Optional[str],
                             store: Optional[str]) -> Optional[str]:
    clauses = []

    if business_date:
        clauses.append(
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
        )

    if store:
        normalized_store = normalize_store_value(store)
        clauses.append(f"{{Store Normalized}}='{normalized_store}'")

    if not clauses:
        return None

    return "AND(" + ",".join(clauses) + ")"


# -----------------------------------------------------------
# 🎯 Unique closing (prefill)
# -----------------------------------------------------------
@app.get("/closings/unique")
def get_unique_closing(
    business_date: str = Query(...),
    store_id: Optional[str] = Query(
        None, description="Linked Store record ID (preferred filter)"
    ),
    store_name: Optional[str] = Query(
        None, description="Store name (e.g., \"Nonie's\")"
    ),
    store: Optional[str] = Query(
        None,
        description="Legacy store name param (alias for store_name, backwards compatible)",
    ),
):
    """
    Fetch a unique closing record for a given date + store.

    Priority:
    1. If `store_id` is provided, match by linked Store record on that date.
    2. Else, use store_name / store and `Store Normalized` + date.

    Returns:
    - status: "found" | "empty"
    - id: Airtable record ID (if found)
    - lock_status: "Locked" | "Unlocked"
    - fields: raw Airtable fields (if found)
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        # ---------------------------------------------------
        # 1) Preferred path: filter by store_id + date
        # ---------------------------------------------------
        if store_id:
            # Build a date-only formula using IS_SAME + DATETIME_PARSE
            date_formula = (
                "IS_SAME("
                "{{Date}}, "
                "DATETIME_PARSE('{bd}', 'YYYY-MM-DD'), "
                "'day'"
                ")"
            ).format(bd=business_date)

            candidates = table.all(formula=date_formula, max_records=50)

            match = None
            for r in candidates:
                f = r.get("fields", {})
                linked_ids = f.get("Store") or []
                # Airtable linked-field is usually a list of record IDs
                if isinstance(linked_ids, list) and store_id in linked_ids:
                    match = r
                    break

            if not match:
                return {
                    "status": "empty",
                    "message": f"No record found for store_id={store_id} on {business_date}",
                    "fields": {},
                    "lock_status": "Unlocked",
                }

            fields = match.get("fields", {})
            return {
                "status": "found",
                "id": match.get("id"),
                "lock_status": fields.get("Lock Status", "Unlocked"),
                "fields": fields,
            }

        # ---------------------------------------------------
        # 2) Fallback: use store_name / store + Store Normalized
        # ---------------------------------------------------
        effective_store_name = store_name or store
        if not effective_store_name:
            raise HTTPException(
                status_code=400,
                detail="Either store_id or store_name/store is required.",
            )

        normalized_store = normalize_store_value(effective_store_name)

        formula = (
            "AND("
            "{{Store Normalized}}='{normalized}', "
            "IS_SAME("
            "{{Date}}, "
            "DATETIME_PARSE('{bd}', 'YYYY-MM-DD'), "
            "'day'"
            ")"
            ")"
        ).format(normalized=normalized_store, bd=business_date)

        records = table.all(formula=formula, max_records=1)

        if not records:
            return {
                "status": "empty",
                "message": f"No record found for {effective_store_name} on {business_date}",
                "fields": {},
                "lock_status": "Unlocked",
            }

        r = records[0]
        fields = r.get("fields", {})
        return {
            "status": "found",
            "id": r.get("id"),
            "lock_status": fields.get("Lock Status", "Unlocked"),
            "fields": fields,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in /closings/unique:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# 📋 Admin list: /closings
# -----------------------------------------------------------
@app.get("/closings")
def list_closings(
        business_date: Optional[str] = Query(
            None, description="Filter by business date YYYY-MM-DD"),
        store: Optional[str] = Query(None, description="Filter by store name"),
        limit: int = Query(50, description="Maximum records to return"),
):
    """
    Lightweight admin endpoint used by the React dashboard to list closings.
    (Still uses legacy store name filter; can be extended to store_id later.)
    """
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
        print("❌ Error listing closings:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# ✏️ Inline update (PATCH /closings/{record_id})
# -----------------------------------------------------------
@app.patch("/closings/{record_id}")
def patch_closing(record_id: str, payload: ClosingUpdate):
    """
    Update individual fields of a daily closing record (admin inline edit).

    - Merges the incoming patch with the existing record
    - Applies the same validation rules as /closings (upsert)
    - Prevents updating Airtable formula fields
    - Logs a 'Patched' entry into Daily Closing History
    """
    try:
        updates = payload.root or {}
        if not isinstance(updates, dict) or not updates:
            raise HTTPException(status_code=400,
                                detail="Payload must be a non-empty object")

        # Never allow formula fields to be patched directly
        for f in FORMULA_FIELDS:
            updates.pop(f, None)

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        existing = table.get(record_id)

        if not existing:
            raise HTTPException(status_code=404, detail="Record not found")

        fields_before = existing.get("fields", {})

        # Respect lock status (same behaviour as /closings upsert)
        lock_status = fields_before.get("Lock Status", "Unlocked")
        if lock_status in ["Locked", "Verified"]:
            raise HTTPException(
                status_code=403,
                detail="Record is locked or verified and cannot be edited. "
                       "Please unlock before editing.",
            )

        # Build merged snapshot as if this were the new full record
        merged = dict(fields_before)
        merged.update(updates)

        # -----------------------------------------
        # Validation — mirror /closings logic
        # -----------------------------------------
        import math

        # Map Airtable numeric fields from merged snapshot
        numeric_values = {
            "Total Sales": merged.get("Total Sales"),
            "Net Sales": merged.get("Net Sales"),
            "Cash Payments": merged.get("Cash Payments"),
            "Card Payments": merged.get("Card Payments"),
            "Digital Payments": merged.get("Digital Payments"),
            "Grab Payments": merged.get("Grab Payments"),
            "Voucher Payments": merged.get("Voucher Payments"),
            "Bank Transfer Payments": merged.get("Bank Transfer Payments"),
            "Marketing Expenses": merged.get("Marketing Expenses"),
            "Actual Cash Counted": merged.get("Actual Cash Counted"),
            "Cash Float": merged.get("Cash Float"),
            "Kitchen Budget": merged.get("Kitchen Budget"),
            "Bar Budget": merged.get("Bar Budget"),
            "Non Food Budget": merged.get("Non Food Budget"),
            "Staff Meal Budget": merged.get("Staff Meal Budget"),
            "Cash for Deposit": merged.get("Cash for Deposit"),
        }

        # 0️⃣ Reject NaN / Infinity / negatives
        for field_name, value in numeric_values.items():
            if value is not None:
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    raise HTTPException(
                        status_code=400,
                        detail=f"{field_name} contains an invalid number."
                    )
                if value < 0:
                    raise HTTPException(
                        status_code=400,
                        detail=f"{field_name} cannot be negative."
                    )

        total_sales = numeric_values["Total Sales"]
        net_sales = numeric_values["Net Sales"]
        cash_payments = numeric_values["Cash Payments"] or 0
        card_payments = numeric_values["Card Payments"] or 0
        digital_payments = numeric_values["Digital Payments"] or 0
        grab_payments = numeric_values["Grab Payments"] or 0
        voucher_payments = numeric_values["Voucher Payments"] or 0
        bank_transfer = numeric_values["Bank Transfer Payments"] or 0
        marketing_expenses = numeric_values["Marketing Expenses"] or 0
        actual_cash = numeric_values["Actual Cash Counted"]
        cash_float = numeric_values["Cash Float"]
        cash_for_deposit = numeric_values["Cash for Deposit"]
        kitchen_budget = numeric_values["Kitchen Budget"] or 0
        bar_budget = numeric_values["Bar Budget"] or 0
        non_food_budget = numeric_values["Non Food Budget"] or 0
        staff_meal_budget = numeric_values["Staff Meal Budget"] or 0

        # 1️⃣ Net Sales ≤ Total Sales
        if total_sales is not None and net_sales is not None:
            if net_sales > total_sales:
                raise HTTPException(
                    status_code=400,
                    detail="Net sales cannot exceed total sales."
                )

        # 2️⃣ Σ Payments must ≈ Total Sales (±1 PHP)
        if total_sales is not None:
            payments_sum = (
                cash_payments
                + card_payments
                + digital_payments
                + grab_payments
                + voucher_payments
                + bank_transfer
                + marketing_expenses
            )
            if abs(payments_sum - total_sales) > 1:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Sum of payments ({payments_sum}) must equal "
                        f"Total Sales ({total_sales}) within ±₱1."
                    ),
                )

        # 3️⃣ Cash reconciliation: Cash for Deposit ≈ Actual Cash - Float
        if actual_cash is not None and cash_float is not None and cash_for_deposit is not None:
            expected_deposit = actual_cash - cash_float
            if abs(expected_deposit - cash_for_deposit) > 1:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Cash for Deposit should be {expected_deposit} "
                        f"based on Actual Cash Counted minus Cash Float."
                    ),
                )

        # 4️⃣ Budgets cannot exceed Net Sales
        budget_total = kitchen_budget + bar_budget + non_food_budget + staff_meal_budget
        if net_sales is not None and budget_total > net_sales:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Total budget allocation ({budget_total}) cannot exceed "
                    f"Net Sales ({net_sales})."
                ),
            )

        # -----------------------------------------
        # Apply PATCH to Airtable
        # -----------------------------------------
        # Never send formula fields even if someone tried to patch them
        for f in FORMULA_FIELDS:
            updates.pop(f, None)

        updated = table.update(record_id, updates)

        # Fetch fresh record including formula values after Airtable recalculation
        fresh = table.get(record_id)
        changed_keys = list(updates.keys())

        # Log history
        try:
            _log_history(
                action="Patched",
                store=fresh["fields"].get("Store Name"),
                business_date=fresh["fields"].get("Date"),
                fields_snapshot=fresh["fields"],
                submitted_by=fresh["fields"].get("Last Updated By"),
                record_id=record_id,
                lock_status=fresh["fields"].get("Lock Status"),
                changed_fields=changed_keys,
                tenant_id=fresh["fields"].get("Tenant ID") or DEFAULT_TENANT_ID,
            )
        except Exception as e:
            print("⚠️ Failed to log patch history:", e)

        return fresh

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in patch_closing:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# Verification Queue — FAST, Airtable-filtered version
# -----------------------------------------------------------
@app.get("/verification-queue")
async def verification_queue():
    try:
        # Airtable handles filtering internally
        records = DAILY_CLOSINGS.all(
            formula="OR({Verified Status}='Pending', {Verified Status}='Needs Update')"
        )
        return {"records": records}

    except Exception as e:
        print("Airtable error:", e)
        raise HTTPException(status_code=500, detail="Failed to fetch closings")

# -----------------------------------------------------------
# 📜 History read (admin view)
# -----------------------------------------------------------
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

        clauses: List[str] = []

        if business_date:
            clauses.append(
                f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'), 'day')"
            )

        if store:
            normalized_store = normalize_store_value(store)
            clauses.append(f"{{Store Normalized}}='{normalized_store}'")

        if tenant_id:
            clauses.append(f'{{Tenant ID}}="{tenant_id}"')

        formula = "AND(" + ", ".join(clauses) + ")" if clauses else None

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
        print("❌ Error fetching history:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# ✅ Verification endpoint (manager review)
# -----------------------------------------------------------
@app.post("/verify")
async def verify_closing(payload: dict):
    """
    Update verification status, notes, and lock state for a closing record.
    Expected payload from frontend:
    {
      "record_id": "recXXXX",
      "status": "Verified" | "Needs Update" | "Pending",
      "verified_by": "Patrick Manager",
      "notes": "Some notes here"
    }
    """
    record_id = payload.get("record_id")
    status = payload.get("status")
    verified_by = payload.get("verified_by")
    notes = payload.get("notes")

    if not record_id or not status:
        raise HTTPException(status_code=400, detail="Missing record_id or status")

    now_iso = datetime.utcnow().isoformat()

    # Base fields that always update
    update_fields = {
        "Verified Status": status,
        "Verification Notes": notes or "",
        "Verified By": verified_by or "System",
        "Last Updated At": now_iso,
        "Last Updated By": verified_by or "System",
    }

    # Locking behaviour
    if status == "Verified":
        # Lock the record + stamp verification time
        update_fields["Verified At"] = now_iso
        update_fields["Lock Status"] = "Locked"
    else:
        # "Needs Update" or "Pending" → unlocked for cashier edits
        update_fields["Verified At"] = None
        update_fields["Lock Status"] = "Unlocked"

    try:
        # Use the existing Airtable helper for the Daily Closing table
        table = _airtable_table("daily_closing")
        table.update(record_id, update_fields)
    except Exception as e:
        print("Airtable update error:", e)
        raise HTTPException(
            status_code=500,
            detail="Failed to update verification status"
        )

    return {
        "status": "success",
        "record_id": record_id,
        "new_status": status,
        "notes_saved": notes or "",
    }

# -----------------------------------------------------------
# 📊 Dashboard endpoint — single-day closing summary
# -----------------------------------------------------------
@app.get("/dashboard/closings")
def dashboard_closing_summary(
    business_date: str = Query(..., description="Business date YYYY-MM-DD"),
    store_id: Optional[str] = Query(
        None, description="Preferred: linked Store record ID"
    ),
    store_name: Optional[str] = Query(
        None, description="Fallback: store name (e.g. \"Nonie's\")"
    ),
    store: Optional[str] = Query(
        None,
        description="Legacy alias for store_name; kept for backwards compatibility",
    ),
):
    """
    Dashboard-friendly endpoint that returns:
    - The unique closing record for a given store + date
    - Backend-computed summary metrics (variance, budgets, cash for deposit, transfer needed)

    Priority:
    1. Use store_id (linked Store record) if provided
    2. Else, use store_name/store and Store Normalized
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        # -----------------------------
        # Helper: safe numeric extraction
        # -----------------------------
        def num(fields: Dict, key: str) -> float:
            val = fields.get(key)
            if isinstance(val, (int, float)):
                return float(val)
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0

        # -----------------------------
        # 1) Preferred path: store_id + date
        # -----------------------------
        record = None

        if store_id:
            date_formula = (
                "IS_SAME("
                "{{Date}}, "
                "DATETIME_PARSE('{bd}', 'YYYY-MM-DD'), "
                "'day'"
                ")"
            ).format(bd=business_date)

            candidates = table.all(formula=date_formula, max_records=50)

            for r in candidates:
                f = r.get("fields", {})
                linked_ids = f.get("Store") or []
                if isinstance(linked_ids, list) and store_id in linked_ids:
                    record = r
                    break

        # -----------------------------
        # 2) Fallback path: store_name/store + Store Normalized
        # -----------------------------
        if not record:
            effective_store = store_name or store
            if not effective_store and not store_id:
                raise HTTPException(
                    status_code=400,
                    detail="Either store_id or store_name/store is required.",
                )

            if effective_store:
                normalized = normalize_store_value(effective_store)
                formula = (
                    "AND("
                    "{{Store Normalized}}='{normalized}', "
                    "IS_SAME("
                    "{{Date}}, "
                    "DATETIME_PARSE('{bd}', 'YYYY-MM-DD'), "
                    "'day'"
                    ")"
                    ")"
                ).format(normalized=normalized, bd=business_date)

                records = table.all(formula=formula, max_records=1)
                if records:
                    record = records[0]

        # -----------------------------
        # No record found
        # -----------------------------
        if not record:
            return {
                "status": "empty",
                "business_date": business_date,
                "store": store_name or store,
                "record_id": None,
                "lock_status": "Unlocked",
                "summary": None,
                "raw_fields": {},
            }

        # -----------------------------
        # Build summary from Airtable fields
        # -----------------------------
        fields = record.get("fields", {})
        lock_status = fields.get("Lock Status", "Unlocked")

        # Core numeric values
        total_sales = num(fields, "Total Sales")
        net_sales = num(fields, "Net Sales")

        cash_payments = num(fields, "Cash Payments")
        card_payments = num(fields, "Card Payments")
        digital_payments = num(fields, "Digital Payments")
        grab_payments = num(fields, "Grab Payments")
        voucher_payments = num(fields, "Voucher Payments")
        bank_transfer = num(fields, "Bank Transfer Payments")
        marketing_expenses = num(fields, "Marketing Expenses")

        kitchen_budget = num(fields, "Kitchen Budget")
        bar_budget = num(fields, "Bar Budget")
        non_food_budget = num(fields, "Non Food Budget")
        staff_meal_budget = num(fields, "Staff Meal Budget")

        actual_cash = num(fields, "Actual Cash Counted")
        cash_float = num(fields, "Cash Float")

        # Backend-computed totals (mirror frontend logic)
        total_budgets = (
            kitchen_budget
            + bar_budget
            + non_food_budget
            + staff_meal_budget
        )

        # Variance: Actual Cash - Cash Payments - Float
        variance = actual_cash - cash_payments - cash_float

        # Cash for deposit & transfer needed:
        # raw = Actual Cash - Float - Total Budgets
        raw_cash_for_deposit = actual_cash - cash_float - total_budgets
        cash_for_deposit = raw_cash_for_deposit if raw_cash_for_deposit > 0 else 0.0
        transfer_needed = abs(raw_cash_for_deposit) if raw_cash_for_deposit < 0 else 0.0

        summary = {
            "total_sales": total_sales,
            "net_sales": net_sales,
            "cash_payments": cash_payments,
            "card_payments": card_payments,
            "digital_payments": digital_payments,
            "grab_payments": grab_payments,
            "voucher_payments": voucher_payments,
            "bank_transfer_payments": bank_transfer,
            "marketing_expenses": marketing_expenses,
            "kitchen_budget": kitchen_budget,
            "bar_budget": bar_budget,
            "non_food_budget": non_food_budget,
            "staff_meal_budget": staff_meal_budget,
            "actual_cash_counted": actual_cash,
            "cash_float": cash_float,
            "total_budgets": total_budgets,
            "variance": variance,
            "cash_for_deposit": cash_for_deposit,
            "transfer_needed": transfer_needed,
        }

        # Optional: include Airtable's formula fields for sanity-checking
        airtable_formulas = {
            "airtable_variance": fields.get("Variance"),
            "airtable_total_budgets": fields.get("Total Budgets"),
            "airtable_cash_for_deposit": fields.get("Cash for Deposit"),
        }

        store_display = (
            fields.get("Store Name")
            or fields.get("Store Display")
            or fields.get("Store")
        )

        return {
            "status": "found",
            "business_date": business_date,
            "store": store_display,
            "record_id": record.get("id"),
            "lock_status": lock_status,
            "summary": summary,
            "formulas": airtable_formulas,
            "raw_fields": fields,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in /dashboard/closings:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# 📊 Management summary /reports/daily-summary
# -----------------------------------------------------------
@app.get("/reports/daily-summary")
def daily_summary(
    business_date: str = Query(..., description="Business date YYYY-MM-DD"),
    store: Optional[str] = Query(
        None, description="Optional store filter, e.g. `Nonie's`"),
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
            normalized_store = normalize_store_value(store)
            clauses.append(f"{{Store Normalized}}='{normalized_store}'")

        formula = "AND(" + ", ".join(clauses) + ")"

        records = closings_table.all(formula=formula, max_records=100)
        if not records:
            return {
                "business_date":
                business_date,
                "store":
                store,
                "preview":
                f"No closings found for {business_date}" +
                (f" at {store}" if store else ""),
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
            joined = (", ".join(
                sorted(s for s in stores_seen if s != "Unknown")) or "N/A")
            lines.append(f"Stores included: {joined}")

        lines.append("")
        lines.append(f"Total Sales: {peso(agg['Total Sales'])}")
        lines.append(f"Net Sales: {peso(agg['Net Sales'])}")
        lines.append(
            "Cash + Digital + Card: "
            f"{peso(agg['Cash Payments'] + agg['Card Payments'] + agg['Digital Payments'])}"
        )
        lines.append(f"Marketing Expenses: {peso(agg['Marketing Expenses'])}")
        lines.append(f"Cash for Deposit: {peso(agg['Cash for Deposit'])}, "
                     f"Transfer Needed: {peso(agg['Transfer Needed'])}")
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


# -----------------------------------------------------------
# 🌅 Entrypoint (Render uses uvicorn directly; this is for local)
# -----------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    print(f"✅ Server starting on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
