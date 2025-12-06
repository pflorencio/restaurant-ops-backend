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
    """
    Resolve logical table keys -> Airtable Table using table IDs.
    """
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")

    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")

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
        raise RuntimeError(
            f"{cfg['id_env']} is not set. Intended table name is '{table_name}'."
        )

    return Table(api_key, base_id, table_id)


DAILY_CLOSINGS_TABLE = "daily_closing"
HISTORY_TABLE = "history"

DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")


def resolve_tenant_id(explicit: Optional[str]) -> str:
    return explicit or DEFAULT_TENANT_ID


def normalize_store_value(store: Optional[str]) -> str:
    """
    Central helper for normalizing store names (lowercase, strip, remove quotes).
    Used both for history + legacy name-based filters.
    """
    return ((store
             or "").lower().strip().replace("’",
                                            "").replace("‘",
                                                        "").replace("'", ""))


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
def _log_history(
    *,
    action: str,
    store: str,
    business_date: str,
    fields_snapshot: dict,
    submitted_by: Optional[str] = None,
    record_id: Optional[str] = None,
    lock_status: Optional[str] = None,
    changed_fields: Optional[List[str]] = None,
    tenant_id: Optional[str] = None,
):
    """
    Write one row to Daily Closing History with full snapshot JSON.
    Formula fields (like Store Normalized) must NOT be included.
    """
    try:
        table = _airtable_table(HISTORY_TABLE)
        changed_csv = ", ".join(changed_fields) if changed_fields else None
        safe_snapshot = _safe_serialize(fields_snapshot)

        payload = {
            "Date": str(business_date),
            "Store": store,  # TEXT FIELD OK
            # DO NOT SEND Store Normalized (formula field)
            "Tenant ID": tenant_id or DEFAULT_TENANT_ID,
            "Action": action,
            "Changed By": submitted_by,
            "Timestamp": datetime.now().isoformat(),
            "Record ID": record_id,
            "Lock Status": lock_status,
            "Changed Fields": changed_csv,
            "Snapshot": json.dumps(safe_snapshot, ensure_ascii=False),
        }

        table.create(payload)
        print(f"📘 History logged for {store} on {business_date}")  # helpful debug
    except Exception as e:
        print("⚠️ Failed to log history:", e)

# -----------------------------------------------------------
# 📌 UPSERT — Create or Update + Lock
# -----------------------------------------------------------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    """
    Create or update a daily closing record in Airtable.

    THIS VERSION:
    - Always prefers store_id
    - Never sends store_name to Airtable
    - Avoids legacy fallbacks unless ABSOLUTELY needed
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        # -------------------------
        # 🔍 Normalize store inputs
        # -------------------------
        store_id = getattr(payload, "store_id", None)
        store_name = getattr(payload, "store_name", None)  # <-- fixed key

        if not store_id:
            raise HTTPException(
                status_code=400,
                detail="store_id is required. Store name cannot be used for linked tables.",
            )

        business_date = payload.business_date.isoformat()
        tenant_id = resolve_tenant_id(getattr(payload, "tenant_id", None))

        # -------------------------
        # 🔍 Validation
        # -------------------------
        if payload.total_sales is not None and payload.total_sales < 0:
            raise HTTPException(status_code=400, detail="Total sales cannot be negative.")

        if payload.net_sales is not None and payload.net_sales < 0:
            raise HTTPException(status_code=400, detail="Net sales cannot be negative.")

        if (payload.total_sales is not None and payload.net_sales is not None
                and payload.total_sales < payload.net_sales):
            raise HTTPException(status_code=400, detail="Net sales cannot exceed total sales.")

        # --------------------------------------------------
        # 🔍 Find existing record by store_id + date ONLY
        # --------------------------------------------------
        date_formula = (
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
        )
        candidates = table.all(formula=date_formula, max_records=50)

        existing = None
        for rec in candidates:
            linked_ids = rec.get("fields", {}).get("Store", [])
            if isinstance(linked_ids, list) and store_id in linked_ids:
                existing = rec
                break

        # --------------------------------------------------
        # 📝 Construct fields (NO Store Name ever sent)
        # --------------------------------------------------
        fields = {
            "Date": business_date,
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
            "Store": [store_id],      # <-- ALWAYS correct Airtable format
        }

        fields = {k: v for k, v in fields.items() if v is not None}

        # --------------------------------------------------
        # 🔄 UPDATE EXISTING
        # --------------------------------------------------
        if existing:
            record_id = existing["id"]
            current_fields = existing.get("fields", {})
            current_lock = current_fields.get("Lock Status", "Unlocked")

            if current_lock in ["Locked", "Verified"]:
                raise HTTPException(
                    status_code=403,
                    detail=f"Record for date {business_date} is locked and cannot be modified.",
                )

            fields["Lock Status"] = "Locked"
            updated = table.update(record_id, fields)

            return {
                "status": "updated_locked",
                "id": record_id,
                "fields": updated.get("fields", {}),
                "lock_status": updated.get("fields", {}).get("Lock Status"),
            }

        # --------------------------------------------------
        # 🆕 CREATE NEW
        # --------------------------------------------------
        fields["Lock Status"] = "Locked"
        created = table.create(fields)

        return {
            "status": "created_locked",
            "id": created.get("id"),
            "fields": created.get("fields", {}),
            "lock_status": created.get("fields", {}).get("Lock Status"),
        }

    except Exception as e:
        print("❌ UPSERT ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------
# 🔓 UNLOCK — Manager PIN
# -----------------------------------------------------------
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

        fields = updated.get("fields", {})

        _log_history(
            action="Unlocked",
            store=fields.get("Store", "Unknown"),
            business_date=fields.get("Date", ""),
            fields_snapshot=fields,
            submitted_by="Manager PIN",
            record_id=record_id,
            lock_status=fields.get("Lock Status"),
            changed_fields=["Lock Status", "Unlocked At"],
            tenant_id=fields.get("Tenant ID") or DEFAULT_TENANT_ID,
        )

        return {
            "status": "unlocked",
            "id": record_id,
            "lock_status": fields.get("Lock Status", "Unlocked"),
            "fields": fields,
        }
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Error in unlock_closing:", e)
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
    Update one or more fields on a closing record.
    """
    try:
        updates = payload.root or {}
        if not isinstance(updates, dict) or not updates:
            raise HTTPException(status_code=400,
                                detail="Payload must be a non-empty object")

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        existing = table.get(record_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Record not found")

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
