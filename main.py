import os
import json
from datetime import date as dt_date, datetime
from datetime import timedelta
from typing import Optional, List, Dict
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, RootModel
from dotenv import load_dotenv
from pyairtable import Table
from fastapi import Query

from email_service import (
    send_closing_submission_email,
    send_closing_verification_email,
)

# -----------------------------------------------------------
# 🔧 Load environment
# -----------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------
# 🔐 Load Airtable Credentials
# -----------------------------------------------------------
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

if not AIRTABLE_BASE_ID or not AIRTABLE_API_KEY:
    raise RuntimeError(
        "❌ Missing Airtable credentials — check Render Environment settings."
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

# Generic OPTIONS for preflight
@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = FRONTEND_URL
    response.headers["Access-Control-Allow-Methods"] = (
        "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    )
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

# -----------------------------------------------------------
# 🔗 Airtable Helpers (STRICT mode using IDs)
# -----------------------------------------------------------
def _airtable_table(table_key: str) -> Table:
    """
    Centralized Airtable table resolver.
    Uses table IDs only (safe for production).
    """

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
        "weekly_budgets": {
            "id_env": "AIRTABLE_WEEKLY_BUDGETS_TABLE_ID",
            "default_name": "Weekly Budgets",
        },
    }

    if table_key not in table_configs:
        raise RuntimeError(f"Unknown table key: {table_key}")

    cfg = table_configs[table_key]
    table_id = os.getenv(cfg["id_env"])

    if not table_id:
        raise RuntimeError(f"Missing table ID for {table_key} → {cfg['id_env']}")

    return Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, table_id)

def parse_airtable_date(value: str) -> dt_date:
    """
    Airtable Date fields often include time + timezone (Z).
    This safely normalizes them to a pure date.
    """
    if not value:
        raise ValueError("Missing Airtable date value")

    return datetime.fromisoformat(
        value.replace("Z", "")
    ).date()

# -----------------------------------------------------------
# TABLE KEYS (single source of truth)
# -----------------------------------------------------------
DAILY_CLOSINGS_TABLE = "daily_closing"
HISTORY_TABLE = "history"
STORES_TABLE = "stores"
USERS_TABLE = "users"
WEEKLY_BUDGETS_TABLE = "weekly_budgets"


# -----------------------------------------------------------
# 🧩 Backward-compat Airtable Table Aliases (for older routes)
# -----------------------------------------------------------
DAILY_CLOSINGS = _airtable_table(DAILY_CLOSINGS_TABLE)

# -----------------------------------------------------------
# 🧠 Tenant Helpers
# -----------------------------------------------------------
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")

def resolve_tenant_id(explicit: Optional[str]) -> str:
    return explicit or DEFAULT_TENANT_ID

def normalize_store_value(store: Optional[str]) -> str:
    return (
        (store or "")
        .lower()
        .strip()
        .replace("’", "")
        .replace("‘", "")
        .replace("'", "")
    )

def monday_of_week(d: dt_date) -> dt_date:
    return d - timedelta(days=d.weekday())

def food_spend_from_fields(fields: dict) -> float:
    return (
        float(fields.get("Kitchen Budget", 0) or 0)
        + float(fields.get("Bar Budget", 0) or 0)
    )

def resolve_store_display_name(store_id: str) -> str:
    """
    Resolve Airtable Stores record ID -> display name used in linked record fields.
    We treat the Stores table primary display as the `Store` field (fallbacks included).
    """
    if not store_id:
        return ""

    try:
        stores_table = _airtable_table(STORES_TABLE)
        rec = stores_table.get(store_id) or {}
        f = rec.get("fields", {}) or {}
        # ✅ Your codebase consistently uses "Store" as the store name field
        return (
            f.get("Store")
            or f.get("Store Name")
            or f.get("Name")
            or ""
        )
    except Exception as e:
        print("⚠️ resolve_store_display_name failed:", e)
        return ""

# -----------------------------------------------------------
# 🧠 Shared User Validation Logic
# -----------------------------------------------------------
def validate_user_payload(
    *,
    role: str,
    store: Optional[str],
    store_access: Optional[List[str]],
):
    """
    Centralized validation rules for Users & Access.

    Rules:
    - cashier → exactly ONE store
    - manager → at least ONE store
    - admin → store optional
    """

    role = (role or "").lower()
    store_access = store_access or []

    if role == "admin":
        return

    if role == "cashier":
        if store_access and len(store_access) != 1:
            raise HTTPException(
                status_code=400,
                detail="Cashiers must be assigned to exactly one store.",
            )
        if not store and not store_access:
            raise HTTPException(
                status_code=400,
                detail="Cashiers must be assigned to one store.",
            )

    if role == "manager":
        if not store and not store_access:
            raise HTTPException(
                status_code=400,
                detail="Managers must be assigned to at least one store.",
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
# 👤 User Create Payload
# -----------------------------------------------------------
class UserCreate(BaseModel):
    name: str
    pin: str
    role: str  # cashier | manager | admin

    # Linked Airtable IDs
    store: Optional[str] = None              # single store (cashier shortcut)
    store_access: Optional[List[str]] = None # multi-store (manager/admin)

    email: Optional[str] = None
    active: Optional[bool] = True

# -----------------------------------------------------------
# 👤 User Update Payload
# -----------------------------------------------------------
class UserUpdate(BaseModel):
    name: Optional[str] = None
    pin: Optional[str] = None
    role: Optional[str] = None

    store: Optional[str] = None
    store_access: Optional[List[str]] = None

    email: Optional[str] = None
    active: Optional[bool] = None

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
# WEEKLY BUDGETS (GET)
# -----------------------------------------------------------
@app.get("/weekly-budgets")
def get_weekly_budget_raw(store_id: str, business_date: str):
    table = _airtable_table(WEEKLY_BUDGETS_TABLE)

    # Normalize any date to Monday of that week
    week_start = monday_of_week(dt_date.fromisoformat(business_date)).isoformat()

    # ✅ PRIMARY: match by Store ID + Week Start using IS_SAME (date-safe)
    formula_primary = (
        "AND("
        f"{{Store ID}}='{store_id}',"
        f"IS_SAME({{Week Start}}, '{week_start}', 'day')"
        ")"
    )

    records = table.all(formula=formula_primary, max_records=1)

    # ✅ FALLBACK (only if needed): match by Store display name (linked record)
    if not records:
        store_name = resolve_store_display_name(store_id)
        if store_name:
            safe_store_name = store_name.replace("'", "\\'")
            formula_fallback = (
                "AND("
                f"FIND('{safe_store_name}', ARRAYJOIN({{Store}})),"
                f"IS_SAME({{Week Start}}, '{week_start}', 'day')"
                ")"
            )
            records = table.all(formula=formula_fallback, max_records=1)

    if not records:
        # Helpful debug fields (won't break anything)
        return {
            "status": "empty",
            "store_id": store_id,
            "week_start": week_start,
            "business_date": business_date,
        }

    r = records[0]
    fields = r.get("fields", {}) or {}

    # ✅ Return BOTH:
    # - "fields" (for the cashier form context UI)
    # - flat keys (for your admin weekly budget page which expects them)
    return {
        "status": "found",
        "id": r["id"],
        "fields": fields,

        # Backward-compatible flattened keys
        "week_start": fields.get("Week Start"),
        "week_end": fields.get("Week End"),
        "weekly_budget": fields.get("Weekly Budget Amount"),
        "kitchen_budget": fields.get("Kitchen Weekly Budget"),
        "bar_budget": fields.get("Bar Weekly Budget"),
        "remaining_budget": fields.get("Remaining Budget"),
        "food_cost_deducted": fields.get("Food Cost Deducted"),
        "locked_by": fields.get("Locked By"),
        "locked_at": fields.get("Locked At"),
        "last_updated_at": fields.get("Last Updated At"),
        "status_text": fields.get("Status"),
    }

# -----------------------------------------------------------
# 🧾 Weekly Budget – Create / Update (Draft)
# -----------------------------------------------------------
@app.post("/weekly-budgets")
def upsert_weekly_budget(payload: dict):
    table = _airtable_table(WEEKLY_BUDGETS_TABLE)

    store_id = payload.get("store_id")
    week_start = payload.get("week_start")  # YYYY-MM-DD (Monday)
    kitchen_budget = float(payload.get("kitchen_budget", 0) or 0)
    bar_budget = float(payload.get("bar_budget", 0) or 0)
    submitted_by = payload.get("submitted_by", "System")

    if not store_id or not week_start:
        raise HTTPException(400, "store_id and week_start are required")

    # -------------------------------
    # Validate week_start
    # -------------------------------
    try:
        ws = dt_date.fromisoformat(week_start)
    except Exception:
        raise HTTPException(400, "Invalid week_start format")

    if ws.weekday() != 0:
        raise HTTPException(400, "week_start must be a Monday")

    week_end = (ws + timedelta(days=6)).isoformat()

    # Prevent edits to past weeks
    if ws < monday_of_week(dt_date.today()):
        raise HTTPException(403, "Cannot edit budgets for past weeks")

    # -------------------------------
    # 🔑 HARD UNIQUE MATCH (Store ID + Week Start)
    # -------------------------------
    formula = (
        "AND("
        f"{{Store ID}}='{store_id}',"
        f"IS_SAME({{Week Start}}, '{week_start}', 'day')"
        ")"
    )

    matches = table.all(formula=formula)

    # 🚨 SAFETY: never allow more than 1 row
    if len(matches) > 1:
        raise HTTPException(
            409,
            f"Multiple weekly budgets found for store {store_id} and week {week_start}"
        )

    record = matches[0] if matches else None
    total_budget = kitchen_budget + bar_budget

    # -------------------------------
    # CREATE (first time only)
    # -------------------------------
    if not record:
        created = table.create({
            "Store": [store_id],
            "Store ID": store_id,  # ⭐ CRITICAL
            "Week Start": week_start,
            "Week End": week_end,

            "Original Weekly Budget": total_budget,
            "Weekly Budget Amount": total_budget,
            "Kitchen Weekly Budget": kitchen_budget,
            "Bar Weekly Budget": bar_budget,

            "Food Cost Deducted": 0,
            "Remaining Budget": total_budget,

            "Status": "Draft",
            "Last Updated At": datetime.utcnow().isoformat(),
            "Locked At": None,
            "Locked By": None,
        })

        return {
            "status": "created",
            "id": created["id"],
            "weekly_budget": total_budget,
            "kitchen_budget": kitchen_budget,
            "bar_budget": bar_budget,
        }

    # -------------------------------
    # UPDATE (idempotent)
    # -------------------------------
    record_id = record["id"]
    fields = record.get("fields", {}) or {}

    if fields.get("Status") == "Locked":
        raise HTTPException(403, "Weekly budget is locked and cannot be edited")

    already_deducted = float(fields.get("Food Cost Deducted", 0) or 0)

    updates = {
        # Always re-assert identity
        "Store": [store_id],
        "Store ID": store_id,

        "Kitchen Weekly Budget": kitchen_budget,
        "Bar Weekly Budget": bar_budget,
        "Weekly Budget Amount": total_budget,

        # Preserve deductions
        "Remaining Budget": max(0, total_budget - already_deducted),

        "Last Updated At": datetime.utcnow().isoformat(),
    }

    table.update(record_id, updates)

    return {
        "status": "updated",
        "id": record_id,
        "weekly_budget": total_budget,
        "kitchen_budget": kitchen_budget,
        "bar_budget": bar_budget,
        "remaining_budget": max(0, total_budget - already_deducted),
    }

# -----------------------------------------------------------
# 🔒 Weekly Budget – Lock (finalize + recalc from verified closings)
# -----------------------------------------------------------
@app.post("/weekly-budgets/lock")
def lock_weekly_budget(payload: dict):
    budgets_table = _airtable_table(WEEKLY_BUDGETS_TABLE)
    closings_table = _airtable_table(DAILY_CLOSINGS_TABLE)

    # Inputs (support both styles: lock by budget_id OR lock by store_id+week_start)
    budget_id = payload.get("budget_id")
    store_id = payload.get("store_id")
    week_start = payload.get("week_start")  # expected YYYY-MM-DD (Monday)
    locked_by = payload.get("locked_by") or payload.get("submitted_by") or payload.get("updated_by") or "System"

    if not week_start:
        raise HTTPException(400, "week_start is required")
    if not budget_id and not store_id:
        raise HTTPException(400, "Either budget_id OR store_id must be provided")

    # Validate week_start is Monday
    try:
        ws = dt_date.fromisoformat(week_start)
    except Exception:
        raise HTTPException(400, "week_start must be YYYY-MM-DD")

    if ws.weekday() != 0:
        raise HTTPException(400, "week_start must be a Monday")

    we = (ws + timedelta(days=6)).isoformat()

    # -------------------------------
    # 1) Find the weekly budget record
    # -------------------------------
    record = None

    if budget_id:
        # Direct fetch
        try:
            record = budgets_table.get(budget_id)
        except Exception:
            raise HTTPException(404, "Weekly budget record not found (invalid budget_id)")
    else:
        # Lookup by Store ID + Week Start
        formula = (
            "AND("
            f"{{Store ID}}='{store_id}',"
            f"IS_SAME({{Week Start}}, '{week_start}', 'day')"
            ")"
        )
        found = budgets_table.all(formula=formula, max_records=1)
        record = found[0] if found else None

    if not record:
        raise HTTPException(404, "Weekly budget record not found for this store + week_start")

    record_id = record["id"]
    fields = record.get("fields", {}) or {}

    # If already locked, return safely (idempotent)
    if (fields.get("Status") or "").lower() == "locked":
        return {
            "status": "already_locked",
            "id": record_id,
            "week_start": week_start,
            "week_end": we,
            "weekly_budget": float(fields.get("Weekly Budget Amount", 0) or 0),
            "remaining_budget": float(fields.get("Remaining Budget", 0) or 0),
            "food_cost_deducted": float(fields.get("Food Cost Deducted", 0) or 0),
        }

    # -------------------------------
    # 2) Determine the locked budget numbers
    #    (Prefer record values; allow payload override if provided)
    # -------------------------------
    # We lock Kitchen/Bar/Total as of the lock action.
    kitchen_budget = payload.get("kitchen_budget", fields.get("Kitchen Weekly Budget", 0)) or 0
    bar_budget = payload.get("bar_budget", fields.get("Bar Weekly Budget", 0)) or 0

    try:
        kitchen_budget = float(kitchen_budget or 0)
        bar_budget = float(bar_budget or 0)
    except Exception:
        raise HTTPException(400, "kitchen_budget and bar_budget must be numbers")

    total_budget = kitchen_budget + bar_budget

    # Backward compat: if someone still sends weekly_budget, we ignore mismatch and recompute from kitchen+bar
    # weekly_budget_in = payload.get("weekly_budget", None)

    # -------------------------------
    # 3) Recalculate spent from VERIFIED closings within Mon..Sun
    # -------------------------------
    # Prefer matching via {Store ID} if present (more reliable than name matching).
    # If your Daily Closings table does NOT have {Store ID}, add it (formula or text).
    spent = 0.0

    # Date window using IS_AFTER/IS_BEFORE with inclusive buffer
    # (Airtable dates can be finicky; this is the safest inclusive pattern)
    start_guard = f"DATEADD(DATETIME_PARSE('{week_start}','YYYY-MM-DD'), -1, 'days')"
    end_guard = f"DATEADD(DATETIME_PARSE('{we}','YYYY-MM-DD'), 1, 'days')"

    closings_formula_primary = (
        "AND("
        "{Verified Status}='Verified',"
        f"IS_AFTER({{Date}}, {start_guard}),"
        f"IS_BEFORE({{Date}}, {end_guard}),"
        f"{{Store ID}}='{store_id}'"
        ")"
    )

    try:
        closing_records = closings_table.all(formula=closings_formula_primary)
        spent = sum(food_spend_from_fields(r.get("fields", {}) or {}) for r in closing_records)
    except Exception:
        # Fallback: match via store DISPLAY NAME if Store ID isn't available / formula errors
        store_name = resolve_store_display_name(store_id)
        if not store_name:
            raise HTTPException(400, "Could not resolve store name for fallback matching")

        safe_store_name = store_name.replace("'", "\\'")
        closings_formula_fallback = (
            "AND("
            "{Verified Status}='Verified',"
            f"IS_AFTER({{Date}}, {start_guard}),"
            f"IS_BEFORE({{Date}}, {end_guard}),"
            f"FIND('{safe_store_name}', ARRAYJOIN({{Store}}))"
            ")"
        )
        closing_records = closings_table.all(formula=closings_formula_fallback)
        spent = sum(food_spend_from_fields(r.get("fields", {}) or {}) for r in closing_records)

    remaining = max(0.0, total_budget - float(spent or 0))

    # -------------------------------
    # 4) Prepare updates (lock + finalize)
    # -------------------------------
    updates = {
        "Kitchen Weekly Budget": kitchen_budget,
        "Bar Weekly Budget": bar_budget,
        "Weekly Budget Amount": total_budget,
        "Food Cost Deducted": float(spent or 0),
        "Remaining Budget": remaining,
        "Status": "Locked",
        "Locked At": datetime.utcnow().isoformat(),
        "Locked By": locked_by,
        "Last Updated At": datetime.utcnow().isoformat(),
    }

    # Only set Original Weekly Budget Amount ONCE (first time we lock)
    # IMPORTANT: make sure this field name matches your Airtable column exactly.
    # Based on your screenshot it starts with "Original Weekly..."
    original_field_name = "Original Weekly Budget Amount"
    if original_field_name in fields:
        if fields.get(original_field_name) in (None, "", 0):
            updates[original_field_name] = total_budget
    else:
        # If your Airtable column name is slightly different, change it here.
        # e.g. "Original Weekly Budget"
        alt_original_field = "Original Weekly Budget"
        if alt_original_field in fields and fields.get(alt_original_field) in (None, "", 0):
            updates[alt_original_field] = total_budget

    budgets_table.update(record_id, updates)

    return {
        "status": "locked",
        "id": record_id,
        "store_id": store_id,
        "week_start": week_start,
        "week_end": we,
        "weekly_budget": total_budget,
        "kitchen_budget": kitchen_budget,
        "bar_budget": bar_budget,
        "food_cost_deducted": float(spent or 0),
        "remaining_budget": remaining,
        "locked_by": locked_by,
    }

# -----------------------------------------------------------
# 📊 Weekly Budget – Read (Frontend)
# -----------------------------------------------------------
@app.get("/weekly-budget")
async def get_weekly_budget(
    store_id: str = Query(...),
    date: str = Query(...)
):
    """
    Returns weekly budget context for a store + date.
    Week starts on Monday.
    """
    try:
        business_date = dt_date.fromisoformat(date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format")

    week_start = monday_of_week(business_date).isoformat()
    week_end = (monday_of_week(business_date) + timedelta(days=6)).isoformat()

    table = _airtable_table(WEEKLY_BUDGETS_TABLE)

    store_name = resolve_store_display_name(store_id)
    if not store_name:
        return {"exists": False, "reason": "Could not resolve store name"}

    safe_store_name = store_name.replace("'", "\\'")

    formula = (
        "AND("
        f"FIND('{safe_store_name}', ARRAYJOIN({{Store}})),"
        f"{{Week Start}}='{week_start}'"
        ")"
    )

    records = table.all(formula=formula, max_records=1)
    if not records:
        return {"exists": False}

    record = records[0]
    fields = record.get("fields", {}) or {}

    weekly_budget = float(fields.get("Weekly Budget Amount", 0) or 0)
    remaining_budget = float(fields.get("Remaining Budget", 0) or 0)
    daily_envelope = weekly_budget / 7 if weekly_budget else 0

    return {
        "exists": True,
        "store_id": store_id,
        "week_start": week_start,
        "week_end": week_end,
        "weekly_budget": weekly_budget,
        "remaining_budget": remaining_budget,
        "daily_envelope": daily_envelope,
        "status": fields.get("Status", "Draft"),
    }

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
        table = _airtable_table("users")

        records = table.all(formula="{Active}=TRUE()", max_records=200)
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
                    "name": (
                        fields.get("Store (from Stores)", [""])[0]
                        if isinstance(fields.get("Store (from Stores)"), list)
                        else fields.get("Store (from Stores)")
                    )
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
        table = _airtable_table("users")

        record = table.get(payload.user_id)
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
                "name": (
                    fields.get("Store (from Stores)", [""])[0]
                    if isinstance(fields.get("Store (from Stores)"), list)
                    else fields.get("Store (from Stores)")
                )
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

# -----------------------------------------------------------
# 👤 ADMIN — UPDATE USER
# -----------------------------------------------------------
@app.patch("/admin/users/{user_id}")
def update_user(user_id: str, payload: UserUpdate):
    """
    Update an existing user with role-based access rules.
    """

    try:
        table = _airtable_table("users")

        # ---------------------------------------------------
        # Fetch existing record
        # ---------------------------------------------------
        existing = table.get(user_id)
        if not existing:
            raise HTTPException(status_code=404, detail="User not found")

        current_fields = existing.get("fields", {})

        # ---------------------------------------------------
        # Resolve final values
        # ---------------------------------------------------
        role = payload.role or current_fields.get("Role")

        store = payload.store
        store_access = payload.store_access

        if store is None:
            existing_store = current_fields.get("Store")
            store = (
                existing_store[0]
                if isinstance(existing_store, list) and existing_store
                else None
            )

        if store_access is None:
            store_access = current_fields.get("Store Access") or []

        # ---------------------------------------------------
        # Validate role + store rules
        # ---------------------------------------------------
        validate_user_payload(
            role=role,
            store=store,
            store_access=store_access,
        )

        update_fields = {}

        # ---------------------------------------------------
        # Apply updates
        # ---------------------------------------------------
        if payload.name is not None:
            update_fields["Name"] = payload.name

        if payload.pin is not None:
            update_fields["PIN"] = str(payload.pin)  # ✅ correct column

        if payload.role is not None:
            update_fields["Role"] = payload.role

        if payload.email is not None:
            update_fields["Email"] = payload.email

        if payload.active is not None:
            update_fields["Active"] = payload.active

        # ---------------------------------------------------
        # Store logic
        # ---------------------------------------------------
        if role == "cashier":
            if not store:
                raise HTTPException(
                    status_code=400,
                    detail="Cashiers must be assigned to one store.",
                )

            update_fields["Store"] = [store]               # linked record
            update_fields["Store Access"] = [store]       # label

        elif role == "manager":
            if store_access:
                update_fields["Store Access"] = store_access

            if store:
                update_fields["Store"] = [store]

        elif role == "admin":
            update_fields["Store"] = []
            update_fields["Store Access"] = []

        # ---------------------------------------------------
        # Update Airtable
        # ---------------------------------------------------
        updated = table.update(user_id, update_fields)

        return {
            "status": "updated",
            "user_id": user_id,
            "fields": updated.get("fields", {}),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Update user error:", e)
        raise HTTPException(
            status_code=500,
            detail="Failed to update user",
        )

# -----------------------------------------------------------
# 👤 ADMIN — CREATE USER
# -----------------------------------------------------------
@app.post("/admin/users")
def create_user(payload: UserCreate):
    """
    Create a new user with role-based access rules.
    """

    try:
        # ---------------------------------------------------
        # Validate role + store rules
        # ---------------------------------------------------
        validate_user_payload(
            role=payload.role,
            store=payload.store,
            store_access=payload.store_access,
        )

        table = _airtable_table("users")

        fields = {
            "Name": payload.name,
            "PIN": str(payload.pin),  # ✅ text field
            "Role": payload.role,
            "Active": payload.active if payload.active is not None else True,
        }

        if payload.email:
            fields["Email"] = payload.email

        # ---------------------------------------------------
        # Store logic
        # ---------------------------------------------------
        if payload.role == "cashier":
            store = payload.store or (
                payload.store_access[0] if payload.store_access else None
            )

            if not store:
                raise HTTPException(
                    status_code=400,
                    detail="Cashiers must be assigned to one store.",
                )

            fields["Store"] = [store]            # linked record
            fields["Store Access"] = [store]    # multiple select label

        elif payload.role == "manager":
            if payload.store:
                fields["Store"] = [payload.store]

            if payload.store_access:
                fields["Store Access"] = payload.store_access

        elif payload.role == "admin":
            fields["Store"] = []
            fields["Store Access"] = []

        created = table.create(fields)

        return {
            "status": "created",
            "user_id": created.get("id"),
            "fields": created.get("fields", {}),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Create user error:", e)
        raise HTTPException(
            status_code=500,
            detail="Failed to create user",
        )

# ---------------------------------------------------------
# GET /admin/roles-metadata  →  Role rules + helper text
# ---------------------------------------------------------
@app.get("/admin/roles-metadata")
async def get_roles_metadata():
    """
    Frontend helper endpoint so UI can enforce rules consistently.
    """
    return {
        "roles": [
            {
                "key": "cashier",
                "label": "Cashier",
                "helper": "Cashiers can only access one store and submit closings.",
                "requires_store": True,
                "max_stores": 1,
            },
            {
                "key": "manager",
                "label": "Manager",
                "helper": "Managers can verify closings and access assigned stores.",
                "requires_store": True,
                "max_stores": None,  # can have multiple via Store Access
            },
            {
                "key": "admin",
                "label": "Admin",
                "helper": "Admins have full access across all stores and settings.",
                "requires_store": False,
                "max_stores": None,
            },
        ]
    }


# ---------------------------------------------------------
# GET /admin/users  →  List users for Users & Access table
# ---------------------------------------------------------
@app.get("/admin/users")
async def admin_list_users():
    """
    Returns all users in Airtable with normalized fields for the frontend table.
    """
    try:
        records = AIRTABLE_USERS.all()

        users = []
        for r in records:
            f = r.get("fields", {}) or {}

            # Build Store Access list
            access_ids = f.get("Store Access") or []
            access_names = f.get("Store (from Store Access)") or []
            store_access_list = []
            for i, sid in enumerate(access_ids):
                store_access_list.append({
                    "id": sid,
                    "name": access_names[i] if i < len(access_names) else ""
                })

            # Primary store (your existing convention uses "Stores")
            store_obj = None
            if isinstance(f.get("Stores"), list) and f.get("Stores"):
                store_obj = {
                    "id": f["Stores"][0],
                    "name": (f.get("Store (from Stores)") or f.get("Store (from store)") or "")
                }

            # Fallback: if no primary store set, use first store access
            if not store_obj and store_access_list:
                store_obj = store_access_list[0]

            users.append({
                "record_id": r.get("id"),
                "user_id": f.get("User ID"),  # autonumber (may be None if not present)
                "name": f.get("Name"),
                "pin": f.get("PIN") or f.get("Pin"),
                "role": str(f.get("Role", "cashier")).lower(),
                "active": bool(f.get("Active", True)),
                "email": f.get("Email"),
                "store": store_obj,
                "store_access": store_access_list,
                "created_at": f.get("Created At"),
                "updated_at": f.get("Updated At"),
            })

        return users

    except Exception as e:
        print("❌ Error in GET /admin/users:", e)
        raise HTTPException(status_code=500, detail="Failed to fetch users")

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
        if isinstance(store_ids, list) and store_ids:
            try:
                stores_table = _airtable_table(STORES_TABLE)
                store_rec = stores_table.get(store_ids[0])
                store_name = store_rec.get("fields", {}).get("Store") or store_name
            except Exception as e:
                print("⚠️ History store resolve failed:", e)

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
                stores_table = _airtable_table(STORES_TABLE)
                rec = stores_table.get(store_id)
                store_name = rec.get("fields", {}).get("Store", "")
                print(f"Resolved store_name → {store_name}")
            except Exception as e:
                print("⚠️ Could not resolve linked store name:", e)

        if not store_id and not store_name:
            raise HTTPException(400, "Either store_id or store name is required.")

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
                raise HTTPException(400, "Net sales cannot exceed total sales.")

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

        # -----------------------------------------
        # 📧 Determine email reason (SAFE & EXPLICIT)
        # -----------------------------------------
        if not existing:
            email_reason = "first_submission"
        else:
            prev_status = existing.get("fields", {}).get("Verified Status")
            email_reason = (
                "resubmission_after_update"
                if prev_status == "Needs Update"
                else None
            )

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

            # 🔄 Resubmission after Needs Update → reset verification state
            prev_verified_status = existing["fields"].get("Verified Status")

            if prev_verified_status == "Needs Update":
                fields.update({
                    "Verified Status": "Pending",
                    "Verified At": None,
                    "Food Cost Deducted": 0,  # reset so /verify recalculates delta cleanly
                })

            # 🔓 Allow edit if coming from Needs Update
            if lock_status in ["Locked", "Verified"] and prev_verified_status != "Needs Update":
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

            # 📧 Email ONLY if resubmission after Needs Update
            if email_reason == "resubmission_after_update":
                send_closing_submission_email(
                    store_name=store_name,
                    business_date=business_date,
                    submitted_by=payload.submitted_by,
                    reason=email_reason,
                    closing_fields=fresh.get("fields", {}),
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
            closing_fields=fresh.get("fields", {}),
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

# --------------------------------------------
# Check if there is a closing that needs update
# --------------------------------------------
@app.get("/closings/needs-update")
async def get_closing_needs_update(store_id: str):
    """
    Returns the most recent closing marked as 'Needs Update' for the given store.
    """
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)

        store_name = resolve_store_display_name(store_id)
        if not store_name:
            raise HTTPException(status_code=400, detail="Could not resolve store name")

        safe_store_name = store_name.replace("'", "\\'")

        formula = (
            "AND("
            "{Verified Status}='Needs Update',"
            f"FIND('{safe_store_name}', ARRAYJOIN({{Store}}))"
            ")"
        )

        records = table.all(formula=formula, max_records=1, sort=["-Date"])
        if not records:
            return {"exists": False}

        r = records[0]
        f = r.get("fields", {}) or {}

        return {
            "exists": True,
            "record_id": r.get("id"),
            "business_date": f.get("Date"),
            "store_name": store_name,
            "notes": (f.get("Verification Notes") or "").strip(),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ needs-update error:", str(e))
        raise HTTPException(status_code=500, detail="Failed to check updates")

# --------------------------------------------
# List all closings that need update (per store)
# --------------------------------------------
@app.get("/closings/needs-update-list")
async def get_closings_needing_update(store_id: str):
    """
    Returns ALL closings marked as 'Needs Update'
    for the given store.

    Store is a Linked Record field.
    Linked record values = Store table PRIMARY FIELD ("Store"), not record IDs.
    """
    try:
        closings_table = _airtable_table(DAILY_CLOSINGS_TABLE)
        stores_table = _airtable_table(STORES_TABLE)

        # 1) Resolve store_id -> store name (primary field)
        store_record = stores_table.get(store_id)
        store_fields = store_record.get("fields", {}) if store_record else {}

        store_name = resolve_store_display_name(store_id)
        if not store_name:
            raise HTTPException(status_code=400, detail="Could not resolve store name")

        safe_store_name = store_name.replace("'", "\\'")

        formula = (
            "AND("
            "{Verified Status}='Needs Update',"
            f"FIND('{safe_store_name}', ARRAYJOIN({{Store}}))"
            ")"
        )

        records = closings_table.all(
            formula=formula,
            sort=["Date"]  # oldest → newest
        )

        results = []
        for r in records:
            f = r.get("fields", {})
            results.append({
                "record_id": r.get("id"),
                "business_date": f.get("Date"),
                "notes": f.get("Verification Notes", ""),
            })

        return {
            "count": len(results),
            "records": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ needs-update-list error:", str(e))
        raise HTTPException(status_code=500, detail="Failed to load update list")


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
    """

    record_id = payload.get("record_id")
    status = payload.get("status")
    verified_by = payload.get("verified_by")
    notes = payload.get("notes")

    if not record_id or not status:
        raise HTTPException(status_code=400, detail="Missing record_id or status")

    now_iso = datetime.utcnow().isoformat()
    table = _airtable_table("daily_closing")

    try:
        # ---------------------------------------------------
        # 0) Fetch BEFORE update (needed for reversal)
        # ---------------------------------------------------
        before = table.get(record_id)
        before_fields = before.get("fields", {}) if before else {}

        prev_status = (before_fields.get("Verified Status") or "").strip()
        prev_food_deducted = float(before_fields.get("Food Cost Deducted", 0) or 0)

        # -------------------------------------------------------
        # 1) Base fields that always update
        # -------------------------------------------------------
        update_fields = {
            "Verified Status": status,
            "Verification Notes": notes or "",
            "Verified By": verified_by or "System",
            "Last Updated By": verified_by or "System",
        }

        # -------------------------------------------------------
        # 2) Locking behaviour
        # -------------------------------------------------------
        if status == "Verified":
            update_fields["Verified At"] = now_iso
            update_fields["Lock Status"] = "Locked"
        else:
            update_fields["Verified At"] = None
            update_fields["Lock Status"] = "Unlocked"

        # ---------------------------------------------------
        # 3) Update Airtable record
        # ---------------------------------------------------
        table.update(record_id, update_fields)

        # ---------------------------------------------------
        # Helper: locate the locked weekly budget row
        # ---------------------------------------------------
        def get_locked_weekly_budget_record(fields: dict):
            store_ids = fields.get("Store") or []
            business_date_raw = fields.get("Date")

            if not store_ids or not business_date_raw:
                return None, None, None

            store_id = store_ids[0]  # recXXXX

            store_name = resolve_store_display_name(store_id)
            if not store_name:
                store_name = str(store_id)  # fallback (won’t match weekly budgets, but avoids crash)

            # Robust date parsing
            try:
                business_date = parse_airtable_date(business_date_raw)
            except Exception:
                business_date = dt_date.fromisoformat(str(business_date_raw)[:10])

            week_start = monday_of_week(business_date).isoformat()
            budget_table = _airtable_table(WEEKLY_BUDGETS_TABLE)

            safe_store_name = store_name.replace("'", "\\'")
            formula = (
                "AND("
                f"FIND('{safe_store_name}', ARRAYJOIN({{Store}})),"
                f"IS_SAME({{Week Start}}, '{week_start}', 'day'),"
                "{Status}='Locked'"
                ")"
            )

            print("Weekly budget lookup:")
            print("Store ID:", store_id)
            print("Resolved store name:", store_name)
            print("Business date raw:", business_date_raw)
            print("Parsed business date:", business_date.isoformat())
            print("Week start:", week_start)
            print("Formula:", formula)

            records = budget_table.all(formula=formula, max_records=1)
            print("Weekly budget records found:", len(records))

            if not records:
                return budget_table, None, week_start

            return budget_table, records[0], week_start

        # ---------------------------------------------------
        # 4) Weekly budget adjustment logic (SAFE + REVERSIBLE)
        # ---------------------------------------------------

        fresh = table.get(record_id)
        fields = fresh.get("fields", {}) if fresh else {}

        current_food_deducted = float(fields.get("Food Cost Deducted", 0) or 0)

        # =========================
        # VERIFYING
        # =========================
        if status == "Verified":
            try:
                budget_table, budget_record, week_start = get_locked_weekly_budget_record(fields)

                if not budget_record:
                    return

                new_food_spend = float(food_spend_from_fields(fields) or 0)
                prev_food_spend = float(prev_food_deducted or 0)

                delta = new_food_spend - prev_food_spend

                # Nothing changed → do nothing
                if delta == 0:
                    print("No food cost change — skipping weekly budget adjustment")
                    return

                remaining = float(budget_record["fields"].get("Remaining Budget", 0) or 0)
                running_deducted = float(budget_record["fields"].get("Food Cost Deducted", 0) or 0)

                # Apply delta (can be + or -)
                budget_table.update(
                    budget_record["id"],
                    {
                        "Remaining Budget": remaining - delta,
                        "Food Cost Deducted": running_deducted + delta,
                        "Last Updated At": now_iso,
                    },
                )

                # Anchor final value on DAILY CLOSING
                table.update(
                    record_id,
                    {
                        "Food Cost Deducted": new_food_spend,
                    },
                )

                print(f"Weekly budget reconciled by delta: {delta}")

            except Exception as budget_err:
                print("Weekly budget update error:", budget_err)

        # =========================
        # UN-VERIFYING → REVERSE
        # =========================
        else:
            if prev_status == "Verified" and current_food_deducted > 0:
                try:
                    budget_table, budget_record, week_start = get_locked_weekly_budget_record(before_fields)
                    if budget_record:
                        remaining = float(budget_record["fields"].get("Remaining Budget", 0) or 0)
                        running_deducted = float(
                            budget_record["fields"].get("Food Cost Deducted", 0) or 0
                        )

                        budget_table.update(
                            budget_record["id"],
                            {
                                "Remaining Budget": remaining + current_food_deducted,
                                "Food Cost Deducted": max(0, running_deducted - current_food_deducted),
                                "Last Updated At": now_iso,
                            },
                        )

                    # Clear anchor
                    table.update(
                        record_id,
                        {
                            "Food Cost Deducted": 0,
                        }
                    )
                except Exception as budget_err:
                    print("Weekly budget reversal error:", budget_err)

        # ---------------------------------------------------
        # 5) 📧 VERIFICATION EMAIL (ONLY WHEN VERIFIED)
        # ---------------------------------------------------
        if status == "Verified":
            store_name = (
                fields.get("Store Name")
                or fields.get("Store Normalized")
                or "Unknown Store"
            )

            send_closing_verification_email(
                store_name=store_name,
                business_date=fields.get("Date"),
                cashier_name=fields.get("Submitted By"),
                verified_by=verified_by or "System",
                manager_notes=notes or "",
                closing_fields=fields,
            )

    except Exception as e:
        print("Airtable update or verification email error:", e)
        raise HTTPException(status_code=500, detail="Failed to update verification status")

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
