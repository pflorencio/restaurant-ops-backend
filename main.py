import os
import smtplib
import json
from email.message import EmailMessage
from typing import Optional, List
from datetime import date as dt_date, datetime
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, RootModel
from dotenv import load_dotenv
from pyairtable import Table

# -----------------------------------------------------------
# 🔧 Load environment
# -----------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------
# 🚀 FastAPI App Init (Production mode)
# -----------------------------------------------------------
app = FastAPI(title="Daily Sales & Cash Management API", version="1.0.0")

# -----------------------------------------------------------
# 🌐 Production CORS — KEEP THESE TWO ONLY
# -----------------------------------------------------------
FRONTEND_URL = "https://restaurant-ops-dashboard-pflorencio.replit.app"
BACKEND_URL = "https://restaurant-ops-backend.onrender.com"

origins = [
    FRONTEND_URL,
    BACKEND_URL,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Handle OPTIONS preflight
@app.options("/{rest_of_path:path}")
async def options_handler(request: Request, rest_of_path: str):
    response = JSONResponse({"ok": True})
    response.headers["Access-Control-Allow-Origin"] = FRONTEND_URL
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


# -----------------------------------------------------------
# 🔗 Airtable Table Helper (Strict Mode)
# -----------------------------------------------------------
def _airtable_table(table_key: str) -> Table:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")

    if not base_id or not api_key:
        raise RuntimeError("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY")

    tables = {
        "daily_closing": {
            "id_env": "AIRTABLE_DAILY_CLOSINGS_TABLE_ID",
            "default_name": "Daily Closing",
        },
        "history": {
            "id_env": "AIRTABLE_HISTORY_TABLE_ID",
            "default_name": "Daily Closing History",
        },
    }

    cfg = tables[table_key]
    table_id = os.getenv(cfg["id_env"])

    if not table_id:
        raise RuntimeError(
            f"Missing required Airtable table ID env var: {cfg['id_env']}"
        )

    return Table(api_key, base_id, table_id)


DAILY_CLOSINGS_TABLE = "daily_closing"
HISTORY_TABLE = "history"

DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")


def resolve_tenant_id(explicit: Optional[str]) -> str:
    return explicit or DEFAULT_TENANT_ID


# -----------------------------------------------------------
# 🧩 Models
# -----------------------------------------------------------
class ClosingCreate(BaseModel):
    business_date: dt_date
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


class CashierLoginRequest(BaseModel):
    cashier_id: str
    pin: str


# -----------------------------------------------------------
# 🔍 Basic Routes
# -----------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "daily-sales-api", "version": "1.0.0"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


# -----------------------------------------------------------
# 🔐 AUTH — Cashier List + Login
# -----------------------------------------------------------
@app.get("/auth/cashiers")
def list_cashiers():
    try:
        base_id = os.getenv("AIRTABLE_BASE_ID")
        api_key = os.getenv("AIRTABLE_API_KEY")

        table = Table(api_key, base_id, "Cashiers")
        records = table.all(formula="{Active} = TRUE()", max_records=100)

        result = []
        for r in records:
            f = r.get("fields", {})
            store = f.get("Store") or ""

            result.append(
                {
                    "cashier_id": f.get("Cashier ID"),
                    "name": f.get("Name"),
                    "store": store,
                    "store_normalized": f.get("Store Normalized")
                    or store.lower().replace("’", "").replace("‘", "").replace("'", ""),
                }
            )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/cashier-login")
def cashier_login(payload: CashierLoginRequest):
    try:
        base_id = os.getenv("AIRTABLE_BASE_ID")
        api_key = os.getenv("AIRTABLE_API_KEY")
        table = Table(api_key, base_id, "Cashiers")

        formula = (
            f"AND("
            f"{{Cashier ID}} = '{payload.cashier_id.strip()}', "
            f"{{PIN}} = '{payload.pin.strip()}', "
            f"{{Active}} = TRUE()"
            f")"
        )

        records = table.all(formula=formula, max_records=1)
        if not records:
            raise HTTPException(status_code=401, detail="Invalid cashier ID or PIN")

        f = records[0]["fields"]
        store = f.get("Store") or ""
        normalized = f.get("Store Normalized") or store.lower().replace("’", "").replace("‘", "").replace("'", "")

        return {
            "cashier_id": f.get("Cashier ID"),
            "name": f.get("Name"),
            "store": store,
            "store_normalized": normalized,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# 📝 HISTORY LOGGER
# -----------------------------------------------------------
def _safe_serialize(obj):
    if isinstance(obj, (datetime, dt_date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_serialize(v) for v in obj]
    return obj


def _log_history(**kwargs):
    try:
        table = _airtable_table(HISTORY_TABLE)
        payload = json.loads(json.dumps(kwargs, default=str))
        table.create(payload)
    except Exception as e:
        print("⚠️ Failed to log history:", e)


# -----------------------------------------------------------
# 🛠️ CRITICAL FIX — CLEAN OUT READ-ONLY FIELDS
# -----------------------------------------------------------
def format_fields_for_airtable(payload: dict):
    """
    Removes formula, lookup, readonly, and auto fields.
    Airtable will reject creation if any of these are included.
    """
    READ_ONLY_FIELDS = {
        "Store Normalized",
        "Submitted By Name",
        "Variance Display",
        "Cash for Deposit Display",
        "Transfer Needed Display",
        "Created Time",
        "Last Modified Time",
        "Record ID",
    }

    clean = {}
    for key, value in payload.items():
        if key in READ_ONLY_FIELDS:
            continue
        if value is None:
            continue
        clean[key] = value

    return clean


# -----------------------------------------------------------
# 📌 UPSERT — Create or Update Closing
# -----------------------------------------------------------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        store = payload.store.strip()
        business_date = payload.business_date.isoformat()
        tenant = resolve_tenant_id(payload.tenant_id)

        normalized_store = store.lower().replace("’", "").replace("‘", "").replace("'", "")
        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized_store}', "
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
            f")"
        )

        existing = table.all(formula=formula, max_records=1)

        fields = {
            "Date": business_date,
            "Store": store,
            "Store Normalized": normalized_store,
            "Tenant ID": tenant,
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
            "Lock Status": "Locked",
        }

        # ❗ Remove all computed/formula fields before sending to Airtable
        fields = format_fields_for_airtable(fields)

        # Update existing
        if existing:
            record_id = existing[0]["id"]
            updated = table.update(record_id, fields)

            _log_history(
                action="Updated",
                store=store,
                business_date=business_date,
                record_id=record_id,
                snapshot=updated.get("fields", {}),
                submitted_by=payload.submitted_by,
                tenant_id=tenant,
            )

            return {
                "status": "updated_locked",
                "id": record_id,
                "fields": updated.get("fields", {}),
            }

        # Create new
        created = table.create(fields)

        _log_history(
            action="Created",
            store=store,
            business_date=business_date,
            record_id=created.get("id"),
            snapshot=created.get("fields", {}),
            submitted_by=payload.submitted_by,
            tenant_id=tenant,
        )

        return {
            "status": "created_locked",
            "id": created.get("id"),
            "fields": created.get("fields", {}),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# 🔓 UNLOCK (Manager PIN)
# -----------------------------------------------------------
class UnlockPayload(BaseModel):
    pin: str


@app.post("/closings/{record_id}/unlock")
def unlock_closing(record_id: str, payload: UnlockPayload):
    try:
        manager_pin = os.getenv("MANAGER_PIN")
        if payload.pin != manager_pin:
            raise HTTPException(status_code=401, detail="Invalid manager PIN")

        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        updated = table.update(
            record_id,
            {"Lock Status": "Unlocked", "Unlocked At": datetime.now().isoformat()},
        )

        _log_history(
            action="Unlocked",
            store=updated["fields"].get("Store", ""),
            business_date=updated["fields"].get("Date", ""),
            record_id=record_id,
            snapshot=updated["fields"],
            submitted_by="Manager PIN",
        )

        return {"status": "unlocked", "id": record_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# 🔍 Fetch Unique Closing
# -----------------------------------------------------------
@app.get("/closings/unique")
def get_unique_closing(business_date: str, store: str):
    try:
        table = _airtable_table(DAILY_CLOSINGS_TABLE)
        normalized = store.lower().replace("’", "").replace("‘", "").replace("'", "")

        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized}', "
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}', 'YYYY-MM-DD'), 'day')"
            f")"
        )

        records = table.all(formula=formula, max_records=1)

        if not records:
            return {"status": "empty", "fields": {}, "lock_status": "Unlocked"}

        r = records[0]
        return {
            "status": "found",
            "id": r["id"],
            "fields": r.get("fields", {}),
            "lock_status": r.get("fields", {}).get("Lock Status", "Unlocked"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------
# 🌅 Entrypoint
# -----------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
