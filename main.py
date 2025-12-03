import os
import json
from datetime import date as dt_date, datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pyairtable import Table

load_dotenv()

# --------------------------------------------------------------------
# FASTAPI APP
# --------------------------------------------------------------------
app = FastAPI(title="Daily Sales & Cash Management API", version="1.0.0")

FRONTEND_URL = "https://restaurant-ops-dashboard-pflorencio.replit.app"
BACKEND_URL = "https://restaurant-ops-backend.onrender.com"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.options("/{rest:path}")
async def options_handler(request: Request, rest: str):
    resp = JSONResponse({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = FRONTEND_URL
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


# --------------------------------------------------------------------
# AIRTABLE HELPERS
# --------------------------------------------------------------------
def _table_from_env(env_id: str) -> Table:
    base = os.getenv("AIRTABLE_BASE_ID")
    key = os.getenv("AIRTABLE_API_KEY")
    table_id = os.getenv(env_id)
    if not (base and key and table_id):
        raise RuntimeError(f"Missing Airtable env vars for {env_id}")
    return Table(key, base, table_id)

DAILY = lambda: _table_from_env("AIRTABLE_DAILY_CLOSINGS_TABLE_ID")
HISTORY = lambda: _table_from_env("AIRTABLE_HISTORY_TABLE_ID")

DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "demo-tenant")


# --------------------------------------------------------------------
# MODELS
# --------------------------------------------------------------------
class ClosingCreate(BaseModel):
    business_date: dt_date
    store: str
    total_sales: float = 0.0
    net_sales: float = 0.0
    cash_payments: float = 0.0
    card_payments: float = 0.0
    digital_payments: float = 0.0
    grab_payments: float = 0.0
    bank_transfer_payments: float = 0.0
    voucher_payments: float = 0.0
    marketing_expenses: float = 0.0
    actual_cash_counted: float = 0.0
    cash_float: float = 0.0
    kitchen_budget: float = 0.0
    bar_budget: float = 0.0
    non_food_budget: float = 0.0
    staff_meal_budget: float = 0.0
    variance_cash: float = 0.0
    total_budgets: float = 0.0
    cash_for_deposit: float = 0.0
    transfer_needed: float = 0.0
    attachments: Optional[str] = None
    tenant_id: Optional[str] = None
    submitted_by: Optional[str] = None

class UnlockPayload(BaseModel):
    pin: str

# --------------------------------------------------------------------
# HISTORY LOGGER — FULL RESTORE (matches your Airtable exactly)
# --------------------------------------------------------------------
def _log_history(
    *,
    action: str,
    store: str,
    business_date: str,
    record_id: Optional[str],
    snapshot: dict,
    lock_status: Optional[str],
    changed_by: Optional[str],
    changed_fields: Optional[list[str]],
    tenant_id: str
):
    try:
        normalized = (
            store.lower()
            .strip()
            .replace("’", "")
            .replace("‘", "")
            .replace("'", "")
        )

        HISTORY().create({
            "Date": business_date,
            "Store": store,
            "Store Normalized": normalized,
            "Tenant ID": tenant_id,
            "Action": action,
            "Changed By": changed_by,
            "Timestamp": datetime.now().isoformat(),
            "Record ID": record_id,
            "Lock Status": lock_status,
            "Changed Fields": ", ".join(changed_fields) if changed_fields else None,
            "Snapshot": json.dumps(snapshot, ensure_ascii=False),
        })

    except Exception as e:
        print("⚠️ Failed to write history:", e)


# --------------------------------------------------------------------
# UPSERT CLOSING
# --------------------------------------------------------------------
@app.post("/closings")
def upsert_closing(payload: ClosingCreate):
    try:
        table = DAILY()
        store = payload.store.strip()
        business_date = payload.business_date.isoformat()
        tenant = payload.tenant_id or DEFAULT_TENANT_ID

        normalized = store.lower().replace("’", "").replace("‘", "").replace("'", "")

        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized}', "
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'),'day')"
            f")"
        )

        existing = table.all(formula=formula, max_records=1)

        fields = {
            "Date": business_date,
            "Store": store,
            "Store Normalized": normalized,
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
        }

        fields = {k: v for k, v in fields.items() if v is not None}

        # UPDATE
        if existing:
            record_id = existing[0]["id"]

            updated = table.update(record_id, {**fields, "Lock Status": "Locked"})
            snapshot = updated.get("fields", {})

            _log_history(
                action="Updated",
                store=store,
                business_date=business_date,
                record_id=record_id,
                snapshot=snapshot,
                lock_status=snapshot.get("Lock Status"),
                changed_by=payload.submitted_by,
                changed_fields=list(fields.keys()) + ["Lock Status"],
                tenant_id=tenant,
            )

            return {"status": "updated_locked", "id": record_id, "fields": snapshot}

        # CREATE
        created = table.create({**fields, "Lock Status": "Locked"})
        snapshot = created.get("fields", {})

        _log_history(
            action="Created",
            store=store,
            business_date=business_date,
            record_id=created.get("id"),
            snapshot=snapshot,
            lock_status=snapshot.get("Lock Status"),
            changed_by=payload.submitted_by,
            changed_fields=list(fields.keys()) + ["Lock Status"],
            tenant_id=tenant,
        )

        return {"status": "created_locked", "id": created.get("id"), "fields": snapshot}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------
# UNLOCK
# --------------------------------------------------------------------
@app.post("/closings/{record_id}/unlock")
def unlock_closing(record_id: str, payload: UnlockPayload):
    try:
        manager_pin = os.getenv("MANAGER_PIN")
        if payload.pin != manager_pin:
            raise HTTPException(status_code=401, detail="Invalid PIN")

        table = DAILY()
        updated = table.update(record_id, {
            "Lock Status": "Unlocked",
            "Unlocked At": datetime.now().isoformat(),
        })

        fields = updated.get("fields", {})
        store = fields.get("Store")
        date = fields.get("Date")

        _log_history(
            action="Unlocked",
            store=store,
            business_date=date,
            record_id=record_id,
            snapshot=fields,
            lock_status=fields.get("Lock Status"),
            changed_by="Manager PIN",
            changed_fields=["Lock Status", "Unlocked At"],
            tenant_id=fields.get("Tenant ID") or DEFAULT_TENANT_ID,
        )

        return {"status": "unlocked", "id": record_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------
# UNIQUE CLOSING (prefill)
# --------------------------------------------------------------------
@app.get("/closings/unique")
def get_unique_closing(business_date: str, store: str):
    try:
        table = DAILY()
        normalized = store.lower().replace("’", "").replace("‘", "").replace("'", "")

        formula = (
            f"AND("
            f"{{Store Normalized}}='{normalized}', "
            f"IS_SAME({{Date}}, DATETIME_PARSE('{business_date}','YYYY-MM-DD'),'day')"
            f")"
        )

        rows = table.all(formula=formula, max_records=1)
        if not rows:
            return {"status": "empty", "fields": {}, "lock_status": "Unlocked"}

        r = rows[0]
        return {
            "status": "found",
            "id": r["id"],
            "fields": r.get("fields", {}),
            "lock_status": r.get("fields", {}).get("Lock Status", "Unlocked"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
