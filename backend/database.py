import hashlib
import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "vendors.db")

_PATCHABLE = {
    "status", "notes", "brand_name", "contact_email", "contact_name",
    "contact_phone", "notify_email", "plan", "store_url", "logo_url",
    "sku_count", "price_range", "oos_mode", "processing_days",
    "shipping_region", "api_version", "bank_name", "account_name",
    "account_number", "ifsc", "account_type",
    "full_legal_name", "billing_street", "billing_city", "billing_state",
    "billing_pin", "billing_country", "catalogue_source", "drive_url",
}


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Password helpers (PBKDF2-HMAC-SHA256, no extra deps)
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
    return f"{salt}${h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, h_hex = stored.split("$", 1)
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
        return secrets.compare_digest(h.hex(), h_hex)
    except Exception:
        return False


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS vendors (
                id                    TEXT PRIMARY KEY,
                created_at            TEXT,
                submitted_at          TEXT,

                brand_name            TEXT,
                store_url             TEXT,
                plan                  TEXT,
                logo_url              TEXT,
                currency              TEXT DEFAULT 'INR',
                contact_name          TEXT,
                contact_email         TEXT,
                contact_phone         TEXT,
                notify_email          TEXT,

                pan                   TEXT,
                gst                   TEXT,
                business_type         TEXT,

                storefront_token      TEXT,
                admin_token           TEXT,
                webhook_secret        TEXT,
                api_version           TEXT DEFAULT '2025-04',

                stripe_connected      INTEGER DEFAULT 0,
                stripe_account_id     TEXT,
                account_name          TEXT,
                bank_name             TEXT,
                account_number        TEXT,
                ifsc                  TEXT,
                account_type          TEXT,

                categories            TEXT DEFAULT '[]',
                sku_count             TEXT,
                price_range           TEXT,
                oos_mode              TEXT DEFAULT 'hide',
                accepts_custom_orders INTEGER DEFAULT 0,

                processing_days       TEXT,
                warehouse_cities      TEXT DEFAULT '[]',
                shipping_region       TEXT,
                accepts_returns       INTEGER DEFAULT 0,
                return_days           TEXT,
                low_stock_alerts      INTEGER DEFAULT 0,

                full_legal_name       TEXT,
                billing_street        TEXT,
                billing_city          TEXT,
                billing_state         TEXT,
                billing_pin           TEXT,
                billing_country       TEXT DEFAULT 'India',
                catalogue_source      TEXT,
                drive_url             TEXT,

                status                TEXT DEFAULT 'pending',
                activated_at          TEXT,
                last_pulled_at        TEXT,
                notes                 TEXT,
                webhook_ids           TEXT DEFAULT '{}'
            );
        """)
            CREATE TABLE IF NOT EXISTS users (
                id            TEXT PRIMARY KEY,
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT DEFAULT 'vendor',
                vendor_id     TEXT,
                created_at    TEXT
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        # Add columns to existing databases that pre-date them
        for col_sql in [
            "ALTER TABLE vendors ADD COLUMN webhook_ids TEXT DEFAULT '{}'",
            "ALTER TABLE vendors ADD COLUMN full_legal_name TEXT",
            "ALTER TABLE vendors ADD COLUMN billing_street TEXT",
            "ALTER TABLE vendors ADD COLUMN billing_city TEXT",
            "ALTER TABLE vendors ADD COLUMN billing_state TEXT",
            "ALTER TABLE vendors ADD COLUMN billing_pin TEXT",
            "ALTER TABLE vendors ADD COLUMN billing_country TEXT DEFAULT 'India'",
            "ALTER TABLE vendors ADD COLUMN catalogue_source TEXT",
            "ALTER TABLE vendors ADD COLUMN drive_url TEXT",
        ]:
            try:
                conn.execute(col_sql)
            except Exception:
                pass


def _row_to_dict(row) -> Optional[dict]:
    if row is None:
        return None
    d = dict(row)
    for field in ("categories", "warehouse_cities"):
        try:
            d[field] = json.loads(d.get(field) or "[]")
        except Exception:
            d[field] = []
    for field in ("stripe_connected", "accepts_custom_orders", "accepts_returns", "low_stock_alerts"):
        d[field] = bool(d.get(field, 0))
    return d


def upsert_vendor(data: dict) -> dict:
    vendor_id = data.get("vendorId") or data.get("id")
    if not vendor_id:
        raise ValueError("vendorId is required")

    fields = {
        "id":                    vendor_id,
        "created_at":            data.get("createdAt"),
        "submitted_at":          data.get("submittedAt") or datetime.now().isoformat(),
        "brand_name":            data.get("brandName"),
        "store_url":             data.get("storeUrl"),
        "plan":                  data.get("plan"),
        "logo_url":              data.get("logoUrl"),
        "currency":              data.get("currency", "INR"),
        "contact_name":          data.get("contactName"),
        "contact_email":         data.get("contactEmail"),
        "contact_phone":         data.get("contactPhone"),
        "notify_email":          data.get("notifyEmail"),
        "pan":                   data.get("pan"),
        "gst":                   data.get("gst"),
        "business_type":         data.get("businessType"),
        "storefront_token":      data.get("storefrontToken"),
        "admin_token":           data.get("adminToken"),
        "webhook_secret":        data.get("webhookSecret"),
        "api_version":           data.get("apiVersion", "2025-04"),
        "stripe_connected":      int(bool(data.get("stripeConnected", False))),
        "stripe_account_id":     data.get("stripeAccountId"),
        "account_name":          data.get("accountName"),
        "bank_name":             data.get("bankName"),
        "account_number":        data.get("accountNumber"),
        "ifsc":                  data.get("ifsc"),
        "account_type":          data.get("accountType"),
        "categories":            json.dumps(data.get("categories", [])),
        "sku_count":             data.get("skuCount"),
        "price_range":           data.get("priceRange"),
        "oos_mode":              data.get("oosMode", "hide"),
        "accepts_custom_orders": int(bool(data.get("acceptsCustomOrders", False))),
        "processing_days":       data.get("processingDays"),
        "warehouse_cities":      json.dumps(data.get("warehouseCities", [])),
        "shipping_region":       data.get("shippingRegion"),
        "accepts_returns":       int(bool(data.get("acceptsReturns", False))),
        "return_days":           data.get("returnDays"),
        "low_stock_alerts":      int(bool(data.get("lowStockAlerts", False))),
        "full_legal_name":       data.get("fullLegalName"),
        "billing_street":        data.get("billingStreet"),
        "billing_city":          data.get("billingCity"),
        "billing_state":         data.get("billingState"),
        "billing_pin":           data.get("billingPin"),
        "billing_country":       data.get("billingCountry", "India"),
        "catalogue_source":      data.get("catalogueSource"),
        "drive_url":             data.get("driveUrl"),
    }

    cols         = list(fields.keys())
    placeholders = ", ".join("?" * len(cols))
    col_names    = ", ".join(cols)
    updates      = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "id")

    with get_db() as conn:
        conn.execute(
            f"INSERT INTO vendors ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {updates}",
            list(fields.values()),
        )
        row = conn.execute("SELECT * FROM vendors WHERE id=?", (vendor_id,)).fetchone()
    return _row_to_dict(row)


def list_vendors() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM vendors ORDER BY submitted_at DESC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_vendor(vendor_id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM vendors WHERE id=?", (vendor_id,)).fetchone()
    return _row_to_dict(row)


def patch_vendor(vendor_id: str, updates: dict) -> Optional[dict]:
    clean = {k: v for k, v in updates.items() if k in _PATCHABLE}
    if not clean:
        return get_vendor(vendor_id)
    if clean.get("status") == "active":
        clean["activated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k}=?" for k in clean)
    with get_db() as conn:
        conn.execute(
            f"UPDATE vendors SET {set_clause} WHERE id=?",
            list(clean.values()) + [vendor_id],
        )
        row = conn.execute("SELECT * FROM vendors WHERE id=?", (vendor_id,)).fetchone()
    return _row_to_dict(row)


def set_last_pulled(vendor_id: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE vendors SET last_pulled_at=? WHERE id=?",
            (datetime.now().isoformat(), vendor_id),
        )


def update_webhook_ids(vendor_id: str, webhook_ids: dict) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE vendors SET webhook_ids=? WHERE id=?",
            (json.dumps(webhook_ids), vendor_id),
        )


def get_webhook_ids(vendor_id: str) -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT webhook_ids FROM vendors WHERE id=?", (vendor_id,)
        ).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row["webhook_ids"] or "{}")
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

SESSION_TTL_DAYS = 30


def create_user(email: str, password: str, role: str = "vendor", vendor_id: Optional[str] = None) -> dict:
    user_id = str(__import__("uuid").uuid4())
    pw_hash = hash_password(password)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO users (id, email, password_hash, role, vendor_id, created_at) VALUES (?,?,?,?,?,?)",
            (user_id, email.lower().strip(), pw_hash, role, vendor_id, datetime.now().isoformat()),
        )
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    return dict(row)


def get_user_by_email(email: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email=?", (email.lower().strip(),)).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    return dict(row) if row else None


def create_session(user_id: str) -> str:
    token = secrets.token_hex(32)
    expires = (datetime.now() + timedelta(days=SESSION_TTL_DAYS)).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)",
            (token, user_id, expires),
        )
    return token


def get_session_user(token: str) -> Optional[dict]:
    """Return user dict if session token is valid and not expired."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE token=?", (token,)
        ).fetchone()
    if not row:
        return None
    if datetime.fromisoformat(row["expires_at"]) < datetime.now():
        return None
    return get_user_by_id(row["user_id"])


def delete_session(token: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))


def update_password(user_id: str, new_password: str) -> None:
    pw_hash = hash_password(new_password)
    with get_db() as conn:
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (pw_hash, user_id))
