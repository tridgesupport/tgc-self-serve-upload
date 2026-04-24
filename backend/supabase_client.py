import os
from typing import Optional

_client = None


def get_supabase():
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_SERVICE_KEY", "")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are not set."
            )
        from supabase import create_client
        _client = create_client(url, key)
    return _client


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

def upsert_product(data: dict) -> dict:
    res = get_supabase().table("products").upsert(
        data, on_conflict="shopify_product_id,vendor_id"
    ).execute()
    return res.data[0] if res.data else {}


def get_product_by_shopify_id(shopify_product_id: int, vendor_id: str) -> Optional[dict]:
    res = (
        get_supabase()
        .table("products")
        .select("*")
        .eq("shopify_product_id", shopify_product_id)
        .eq("vendor_id", vendor_id)
        .maybe_single()
        .execute()
    )
    return res.data


def get_product_by_id(product_id: str) -> Optional[dict]:
    res = (
        get_supabase()
        .table("products")
        .select("*")
        .eq("id", product_id)
        .maybe_single()
        .execute()
    )
    return res.data


def list_pending_products(vendor_id: Optional[str] = None) -> list[dict]:
    q = (
        get_supabase()
        .table("products")
        .select("*")
        .eq("status", "pending")
        .order("created_at", desc=True)
    )
    if vendor_id:
        q = q.eq("vendor_id", vendor_id)
    return q.execute().data or []


def list_approved_products(
    vendor_id: Optional[str] = None,
    level_1: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
    q = (
        get_supabase()
        .table("products")
        .select("*")
        .eq("status", "approved")
        .order("approved_at", desc=True)
        .range(offset, offset + limit - 1)
    )
    if vendor_id:
        q = q.eq("vendor_id", vendor_id)
    if level_1:
        q = q.eq("level_1", level_1)
    return q.execute().data or []


def approve_product(product_id: str, updates: dict) -> Optional[dict]:
    res = (
        get_supabase()
        .table("products")
        .update(updates)
        .eq("id", product_id)
        .execute()
    )
    return res.data[0] if res.data else None


def reject_product(product_id: str) -> None:
    get_supabase().table("products").delete().eq("id", product_id).execute()


def update_product_by_shopify_id(
    shopify_product_id: int, vendor_id: str, updates: dict
) -> Optional[dict]:
    res = (
        get_supabase()
        .table("products")
        .update(updates)
        .eq("shopify_product_id", shopify_product_id)
        .eq("vendor_id", vendor_id)
        .execute()
    )
    return res.data[0] if res.data else None


def delete_product_by_shopify_id(shopify_product_id: int, vendor_id: str) -> None:
    get_supabase().table("products").delete().eq(
        "shopify_product_id", shopify_product_id
    ).eq("vendor_id", vendor_id).execute()
