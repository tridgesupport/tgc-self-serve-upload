import os
import requests

WEBHOOK_TOPICS = ["products/create", "products/update", "products/delete"]


def register_product_webhooks(vendor: dict) -> dict[str, int]:
    """Register all three product event webhooks for a vendor via Shopify Admin API.

    Returns a dict mapping topic → Shopify webhook ID.
    Raises ValueError if APP_BASE_URL is not set or vendor is missing credentials.
    """
    store_url   = (vendor.get("store_url") or "").strip()
    admin_token = (vendor.get("admin_token") or "").strip()
    api_version = vendor.get("api_version") or "2025-04"
    vendor_id   = vendor.get("id")

    if not store_url or not admin_token:
        raise ValueError("Vendor must have both store_url and admin_token to register webhooks.")

    if not store_url.startswith("http"):
        store_url = f"https://{store_url}"

    app_base = os.environ.get("APP_BASE_URL", "").rstrip("/")
    if not app_base:
        raise ValueError("APP_BASE_URL environment variable is not set.")

    headers = {
        "X-Shopify-Access-Token": admin_token,
        "Content-Type": "application/json",
    }
    endpoint = f"{store_url}/admin/api/{api_version}/webhooks.json"
    webhook_ids: dict[str, int] = {}

    for topic in WEBHOOK_TOPICS:
        try:
            res = requests.post(
                endpoint,
                headers=headers,
                json={
                    "webhook": {
                        "topic":   topic,
                        "address": f"{app_base}/api/webhooks/shopify/{vendor_id}",
                        "format":  "json",
                    }
                },
                timeout=15,
            )
            if res.status_code in (200, 201):
                wh = res.json().get("webhook", {})
                webhook_ids[topic] = wh.get("id")
                print(f"[Webhooks] Registered {topic} → id={wh.get('id')} for vendor {vendor_id}")
            else:
                print(f"[Webhooks] Failed to register {topic}: HTTP {res.status_code} — {res.text[:300]}")
        except Exception as exc:
            print(f"[Webhooks] Exception registering {topic}: {exc}")

    return webhook_ids


def deregister_product_webhooks(vendor: dict, webhook_ids: dict[str, int]) -> None:
    """Delete previously registered webhooks from Shopify."""
    store_url   = (vendor.get("store_url") or "").strip()
    admin_token = (vendor.get("admin_token") or "").strip()
    api_version = vendor.get("api_version") or "2025-04"

    if not store_url or not admin_token:
        return

    if not store_url.startswith("http"):
        store_url = f"https://{store_url}"

    headers = {"X-Shopify-Access-Token": admin_token}

    for topic, wid in (webhook_ids or {}).items():
        if not wid:
            continue
        try:
            res = requests.delete(
                f"{store_url}/admin/api/{api_version}/webhooks/{wid}.json",
                headers=headers,
                timeout=10,
            )
            print(f"[Webhooks] Deregistered {topic} (id={wid}): HTTP {res.status_code}")
        except Exception as exc:
            print(f"[Webhooks] Exception deregistering {topic} id={wid}: {exc}")
