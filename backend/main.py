import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.background import BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

SCRAPE_TIMEOUT_SECS = 300   # 5 minutes overall
SCRAPE_MAX_RETRIES  = 2
_executor = ThreadPoolExecutor(max_workers=4)

# In-memory job store: job_id -> {status, result, detail}
_jobs: dict[str, dict] = {}

load_dotenv()

# Make sure sibling modules are importable when running from project root
sys.path.insert(0, os.path.dirname(__file__))

from drive_client import (
    append_to_imagekit_sheet,
    create_brand_folder,
    read_sheet_data,
    upload_csv_to_drive,
    PARENT_FOLDER_ID,
)
from drive_scraper import scrape_drive, extract_drive_id
from imagekit_client import upload_to_imagekit
from instagram_scraper import scrape_instagram
from database import (
    init_db, upsert_vendor, list_vendors, get_vendor, patch_vendor,
    set_last_pulled, update_webhook_ids, get_webhook_ids,
)
from scraper import detect_and_scrape, scrape_shopify, scrape_shopify_authenticated, clean_html
from supabase_client import (
    upsert_product,
    get_product_by_shopify_id,
    get_product_by_id,
    list_pending_products,
    list_approved_products,
    approve_product as sb_approve_product,
    reject_product as sb_reject_product,
    update_product_by_shopify_id,
    delete_product_by_shopify_id,
)
from shopify_webhooks import register_product_webhooks, deregister_product_webhooks

app = FastAPI(title="TGC Self-Serve Upload")
init_db()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ScrapeRequest(BaseModel):
    url: str
    brand: str


class AssetIn(BaseModel):
    url: str
    type: str  # 'image' | 'video'


class ProductIn(BaseModel):
    product_name: str
    price: str
    product_description: str
    assets: list[AssetIn]
    brand: Optional[str] = ""
    level_1: Optional[str] = ""
    level_2: Optional[str] = ""
    level_3: Optional[str] = ""
    level_4: Optional[str] = ""
    level_5: Optional[str] = ""


class PushRequest(BaseModel):
    products: list[ProductIn]  # only products with selected assets
    brand: str


class InstagramRequest(BaseModel):
    handle: str
    brand: str


class InstagramPushRequest(BaseModel):
    products: list[ProductIn]
    brand: str


class VendorRegisterRequest(BaseModel):
    """Mirrors the localStorage shape from the vendor registration form."""
    vendorId: str
    brandName: Optional[str] = None
    storeUrl: Optional[str] = None
    plan: Optional[str] = None
    logoUrl: Optional[str] = None
    currency: Optional[str] = "INR"
    contactName: Optional[str] = None
    contactEmail: Optional[str] = None
    contactPhone: Optional[str] = None
    notifyEmail: Optional[str] = None
    pan: Optional[str] = None
    gst: Optional[str] = None
    businessType: Optional[str] = None
    storefrontToken: Optional[str] = None
    adminToken: Optional[str] = None
    webhookSecret: Optional[str] = None
    apiVersion: Optional[str] = "2025-04"
    stripeConnected: Optional[bool] = False
    stripeAccountId: Optional[str] = None
    accountName: Optional[str] = None
    bankName: Optional[str] = None
    accountNumber: Optional[str] = None
    ifsc: Optional[str] = None
    accountType: Optional[str] = None
    categories: Optional[list] = []
    skuCount: Optional[str] = None
    priceRange: Optional[str] = None
    oosMode: Optional[str] = "hide"
    acceptsCustomOrders: Optional[bool] = False
    processingDays: Optional[str] = None
    warehouseCities: Optional[list] = []
    shippingRegion: Optional[str] = None
    acceptsReturns: Optional[bool] = False
    returnDays: Optional[str] = None
    lowStockAlerts: Optional[bool] = False
    createdAt: Optional[str] = None
    submittedAt: Optional[str] = None
    submitted: Optional[bool] = False


class VendorPatchRequest(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    brand_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: Optional[str] = None
    plan: Optional[str] = None


class ApproveProductRequest(BaseModel):
    assets: list[AssetIn]
    brand: str
    product_name: str
    product_description: str
    price: str
    level_1: Optional[str] = ""
    level_2: Optional[str] = ""
    level_3: Optional[str] = ""
    level_4: Optional[str] = ""
    level_5: Optional[str] = ""


# Sheet URL is fixed — stored in env var, not entered by users
VENDOR_SHEET_URL = os.environ.get(
    "VENDOR_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1_TqzNolDHdHGECOhLVHpu2cpldcXJwjr7FUUQjuuZwE/edit?gid=67972522#gid=67972522",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize(name: str) -> str:
    return re.sub(r"[^\w\-]", "_", name).strip("_")



# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verify_shopify_hmac(body: bytes, hmac_header: str, secret: str) -> bool:
    """Return True if the Shopify HMAC-SHA256 signature is valid.

    If secret is empty (vendor hasn't set one), skip verification and accept
    the request — tighten this once secrets are confirmed working.
    """
    if not secret or not hmac_header:
        return True
    computed = base64.b64encode(
        hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    ).decode()
    return hmac.compare_digest(computed, hmac_header)


def _handle_shopify_event(vendor: dict, topic: str, payload: dict) -> None:
    """Sync handler called as a FastAPI BackgroundTask for Shopify webhook events."""
    vendor_id  = vendor["id"]
    brand_name = vendor.get("brand_name") or vendor_id
    shopify_id = payload.get("id")
    if not shopify_id:
        return

    try:
        if topic == "products/create":
            assets = [
                {"url": img["src"], "type": "image"}
                for img in payload.get("images", [])
                if img.get("src")
            ]
            upsert_product({
                "shopify_product_id": shopify_id,
                "vendor_id":          vendor_id,
                "vendor_brand_name":  brand_name,
                "title":              payload.get("title", ""),
                "description":        clean_html(payload.get("body_html", "")),
                "price":              str(payload["variants"][0].get("price", "0"))
                                      if payload.get("variants") else "0",
                "assets":             assets,
                "status":             "pending",
                "shopify_updated_at": payload.get("updated_at"),
            })
            print(f"[Webhook] New product queued for approval: {payload.get('title')} (vendor={vendor_id})")

        elif topic == "products/update":
            existing = get_product_by_shopify_id(shopify_id, vendor_id)
            if not existing:
                return  # product not in our catalogue yet, ignore
            assets = [
                {"url": img["src"], "type": "image"}
                for img in payload.get("images", [])
                if img.get("src")
            ]
            updates = {
                "title":              payload.get("title", existing.get("title", "")),
                "description":        clean_html(payload.get("body_html", "")),
                "price":              str(payload["variants"][0].get("price", "0"))
                                      if payload.get("variants") else existing.get("price", "0"),
                "assets":             assets,
                "shopify_updated_at": payload.get("updated_at"),
            }
            update_product_by_shopify_id(shopify_id, vendor_id, updates)
            print(f"[Webhook] Product updated: {payload.get('title')} (vendor={vendor_id})")

        elif topic == "products/delete":
            delete_product_by_shopify_id(shopify_id, vendor_id)
            print(f"[Webhook] Product deleted: shopify_id={shopify_id} (vendor={vendor_id})")

    except Exception as exc:
        print(f"[Webhook] Failed to handle {topic} for vendor {vendor_id}: {exc}")


# ---------------------------------------------------------------------------
# Shopify webhook receiver
# ---------------------------------------------------------------------------

@app.post("/api/webhooks/shopify/{vendor_id}", status_code=200)
async def shopify_webhook(vendor_id: str, request: Request, background_tasks: BackgroundTasks):
    """Receive Shopify product webhook events, verify HMAC, handle asynchronously."""
    body = await request.body()

    vendor = await asyncio.get_event_loop().run_in_executor(_executor, get_vendor, vendor_id)
    if not vendor:
        # Return 200 so Shopify doesn't keep retrying for unknown vendors
        return {"received": False, "reason": "unknown vendor"}

    hmac_header    = request.headers.get("X-Shopify-Hmac-Sha256", "")
    webhook_secret = (vendor.get("webhook_secret") or "").strip()

    if not _verify_shopify_hmac(body, hmac_header, webhook_secret):
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")

    topic   = request.headers.get("X-Shopify-Topic", "")
    payload = json.loads(body) if body else {}

    background_tasks.add_task(_handle_shopify_event, vendor, topic, payload)
    return {"received": True}


# ---------------------------------------------------------------------------
# Product catalogue (production frontend)
# ---------------------------------------------------------------------------

@app.get("/api/catalogue")
async def get_catalogue(
    vendor_id: Optional[str] = None,
    level_1:   Optional[str] = None,
    limit:     int = 500,
    offset:    int = 0,
):
    """Public endpoint: returns approved products for the production frontend."""
    try:
        products = await asyncio.get_event_loop().run_in_executor(
            _executor, list_approved_products, vendor_id, level_1, limit, offset
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"products": products, "count": len(products)}


# ---------------------------------------------------------------------------
# Admin product endpoints (pending queue + approve/reject)
# ---------------------------------------------------------------------------

@app.get("/api/admin/products/pending")
async def get_pending_products(vendor_id: Optional[str] = None):
    """Return all products awaiting admin approval."""
    try:
        products = await asyncio.get_event_loop().run_in_executor(
            _executor, list_pending_products, vendor_id
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"products": products, "count": len(products)}


@app.post("/api/admin/products/{product_id}/approve")
async def approve_product_endpoint(product_id: str, req: ApproveProductRequest):
    """Approve a pending product: upload selected assets to ImageKit, update Supabase."""
    loop = asyncio.get_event_loop()

    try:
        product = await loop.run_in_executor(_executor, get_product_by_id, product_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if not product:
        raise HTTPException(status_code=404, detail="Product not found.")

    brand_safe = sanitize(req.brand or product.get("vendor_brand_name") or "unknown")

    metadata = {
        "product_name":        req.product_name,
        "product_description": req.product_description,
        "price":               req.price,
        "brand":               req.brand,
        "level_1":             req.level_1 or "",
        "level_2":             req.level_2 or "",
        "level_3":             req.level_3 or "",
        "level_4":             req.level_4 or "",
        "level_5":             req.level_5 or "",
    }

    uploaded_assets: list[dict] = []
    first_ik_url = None
    first_ik_fid = None

    for i, asset in enumerate(req.assets):
        url_path = urlparse(asset.url).path
        _, ext   = os.path.splitext(url_path)
        if ext.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".mp4", ".mov", ".webm"}:
            ext = ".jpg" if asset.type != "video" else ".mp4"
        filename = sanitize(f"{brand_safe}_{req.product_name}_{i + 1}") + ext

        result = await loop.run_in_executor(
            _executor, upload_to_imagekit, asset.url, filename, brand_safe, metadata
        )
        if result:
            uploaded_assets.append({"url": result["url"], "type": asset.type})
            if not first_ik_url:
                first_ik_url = result.get("url")
                first_ik_fid = result.get("fileId")
        else:
            uploaded_assets.append({"url": asset.url, "type": asset.type})

    update_data = {
        "status":           "approved",
        "approved_at":      datetime.now(timezone.utc).isoformat(),
        "title":            req.product_name,
        "description":      req.product_description,
        "price":            req.price,
        "assets":           uploaded_assets,
        "imagekit_url":     first_ik_url,
        "imagekit_file_id": first_ik_fid,
        "level_1":          req.level_1 or "",
        "level_2":          req.level_2 or "",
        "level_3":          req.level_3 or "",
        "level_4":          req.level_4 or "",
        "level_5":          req.level_5 or "",
    }

    try:
        updated = await loop.run_in_executor(
            _executor, sb_approve_product, product_id, update_data
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return {"product": updated, "upload_count": len(uploaded_assets)}


@app.delete("/api/admin/products/{product_id}", status_code=204)
async def reject_product_endpoint(product_id: str):
    """Reject (delete) a pending product."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            _executor, sb_reject_product, product_id
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


# ---------------------------------------------------------------------------
# Vendor registration & admin endpoints
# ---------------------------------------------------------------------------

@app.post("/api/vendors", status_code=201)
async def register_vendor(req: VendorRegisterRequest):
    """Persist a submitted vendor registration to the database."""
    try:
        vendor = await asyncio.get_event_loop().run_in_executor(
            _executor, upsert_vendor, req.model_dump()
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return vendor


@app.get("/api/vendors")
async def list_vendors_endpoint():
    """Return all registered vendors for the admin panel."""
    vendors = await asyncio.get_event_loop().run_in_executor(_executor, list_vendors)
    return {"vendors": vendors, "count": len(vendors)}


@app.patch("/api/vendors/{vendor_id}")
async def patch_vendor_endpoint(vendor_id: str, req: VendorPatchRequest):
    """Partial update — status, notes, and other editable fields.

    When status is set to 'active' and the vendor has an admin_token,
    automatically registers Shopify product webhooks.
    """
    loop    = asyncio.get_event_loop()
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    vendor  = await loop.run_in_executor(_executor, patch_vendor, vendor_id, updates)
    if vendor is None:
        raise HTTPException(status_code=404, detail="Vendor not found.")

    # Auto-register webhooks when vendor is activated for the first time
    if updates.get("status") == "active" and vendor.get("admin_token"):
        existing_ids = await loop.run_in_executor(_executor, get_webhook_ids, vendor_id)
        if not existing_ids:
            try:
                new_ids = await loop.run_in_executor(
                    _executor, register_product_webhooks, vendor
                )
                if new_ids:
                    await loop.run_in_executor(
                        _executor, update_webhook_ids, vendor_id, new_ids
                    )
                    print(f"[Webhooks] Registered {len(new_ids)} webhooks for vendor {vendor_id}")
            except Exception as exc:
                print(f"[Webhooks] Registration failed for {vendor_id}: {exc}")
                # Non-fatal — vendor is still activated, just without webhooks

    return vendor


# ---------------------------------------------------------------------------
# Vendor product pull — same async job flow as /api/scrape
# ---------------------------------------------------------------------------

async def _run_vendor_pull_job(job_id: str, vendor_id: str):
    """Background task: pull vendor's Shopify catalogue and upload to Drive."""
    try:
        await _do_vendor_pull_job(job_id, vendor_id)
    except BaseException as exc:
        print(f"[VendorPull] Unhandled crash for job {job_id}: {exc}")
        _jobs[job_id] = {"status": "error", "detail": "An unexpected error occurred while pulling products."}


async def _do_vendor_pull_job(job_id: str, vendor_id: str):
    loop    = asyncio.get_event_loop()
    vendor  = await loop.run_in_executor(_executor, get_vendor, vendor_id)

    if not vendor:
        _jobs[job_id] = {"status": "error", "detail": "Vendor not found."}
        return

    store_url    = (vendor.get("store_url") or "").strip()
    admin_token  = (vendor.get("admin_token") or "").strip()
    api_version  = vendor.get("api_version") or "2025-04"
    brand        = vendor.get("brand_name") or vendor_id

    if not store_url:
        _jobs[job_id] = {"status": "error", "detail": "Vendor has no store URL configured."}
        return

    if not store_url.startswith("http"):
        store_url = f"https://{store_url}"

    deadline = loop.time() + SCRAPE_TIMEOUT_SECS
    products, platform = [], "shopify"

    for attempt in range(SCRAPE_MAX_RETRIES):
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            if admin_token:
                products = await asyncio.wait_for(
                    loop.run_in_executor(
                        _executor, scrape_shopify_authenticated, store_url, admin_token, api_version
                    ),
                    timeout=remaining,
                )
                platform = "shopify_admin"
            else:
                products = await asyncio.wait_for(
                    loop.run_in_executor(_executor, scrape_shopify, store_url),
                    timeout=remaining,
                )
                platform = "shopify"
            if products:
                break
        except asyncio.TimeoutError:
            _jobs[job_id] = {
                "status": "error",
                "detail": "Product pull timed out after 5 minutes.",
            }
            return
        except Exception as exc:
            print(f"[VendorPull] Attempt {attempt + 1} failed: {exc}")
            if attempt < SCRAPE_MAX_RETRIES - 1:
                await asyncio.sleep(2)

    if not products:
        _jobs[job_id] = {
            "status": "error",
            "detail": "No products found. Check the store URL and API token are correct.",
        }
        return

    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    brand_safe = sanitize(brand)

    folder_id  = None
    folder_url = None
    try:
        folder_id, folder_url, _ = create_brand_folder(brand_safe, timestamp)
    except Exception as exc:
        print(f"[Drive] Folder creation failed: {exc}")

    csv_rows: list[dict] = []
    for prod in products:
        row: dict = {
            "product_name":        prod["product_name"],
            "price":               prod["price"],
            "product_description": prod["product_description"],
            "brand":               brand,
        }
        for i, asset in enumerate(prod["assets"][:4]):
            row[f"asset_{i+1}_url"]  = asset["url"]
            row[f"asset_{i+1}_type"] = asset["type"]
        csv_rows.append(row)

    if folder_id and csv_rows:
        csv_headers = [
            "product_name", "price", "product_description", "brand",
            "asset_1_url", "asset_1_type",
            "asset_2_url", "asset_2_type",
            "asset_3_url", "asset_3_type",
            "asset_4_url", "asset_4_type",
        ]
        try:
            upload_csv_to_drive(
                csv_rows, csv_headers,
                f"{brand_safe}_{timestamp}_catalog.csv",
                folder_id,
            )
        except Exception as exc:
            print(f"[Drive] CSV upload error: {exc}")

    await loop.run_in_executor(_executor, set_last_pulled, vendor_id)

    _jobs[job_id] = {
        "status": "done",
        "result": {
            "products":        products,
            "platform":        platform,
            "count":           len(products),
            "drive_folder_url": folder_url,
        },
    }


@app.post("/api/vendors/{vendor_id}/pull-products", status_code=202)
async def vendor_pull_products(vendor_id: str, background_tasks: BackgroundTasks):
    """Start an async product pull for the given vendor. Returns a job_id to poll."""
    vendor = await asyncio.get_event_loop().run_in_executor(_executor, get_vendor, vendor_id)
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found.")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "pending"}
    background_tasks.add_task(_run_vendor_pull_job, job_id, vendor_id)
    return {"job_id": job_id}


# ---------------------------------------------------------------------------
# POST /api/scrape  — starts a background job, returns job_id immediately
# GET  /api/scrape-status/{job_id} — poll for result
# ---------------------------------------------------------------------------

async def _run_scrape_job(job_id: str, req: ScrapeRequest):
    """Background task: scrape + Drive upload. Writes result into _jobs."""
    try:
        await _do_scrape_job(job_id, req)
    except BaseException as exc:
        print(f"[Scraper] Unhandled crash for job {job_id}: {exc}")
        _jobs[job_id] = {"status": "error", "detail": "An unexpected error occurred while scraping."}


async def _do_scrape_job(job_id: str, req: ScrapeRequest):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + SCRAPE_TIMEOUT_SECS
    products, platform = [], "unknown"

    for attempt in range(SCRAPE_MAX_RETRIES):
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            products, platform = await asyncio.wait_for(
                loop.run_in_executor(_executor, detect_and_scrape, req.url),
                timeout=remaining,
            )
            if products:
                break
        except asyncio.TimeoutError:
            _jobs[job_id] = {
                "status": "error",
                "detail": "This website couldn't be scraped — it timed out after 5 minutes. "
                          "The site may not expose a public product API.",
            }
            return
        except Exception as exc:
            print(f"[Scraper] Attempt {attempt + 1} failed: {exc}")
            if attempt < SCRAPE_MAX_RETRIES - 1:
                await asyncio.sleep(2)

    if not products:
        _jobs[job_id] = {
            "status": "error",
            "detail": f"This website couldn't be scraped after {SCRAPE_MAX_RETRIES} attempts. "
                      "It may not be a Shopify or WordPress store, or the product API may be private.",
        }
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    brand_safe = sanitize(req.brand)

    folder_id = None
    folder_url = None
    drive_ok = False
    try:
        folder_id, folder_url, _ = create_brand_folder(brand_safe, timestamp)
        drive_ok = True
    except Exception as exc:
        print(f"[Drive] Folder creation failed: {exc}")

    csv_rows: list[dict] = []
    for prod in products:
        row: dict = {
            "product_name": prod["product_name"],
            "price": prod["price"],
            "product_description": prod["product_description"],
            "brand": req.brand,
        }
        for i, asset in enumerate(prod["assets"][:4]):
            row[f"asset_{i+1}_url"]  = asset["url"]
            row[f"asset_{i+1}_type"] = asset["type"]
        csv_rows.append(row)

    if drive_ok and folder_id and csv_rows:
        csv_headers = [
            "product_name", "price", "product_description", "brand",
            "asset_1_url", "asset_1_type",
            "asset_2_url", "asset_2_type",
            "asset_3_url", "asset_3_type",
            "asset_4_url", "asset_4_type",
        ]
        try:
            upload_csv_to_drive(
                csv_rows, csv_headers,
                f"{brand_safe}_{timestamp}_catalog.csv",
                folder_id,
            )
        except Exception as exc:
            print(f"[Drive] CSV upload error: {exc}")

    _jobs[job_id] = {
        "status": "done",
        "result": {
            "products": products,
            "platform": platform,
            "count": len(products),
            "drive_folder_url": folder_url,
        },
    }


@app.post("/api/scrape", status_code=202)
async def scrape_endpoint(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start a scrape job and return a job_id immediately."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "pending"}
    background_tasks.add_task(_run_scrape_job, job_id, req)
    return {"job_id": job_id}


@app.get("/api/scrape-status/{job_id}")
async def scrape_status(job_id: str):
    """Poll for the result of a scrape job."""
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


# ---------------------------------------------------------------------------
# POST /api/scrape-drive
# ---------------------------------------------------------------------------

@app.post("/api/scrape-drive")
async def scrape_drive_endpoint(req: ScrapeRequest):
    """
    Scrape media from a public Google Drive folder or file URL.
    Downloads files, uploads to a new Drive folder, returns product list.
    """
    # Quick URL validation before doing any work
    drive_id, id_type = extract_drive_id(req.url)
    if not drive_id:
        raise HTTPException(
            status_code=400,
            detail="That doesn't look like a valid Google Drive URL. "
                   "Please paste a folder link (drive.google.com/drive/folders/...) "
                   "or a file link (drive.google.com/file/d/...).",
        )

    try:
        products, status, folder_url, error_detail = await asyncio.get_event_loop().run_in_executor(
            _executor,
            scrape_drive,
            req.url,
            req.brand,
            PARENT_FOLDER_ID,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail="The Drive download timed out. Try a folder with fewer files.",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}")

    base_messages = {
        "invalid_url": "That doesn't look like a valid Google Drive URL.",
        "not_public": (
            "This Google Drive folder is not accessible. "
            "Set sharing to 'Anyone with the link can view', or share it directly with the service account email."
        ),
        "empty": "No image or video files were found at that Drive URL.",
        "error": "Could not access that Drive folder.",
    }

    if status != "ok":
        base = base_messages.get(status, "Unknown error.")
        detail = f"{base} — {error_detail}" if error_detail else base
        raise HTTPException(status_code=400, detail=detail)

    return {
        "products": products,
        "count": len(products),
        "drive_folder_url": folder_url,
        "platform": "google_drive",
    }


# ---------------------------------------------------------------------------
# POST /api/scrape-instagram
# ---------------------------------------------------------------------------

@app.post("/api/scrape-instagram")
async def scrape_instagram_endpoint(req: InstagramRequest):
    """
    Scrape the latest posts from a public Instagram profile.
    Returns up to 20 posts with image/video assets.
    """
    handle = req.handle.lstrip("@").strip()
    if not handle:
        raise HTTPException(status_code=400, detail="Please enter an Instagram handle.")

    try:
        products, status = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(_executor, scrape_instagram, handle),
            timeout=300,  # 5 minutes — Apify runs can be slow
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail="Instagram scraping timed out after 5 minutes. Please try again.",
        )
    except Exception as exc:
        print(f"[Instagram] Unexpected error: {exc}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred. Please try again.")

    if status != "ok":
        if status == "not_found":
            detail = f"No Instagram account found for @{handle}. Check the handle and try again."
        elif status == "private":
            detail = f"@{handle} is a private account. Only public profiles can be scraped."
        elif status == "rate_limited":
            detail = "Instagram has temporarily rate-limited this request. Please wait a few minutes and try again."
        elif status == "empty":
            detail = f"No posts found on @{handle}."
        elif status == "no_api_key":
            detail = "APIFY_API_KEY is not set. Please add it to your environment variables."
        elif status.startswith("error:"):
            detail = f"Instagram scraping failed: {status[6:]}"
        else:
            detail = "Something went wrong while accessing Instagram. Please try again."
        raise HTTPException(status_code=400, detail=detail)

    # Create Drive folder + CSV (non-fatal if it fails)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    brand_safe = sanitize(req.brand)
    folder_url = None

    try:
        folder_id, folder_url, _ = create_brand_folder(brand_safe, timestamp)
        csv_rows = []
        for prod in products:
            row = {
                "product_name": prod.get("product_name", ""),
                "price": prod.get("price", ""),
                "product_description": prod.get("product_description", ""),
                "brand": req.brand,
                "post_url": prod.get("post_url", ""),
            }
            for i, a in enumerate(prod.get("assets", [])[:4]):
                row[f"asset_{i+1}_url"] = a["url"]
                row[f"asset_{i+1}_type"] = a["type"]
            csv_rows.append(row)

        csv_headers = [
            "product_name", "price", "product_description", "brand", "post_url",
            "asset_1_url", "asset_1_type", "asset_2_url", "asset_2_type",
            "asset_3_url", "asset_3_type", "asset_4_url", "asset_4_type",
        ]
        upload_csv_to_drive(
            csv_rows, csv_headers,
            f"{brand_safe}_{timestamp}_instagram.csv",
            folder_id,
        )
    except Exception as exc:
        print(f"[Drive] Instagram CSV upload error: {exc}")

    return {
        "products": products,
        "count": len(products),
        "platform": "instagram",
        "handle": handle,
        "drive_folder_url": folder_url,
    }


# ---------------------------------------------------------------------------
# POST /api/read-sheet  (Vendor Uploads tab)
# ---------------------------------------------------------------------------

@app.get("/api/read-sheet")
async def read_sheet_endpoint():
    """
    Read the configured vendor Google Sheet and return products.
    Sheet URL comes from the VENDOR_SHEET_URL env var — users don't enter it.
    Brand is read from each row's 'brand' column.
    Expected columns (case-insensitive):
      product_name, brand, description, price, level_1…level_5,
      asset_1_url…asset_N_url, asset_1_type…asset_N_type (type optional)
    """
    rows, error = await asyncio.get_event_loop().run_in_executor(
        _executor, read_sheet_data, VENDOR_SHEET_URL
    )

    if error:
        raise HTTPException(status_code=400, detail=error)
    if not rows:
        raise HTTPException(status_code=404, detail="No data rows found in the sheet.")

    products = []
    for row in rows:
        assets = []
        for i in range(1, 20):
            url = (
                row.get(f"asset_{i}_url") or
                row.get(f"asset{i}_url") or
                row.get(f"image_{i}_url") or
                row.get(f"image{i}_url") or
                ""
            ).strip()
            if not url:
                break
            atype = (row.get(f"asset_{i}_type") or row.get(f"asset{i}_type") or "image").strip().lower()
            # Convert Google Drive share links to directly fetchable URLs
            drive_m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
            if drive_m:
                file_id = drive_m.group(1)
                if atype == "video":
                    url = f"https://drive.google.com/uc?id={file_id}&export=download"
                else:
                    url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w1200"
            if atype not in ("image", "video"):
                atype = "image"
            assets.append({"url": url, "type": atype})

        if not assets:
            continue

        products.append({
            "product_name": row.get("product_name") or row.get("name") or "",
            "brand":        row.get("brand") or "",
            "product_description": row.get("product_description") or row.get("description") or row.get("desc") or "",
            "price":        row.get("price") or "",
            "level_1":      row.get("level_1") or row.get("level1") or "",
            "level_2":      row.get("level_2") or row.get("level2") or "",
            "level_3":      row.get("level_3") or row.get("level3") or "",
            "level_4":      row.get("level_4") or row.get("level4") or "",
            "level_5":      row.get("level_5") or row.get("level5") or "",
            "assets":       assets,
        })

    if not products:
        raise HTTPException(
            status_code=404,
            detail="No rows with asset URLs found. Check column names: asset_1_url, asset_2_url, etc."
        )

    return {"products": products, "count": len(products), "platform": "sheet"}


# ---------------------------------------------------------------------------
# POST /api/push-to-storage
# ---------------------------------------------------------------------------

@app.post("/api/push-to-storage")
async def push_to_storage(req: PushRequest):
    """
    Part 3 — Upload each asset to ImageKit with product metadata.
    Part 4 — After all uploads, export the full ImageKit library to a
              new Google Sheet in the Drive parent folder.
    """

    brand_safe = sanitize(req.brand)
    uploaded: list[dict] = []
    errors: list[str] = []

    # --- Part 3: ImageKit uploads ---
    for product in req.products:
        # Use per-product brand (vendor sheet) if set, otherwise fall back to request brand
        effective_brand = product.brand or req.brand

        metadata = {
            "product_name": product.product_name,
            "product_description": product.product_description,
            "price": product.price,
            "brand": effective_brand,
            "level_1": product.level_1 or "",
            "level_2": product.level_2 or "",
            "level_3": product.level_3 or "",
            "level_4": product.level_4 or "",
            "level_5": product.level_5 or "",
        }

        for i, asset in enumerate(product.assets):
            url = asset.url
            # Robustly extract extension from URL path only (ignore query string)
            url_path = urlparse(url).path          # e.g. "/files/image.jpg" or "/thumbnail"
            _, ext = os.path.splitext(url_path)    # e.g. ".jpg" or ""
            # Validate: must be a real image/video extension
            if ext.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif",
                                   ".avif", ".mp4", ".mov", ".webm"}:
                ext = ".jpg" if asset.type != "video" else ".mp4"
            filename = sanitize(f"{brand_safe}_{product.product_name}_{i+1}") + ext

            folder_safe = sanitize(effective_brand) or brand_safe
            result = upload_to_imagekit(url, filename, folder_safe, metadata)

            if result:
                uploaded.append(
                    {
                        "filename":     filename,
                        "imagekit_url": result.get("url", ""),
                        "file_id":      result.get("fileId", ""),
                        "product_name": product.product_name,
                        "product_description": product.product_description,
                        "price":        product.price,
                        "brand":        effective_brand,
                        "level_1":      product.level_1 or "",
                        "level_2":      product.level_2 or "",
                        "level_3":      product.level_3 or "",
                        "level_4":      product.level_4 or "",
                        "level_5":      product.level_5 or "",
                    }
                )
            else:
                errors.append(f"Failed: {filename} (check Render logs for detail)")

    # --- Part 4: Append newly uploaded assets to the persistent ImageKit sheet ---
    sheets_url = None
    try:
        if uploaded:
            sheet_rows = [
                {
                    "product_name": u["product_name"],
                    "product_description": u["product_description"],
                    "price":        u["price"],
                    "brand":        u["brand"],
                    "level_1":      u["level_1"],
                    "level_2":      u["level_2"],
                    "level_3":      u["level_3"],
                    "level_4":      u["level_4"],
                    "level_5":      u["level_5"],
                    "imagekit_url": u["imagekit_url"],
                    "file_name":    u["filename"],
                    "file_id":      u["file_id"],
                }
                for u in uploaded
            ]
            sheets_url = append_to_imagekit_sheet(sheet_rows, PARENT_FOLDER_ID)
    except Exception as exc:
        print(f"[Sheets] Append error: {exc}")

    return {
        "uploaded": uploaded,
        "upload_count": len(uploaded),
        "errors": errors,
        "sheets_url": sheets_url,
    }


# ---------------------------------------------------------------------------
# POST /api/push-to-instagram
# ---------------------------------------------------------------------------

IG_ACCOUNT_ID   = os.environ.get("INSTAGRAM_ACCOUNT_ID", "")
IG_ACCESS_TOKEN = os.environ.get("INSTAGRAM_ACCESS_TOKEN", "")
IG_API_BASE     = "https://graph.facebook.com/v19.0"


def _ig_post(path: str, params: dict) -> dict:
    """Make a POST to the Instagram Graph API and return the JSON response."""
    import requests as _requests
    params["access_token"] = IG_ACCESS_TOKEN
    resp = _requests.post(f"{IG_API_BASE}{path}", params=params, timeout=30)
    return resp.json()


@app.post("/api/push-to-instagram")
async def push_to_instagram(req: InstagramPushRequest):
    """
    Post each product as a separate Instagram post to the pre-configured
    Business account.  Multiple image assets → carousel; single image → photo post.
    Videos are skipped (Graph API carousel items must be images).
    """
    if not IG_ACCOUNT_ID or not IG_ACCESS_TOKEN:
        raise HTTPException(
            status_code=400,
            detail="Instagram credentials not configured. Set INSTAGRAM_ACCOUNT_ID and INSTAGRAM_ACCESS_TOKEN in environment.",
        )

    posted: list[dict] = []
    errors: list[str] = []

    loop = asyncio.get_event_loop()

    for product in req.products:
        # Build caption
        parts = []
        if product.product_name:
            parts.append(product.product_name)
        if product.product_description:
            parts.append(product.product_description)
        if product.price:
            parts.append(f"💰 {product.price}")
        caption = "\n\n".join(parts)

        # Only image assets; cap at 10 (Instagram carousel limit)
        image_assets = [a for a in product.assets if a.type != "video"][:10]

        if not image_assets:
            errors.append(f"{product.product_name or 'Unknown'}: no image assets to post")
            continue

        try:
            if len(image_assets) == 1:
                # Single image post
                data = await loop.run_in_executor(
                    _executor,
                    lambda: _ig_post(
                        f"/{IG_ACCOUNT_ID}/media",
                        {"image_url": image_assets[0].url, "caption": caption},
                    ),
                )
                if "error" in data:
                    raise ValueError(data["error"].get("message", str(data["error"])))
                container_id = data["id"]

                pub = await loop.run_in_executor(
                    _executor,
                    lambda: _ig_post(
                        f"/{IG_ACCOUNT_ID}/media_publish",
                        {"creation_id": container_id},
                    ),
                )
                if "error" in pub:
                    raise ValueError(pub["error"].get("message", str(pub["error"])))
                posted.append({"product_name": product.product_name, "post_id": pub.get("id", "")})

            else:
                # Carousel post — create item containers first
                item_ids: list[str] = []
                for asset in image_assets:
                    item_data = await loop.run_in_executor(
                        _executor,
                        lambda a=asset: _ig_post(
                            f"/{IG_ACCOUNT_ID}/media",
                            {"image_url": a.url, "is_carousel_item": "true"},
                        ),
                    )
                    if "error" in item_data:
                        raise ValueError(item_data["error"].get("message", str(item_data["error"])))
                    item_ids.append(item_data["id"])

                # Create carousel container
                carousel_data = await loop.run_in_executor(
                    _executor,
                    lambda: _ig_post(
                        f"/{IG_ACCOUNT_ID}/media",
                        {
                            "media_type": "CAROUSEL",
                            "children": ",".join(item_ids),
                            "caption": caption,
                        },
                    ),
                )
                if "error" in carousel_data:
                    raise ValueError(carousel_data["error"].get("message", str(carousel_data["error"])))
                carousel_id = carousel_data["id"]

                # Publish
                pub = await loop.run_in_executor(
                    _executor,
                    lambda: _ig_post(
                        f"/{IG_ACCOUNT_ID}/media_publish",
                        {"creation_id": carousel_id},
                    ),
                )
                if "error" in pub:
                    raise ValueError(pub["error"].get("message", str(pub["error"])))
                posted.append({"product_name": product.product_name, "post_id": pub.get("id", "")})

        except Exception as exc:
            errors.append(f"{product.product_name or 'Unknown'}: {exc}")

    return {
        "posted": posted,
        "post_count": len(posted),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Serve static frontend (must be last)
# ---------------------------------------------------------------------------

_frontend = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend):
    app.mount("/", StaticFiles(directory=_frontend, html=True), name="static")
