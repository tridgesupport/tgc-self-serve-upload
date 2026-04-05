import asyncio
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

SCRAPE_TIMEOUT_SECS = 300   # 5 minutes overall
SCRAPE_MAX_RETRIES  = 2
_executor = ThreadPoolExecutor(max_workers=4)

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
from scraper import detect_and_scrape

app = FastAPI(title="TGC Self-Serve Upload")

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
# POST /api/scrape
# ---------------------------------------------------------------------------

@app.post("/api/scrape")
async def scrape_endpoint(req: ScrapeRequest):
    """
    1. Scrape products from the given URL.
    2. Download all media and upload to a new Google Drive folder.
    3. Save a CSV to the same Drive folder.
    4. Return the product list + Drive folder URL.
    """

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
            raise HTTPException(
                status_code=408,
                detail="This website couldn't be scraped — it timed out after 5 minutes. "
                       "The site may not expose a public product API.",
            )
        except Exception as exc:
            print(f"[Scraper] Attempt {attempt + 1} failed: {exc}")
            if attempt < SCRAPE_MAX_RETRIES - 1:
                await asyncio.sleep(2)

    if not products:
        raise HTTPException(
            status_code=404,
            detail=f"This website couldn't be scraped after {SCRAPE_MAX_RETRIES} attempts. "
                   "It may not be a Shopify or WordPress store, or the product API may be private.",
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    brand_safe = sanitize(req.brand)

    # Try to create Drive folder — non-fatal if it fails
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
        # Use original source URLs in the CSV — service accounts cannot upload
        # file content to personal Drive folders (no storage quota).
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

    # Upload CSV to Drive (works when user OAuth credentials are configured)
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

    return {
        "products": products,
        "platform": platform,
        "count": len(products),
        "drive_folder_url": folder_url,
    }


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

    error_messages = {
        "not_found":    f"No Instagram account found for @{handle}. Check the handle and try again.",
        "private":      f"@{handle} is a private account. Only public profiles can be scraped.",
        "rate_limited": "Instagram has temporarily rate-limited this request. Please wait a few minutes and try again.",
        "empty":        f"No posts found on @{handle}.",
        "no_api_key":   "APIFY_API_KEY is not set. Please add it to your environment variables.",
        "error":        "Something went wrong while accessing Instagram. Please try again.",
    }

    if status != "ok":
        raise HTTPException(status_code=400, detail=error_messages.get(status, "Unknown error."))

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
