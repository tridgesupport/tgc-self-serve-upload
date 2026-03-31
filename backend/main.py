import asyncio
import mimetypes
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import requests
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
    create_brand_folder,
    create_sheet_in_drive,
    upload_csv_to_drive,
    upload_media_bytes,
    PARENT_FOLDER_ID,
)
from drive_scraper import scrape_drive, extract_drive_id
from imagekit_client import fetch_all_imagekit_files, upload_to_imagekit
from instagram_scraper import scrape_instagram
from scraper import detect_and_scrape

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )
}

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
    description: str
    assets: list[AssetIn]
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize(name: str) -> str:
    return re.sub(r"[^\w\-]", "_", name).strip("_")


def download_bytes(url: str) -> tuple[bytes, str]:
    resp = requests.get(url, headers=HEADERS, stream=True, timeout=30)
    resp.raise_for_status()
    mime = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
    return resp.content, mime


def ext_from_mime(mime: str) -> str:
    ext = mimetypes.guess_extension(mime) or ".jpg"
    return ".jpg" if ext == ".jpe" else ext


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
        drive_asset_urls: list[str] = []

        for i, asset in enumerate(prod["assets"]):
            try:
                data, mime = download_bytes(asset["url"])
                ext = ext_from_mime(mime)
                filename = sanitize(f"{brand_safe}_{prod['product_name']}_{i+1}") + ext

                if drive_ok and folder_id:
                    drive_url = upload_media_bytes(data, filename, folder_id, mime)
                    drive_asset_urls.append(drive_url)
                else:
                    drive_asset_urls.append(asset["url"])

            except Exception as exc:
                print(f"[Drive] Media upload error: {exc}")
                drive_asset_urls.append(asset["url"])

        # Build CSV row
        row: dict = {
            "product_name": prod["product_name"],
            "price": prod["price"],
            "description": prod["description"],
            "brand": req.brand,
        }
        for i, url in enumerate(drive_asset_urls[:4]):
            row[f"asset_{i+1}_url"] = url
            row[f"asset_{i+1}_type"] = (
                prod["assets"][i]["type"] if i < len(prod["assets"]) else ""
            )
        csv_rows.append(row)

    # Upload CSV to Drive
    if drive_ok and folder_id and csv_rows:
        csv_headers = [
            "product_name", "price", "description", "brand",
            "asset_1_url", "asset_1_type",
            "asset_2_url", "asset_2_type",
            "asset_3_url", "asset_3_type",
            "asset_4_url", "asset_4_type",
        ]
        try:
            upload_csv_to_drive(
                csv_rows,
                csv_headers,
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
        products, status, folder_url = await asyncio.get_event_loop().run_in_executor(
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

    error_messages = {
        "invalid_url": "That doesn't look like a valid Google Drive URL.",
        "not_public": (
            "This Google Drive folder or file is not publicly accessible. "
            "Please set sharing to 'Anyone with the link can view' and try again."
        ),
        "empty": "No image or video files were found at that Drive URL.",
        "error": "Something went wrong while accessing that Drive folder. Please try again.",
    }

    if status != "ok":
        raise HTTPException(status_code=400, detail=error_messages.get(status, "Unknown error."))

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
        products, status = await asyncio.get_event_loop().run_in_executor(
            _executor,
            scrape_instagram,
            handle,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail="Instagram scraping timed out. Please try again.",
        )

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
                "description": prod.get("description", ""),
                "brand": req.brand,
                "post_url": prod.get("post_url", ""),
            }
            for i, a in enumerate(prod.get("assets", [])[:4]):
                row[f"asset_{i+1}_url"] = a["url"]
                row[f"asset_{i+1}_type"] = a["type"]
            csv_rows.append(row)

        csv_headers = [
            "product_name", "price", "description", "brand", "post_url",
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
        metadata = {
            "product_name": product.product_name,
            "description": product.description,
            "price": product.price,
            "brand": req.brand,
            "level_1": product.level_1 or "",
            "level_2": product.level_2 or "",
            "level_3": product.level_3 or "",
            "level_4": product.level_4 or "",
            "level_5": product.level_5 or "",
        }

        for i, asset in enumerate(product.assets):
            url = asset.url
            raw_ext = url.split(".")[-1].split("?")[0][:5]
            ext = f".{raw_ext}" if raw_ext else ".jpg"
            filename = sanitize(f"{brand_safe}_{product.product_name}_{i+1}") + ext

            result = upload_to_imagekit(url, filename, brand_safe, metadata)

            if result:
                uploaded.append(
                    {
                        "filename": filename,
                        "imagekit_url": result.get("url", ""),
                        "file_id": result.get("fileId", ""),
                        "product_name": product.product_name,
                    }
                )
            else:
                errors.append(f"Failed to upload: {filename}")

    # --- Part 4: Export ImageKit → Google Sheet ---
    sheets_url = None
    try:
        ik_files = fetch_all_imagekit_files()
        rows = []
        for f in ik_files:
            meta = f.get("customMetadata") or {}
            rows.append(
                {
                    "product_name": meta.get("product_name", ""),
                    "description": meta.get("description", ""),
                    "price": meta.get("price", ""),
                    "brand": meta.get("brand", ""),
                    "level_1": meta.get("level_1", ""),
                    "level_2": meta.get("level_2", ""),
                    "level_3": meta.get("level_3", ""),
                    "level_4": meta.get("level_4", ""),
                    "level_5": meta.get("level_5", ""),
                    "imagekit_url": f.get("url", ""),
                    "file_name": f.get("name", ""),
                    "file_id": f.get("fileId", ""),
                }
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sheets_url = create_sheet_in_drive(
            rows,
            f"ImageKit_Export_{timestamp}",
            PARENT_FOLDER_ID,
        )
    except Exception as exc:
        print(f"[Sheets] Export error: {exc}")

    return {
        "uploaded": uploaded,
        "upload_count": len(uploaded),
        "errors": errors,
        "sheets_url": sheets_url,
    }


# ---------------------------------------------------------------------------
# Serve static frontend (must be last)
# ---------------------------------------------------------------------------

_frontend = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend):
    app.mount("/", StaticFiles(directory=_frontend, html=True), name="static")
