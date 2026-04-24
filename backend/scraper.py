import re
import mimetypes
import requests
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

VIDEO_EXTS = {".mp4", ".mov", ".webm", ".avi", ".m4v"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", str(raw_html))
    return " ".join(cleaned.split())


def asset_type_from_url(url: str) -> str:
    path = urlparse(url).path.lower().split("?")[0]
    ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
    return "video" if ext in VIDEO_EXTS else "image"


def download_bytes(url: str) -> tuple[bytes, str]:
    """Download a URL and return (bytes, mime_type)."""
    resp = requests.get(url, headers=HEADERS, stream=True, timeout=30)
    resp.raise_for_status()
    content_type = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
    return resp.content, content_type


# ---------------------------------------------------------------------------
# Shopify
# ---------------------------------------------------------------------------

def scrape_shopify(root_url: str) -> list[dict]:
    all_products = []
    page = 1

    while True:
        api_url = f"{root_url.rstrip('/')}/products.json?limit=250&page={page}"
        try:
            res = requests.get(api_url, headers=HEADERS, timeout=20)
        except Exception:
            break

        if res.status_code != 200:
            break

        try:
            products = res.json().get("products", [])
        except Exception:
            break

        if not products:
            break

        for p in products:
            assets = []

            # All images (carousel = multiple images)
            for img in p.get("images", []):
                src = img.get("src", "")
                if src:
                    assets.append({"url": src, "type": asset_type_from_url(src)})

            # Videos from media field
            for media in p.get("media", []):
                if media.get("media_type") in ("video", "external_video"):
                    for source in media.get("sources", []):
                        vid_url = source.get("url", "")
                        if vid_url:
                            assets.append({"url": vid_url, "type": "video"})
                            break  # one source per media item is enough

            # Deduplicate by URL
            seen = set()
            deduped = []
            for a in assets:
                if a["url"] not in seen:
                    seen.add(a["url"])
                    deduped.append(a)

            price = "0"
            if p.get("variants"):
                price = str(p["variants"][0].get("price", "0"))

            all_products.append(
                {
                    "product_name": p.get("title", ""),
                    "price": price,
                    "product_description": clean_html(p.get("body_html", "")),
                    "assets": deduped,
                }
            )

        page += 1

    return all_products


# ---------------------------------------------------------------------------
# WordPress / WooCommerce
# ---------------------------------------------------------------------------

def scrape_wordpress(root_url: str) -> list[dict]:
    base = root_url.rstrip("/")
    endpoint_candidates = [
        f"{base}/wp-json/wc/store/v1/products",
        f"{base}/wp-json/wc/v3/products",
    ]

    for base_url in endpoint_candidates:
        all_products = []
        page = 1

        while True:
            api_url = f"{base_url}?per_page=100&page={page}"
            try:
                res = requests.get(api_url, headers=HEADERS, timeout=20)
            except Exception:
                break

            if res.status_code != 200:
                break

            try:
                products = res.json()
            except Exception:
                break

            if not products:
                break

            for p in products:
                # Name — handles both store/v1 and v3 formats
                name = p.get("name") or ""
                if not name and isinstance(p.get("title"), dict):
                    name = p["title"].get("rendered", "")

                # Price
                price = ""
                if isinstance(p.get("prices"), dict):
                    price = p["prices"].get("price") or p["prices"].get("regular_price", "0")
                elif p.get("price"):
                    price = str(p["price"])

                # Description
                desc_raw = p.get("description", "")
                if not desc_raw:
                    short = p.get("short_description", "")
                    desc_raw = short.get("rendered", "") if isinstance(short, dict) else short

                # Images
                assets = []
                for img in p.get("images", []):
                    src = img.get("src") or img.get("url", "")
                    if src:
                        assets.append({"url": src, "type": "image"})

                all_products.append(
                    {
                        "product_name": name,
                        "price": str(price),
                        "product_description": clean_html(desc_raw),
                        "assets": assets,
                    }
                )

            page += 1

        if all_products:
            return all_products

    return []


# ---------------------------------------------------------------------------
# Shopify — authenticated (Admin REST API, cursor-based pagination)
# ---------------------------------------------------------------------------

def scrape_shopify_authenticated(root_url: str, admin_token: str, api_version: str = "2025-04") -> list[dict]:
    """Use the Shopify Admin API with an access token.

    Accesses all products (including unpublished) and handles cursor-based
    pagination via the Link response header introduced in API version 2019-10.
    """
    base         = root_url.rstrip("/")
    auth_headers = {**HEADERS, "X-Shopify-Access-Token": admin_token}
    all_products = []
    url: str | None = f"{base}/admin/api/{api_version}/products.json?limit=250"

    while url:
        try:
            res = requests.get(url, headers=auth_headers, timeout=20)
        except Exception:
            break

        if res.status_code != 200:
            break

        try:
            products = res.json().get("products", [])
        except Exception:
            break

        if not products:
            break

        for p in products:
            assets = []

            for img in p.get("images", []):
                src = img.get("src", "")
                if src:
                    assets.append({"url": src, "type": asset_type_from_url(src)})

            for media in p.get("media", []):
                if media.get("media_type") in ("video", "external_video"):
                    for source in media.get("sources", []):
                        vid_url = source.get("url", "")
                        if vid_url:
                            assets.append({"url": vid_url, "type": "video"})
                            break

            seen   = set()
            deduped = []
            for a in assets:
                if a["url"] not in seen:
                    seen.add(a["url"])
                    deduped.append(a)

            price = "0"
            if p.get("variants"):
                price = str(p["variants"][0].get("price", "0"))

            all_products.append({
                "product_name":        p.get("title", ""),
                "price":               price,
                "product_description": clean_html(p.get("body_html", "")),
                "assets":              deduped,
            })

        # Advance via Link header  <url>; rel="next"
        link_header = res.headers.get("Link", "")
        next_url    = None
        for part in link_header.split(","):
            part = part.strip()
            if 'rel="next"' in part:
                m = re.search(r"<([^>]+)>", part)
                if m:
                    next_url = m.group(1)
                    break
        url = next_url

    return all_products


# ---------------------------------------------------------------------------
# Auto-detect and scrape
# ---------------------------------------------------------------------------

def detect_and_scrape(url: str) -> tuple[list[dict], str]:
    """Returns (products, platform_name)."""
    if not url.startswith("http"):
        url = f"https://{url}"

    products = scrape_shopify(url)
    if products:
        return products, "shopify"

    products = scrape_wordpress(url)
    if products:
        return products, "wordpress"

    return [], "unknown"
