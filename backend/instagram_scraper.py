"""
Instagram scraper using the Apify Instagram Scraper actor.
Requires APIFY_API_KEY environment variable.
Returns the last 20 posts for a handle, sorted by date descending.
"""

import os
from datetime import datetime, timezone

MAX_POSTS = 20
# Fetch a few extra so sorting+slicing works even if Apify returns slightly fewer
FETCH_LIMIT = 30


def _parse_timestamp(ts: str | None) -> datetime:
    """Parse ISO-8601 timestamp from Apify; returns epoch if missing."""
    if not ts:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        # Handle both 'Z' suffix and '+00:00'
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _extract_assets(item: dict) -> list[dict]:
    """
    Extract all image/video assets from an Apify Instagram item.
    Handles single images, videos/reels, and carousels (Sidecar).
    """
    assets: list[dict] = []
    post_type = item.get("type", "")

    if post_type == "Sidecar":
        # Carousel — iterate child posts first
        child_posts = item.get("childPosts") or []
        sidecar_images = item.get("images") or []

        if child_posts:
            for child in child_posts:
                video_url = child.get("videoUrl")
                display_url = child.get("displayUrl")
                if video_url:
                    assets.append({"url": video_url, "type": "video"})
                elif display_url:
                    assets.append({"url": display_url, "type": "image"})
        elif sidecar_images:
            # Some actor versions return a flat list of image URLs
            for img_url in sidecar_images:
                if img_url:
                    assets.append({"url": img_url, "type": "image"})
        else:
            # Fallback to main display URL
            display_url = item.get("displayUrl")
            if display_url:
                assets.append({"url": display_url, "type": "image"})

    elif post_type == "Video" or item.get("videoUrl"):
        video_url = item.get("videoUrl")
        display_url = item.get("displayUrl")
        if video_url:
            assets.append({"url": video_url, "type": "video"})
        if display_url:
            assets.append({"url": display_url, "type": "image"})

    else:
        # Single image
        display_url = item.get("displayUrl")
        if display_url:
            assets.append({"url": display_url, "type": "image"})

    return assets


def scrape_instagram(handle: str, max_posts: int = MAX_POSTS) -> tuple[list[dict], str]:
    """
    Scrape the most recent posts from a public Instagram profile via Apify.

    Returns (products, status) where status is one of:
      'ok' | 'not_found' | 'private' | 'rate_limited' | 'empty' | 'error' | 'no_api_key'

    Each product:
      product_name  — blank (user fills in)
      price         — blank
      description   — post caption
      assets        — list of {url, type}
      post_url      — permalink to the IG post
    """
    handle = handle.lstrip("@").strip()
    if not handle:
        return [], "not_found"

    api_key = os.environ.get("APIFY_API_KEY", "").strip()
    if not api_key:
        print("[Instagram/Apify] APIFY_API_KEY not set")
        return [], "no_api_key"

    try:
        from apify_client import ApifyClient
        client = ApifyClient(api_key)

        run_input = {
            "directUrls": [f"https://www.instagram.com/{handle}/"],
            "resultsType": "posts",
            "resultsLimit": FETCH_LIMIT,
        }

        run = client.actor("apify/instagram-scraper").call(run_input=run_input)

        if not run or not run.get("defaultDatasetId"):
            return [], "error"

        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

    except ImportError:
        print("[Instagram/Apify] apify-client not installed")
        return [], "error:apify-client not installed"
    except Exception as exc:
        msg = str(exc).lower()
        print(f"[Instagram/Apify] Error for @{handle}: {exc}")
        if "not found" in msg or "doesn't exist" in msg or "no user" in msg:
            return [], "not_found"
        if "private" in msg or "login" in msg:
            return [], "private"
        if any(k in msg for k in ("rate", "429", "too many", "blocked")):
            return [], "rate_limited"
        if "unauthorized" in msg or "invalid token" in msg or "api key" in msg:
            return [], "no_api_key"
        return [], f"error:{exc}"

    if not items:
        return [], "empty"

    # Sort by timestamp descending (most recent first), take top max_posts
    items.sort(key=lambda x: _parse_timestamp(x.get("timestamp")), reverse=True)
    items = items[:max_posts]

    products: list[dict] = []
    for item in items:
        assets = _extract_assets(item)
        if not assets:
            continue

        post_url = item.get("url") or f"https://www.instagram.com/{handle}/"
        caption  = (item.get("caption") or "").replace("\n", " ").strip()

        products.append({
            "product_name": "",
            "price": "",
            "product_description": caption,
            "assets": assets,
            "post_url": post_url,
        })

    return (products, "ok") if products else ([], "empty")
