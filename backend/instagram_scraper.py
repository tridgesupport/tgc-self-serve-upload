"""
Instagram scraper using instaloader.
Works on public profiles without login.
Respects rate limits with cooldown pauses.
"""

import time
import instaloader

MAX_POSTS = 20
COOLDOWN_EVERY = 10   # pause after every N posts
COOLDOWN_SECS = 20    # seconds to sleep during cooldown


def scrape_instagram(handle: str, max_posts: int = MAX_POSTS) -> tuple[list[dict], str]:
    """
    Scrape the most recent posts from a public Instagram profile.

    Returns (products, status) where status is one of:
      'ok' | 'not_found' | 'private' | 'rate_limited' | 'empty' | 'error'

    Each product has:
      product_name  — blank (user fills in)
      price         — blank
      description   — post caption
      assets        — list of {url, type} covering all carousel images/videos
      post_url      — permalink to the IG post
    """
    handle = handle.lstrip("@").strip()
    if not handle:
        return [], "not_found"

    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )

    # Load profile
    try:
        profile = instaloader.Profile.from_username(L.context, handle)
    except instaloader.exceptions.ProfileNotExistsException:
        return [], "not_found"
    except instaloader.exceptions.LoginRequiredException:
        return [], "private"
    except Exception as exc:
        msg = str(exc).lower()
        if "private" in msg or "login" in msg:
            return [], "private"
        if any(k in msg for k in ("rate", "429", "blocked", "too many")):
            return [], "rate_limited"
        print(f"[Instagram] Profile load error for @{handle}: {exc}")
        return [], "error"

    if profile.is_private:
        return [], "private"

    products: list[dict] = []
    post_count = 0

    try:
        for post in profile.get_posts():
            if post_count >= max_posts:
                break

            assets = _extract_assets(post)

            products.append(
                {
                    "product_name": "",
                    "price": "",
                    "description": (post.caption or "").replace("\n", " ").strip(),
                    "assets": assets,
                    "post_url": f"https://www.instagram.com/p/{post.shortcode}/",
                }
            )

            post_count += 1

            # Cooldown every N posts to avoid rate-limiting
            if post_count % COOLDOWN_EVERY == 0 and post_count < max_posts:
                print(f"[Instagram] Cooldown after {post_count} posts…")
                time.sleep(COOLDOWN_SECS)

    except instaloader.exceptions.LoginRequiredException:
        return products, "private" if not products else "ok"
    except Exception as exc:
        msg = str(exc).lower()
        if any(k in msg for k in ("rate", "429", "blocked", "too many")):
            # Return whatever we got before being blocked
            return products, "ok" if products else "rate_limited"
        print(f"[Instagram] Scrape error for @{handle}: {exc}")
        return products, "ok" if products else "error"

    return (products, "ok") if products else ([], "empty")


def _extract_assets(post) -> list[dict]:
    """Return all image/video assets from a post (including carousel)."""
    assets = []

    try:
        if post.typename == "GraphSidecar":
            # Carousel — iterate each slide
            for node in post.get_sidecar_nodes():
                if node.is_video:
                    if node.video_url:
                        assets.append({"url": node.video_url, "type": "video"})
                else:
                    if node.display_url:
                        assets.append({"url": node.display_url, "type": "image"})
        elif post.is_video:
            if post.video_url:
                assets.append({"url": post.video_url, "type": "video"})
            # Include thumbnail as a separate image asset
            if post.url:
                assets.append({"url": post.url, "type": "image"})
        else:
            if post.url:
                assets.append({"url": post.url, "type": "image"})
    except Exception as exc:
        print(f"[Instagram] Asset extraction error: {exc}")
        # Fallback: use whatever main URL is available
        try:
            if post.url:
                assets.append({"url": post.url, "type": "image"})
        except Exception:
            pass

    return assets
