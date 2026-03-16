"""
Threads scraper using requests + HTML parsing.

Strategy:
1. Fetch profile page to extract user ID and lsd token
2. Use Threads unofficial GraphQL endpoint to fetch posts
3. Fall back to HTML parsing if API fails
"""

import re
import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
THREADS_BASE   = "https://www.threads.net"
THREADS_GQL    = "https://www.threads.net/api/graphql"
APP_ID         = "238260118697367"

HEADERS_BASE = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/123.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Sec-Fetch-Dest":  "document",
    "Sec-Fetch-Mode":  "navigate",
    "Sec-Fetch-Site":  "none",
}

API_HEADERS_EXTRA = {
    "X-IG-App-ID":       APP_ID,
    "X-ASBD-ID":         "129477",
    "Content-Type":      "application/x-www-form-urlencoded",
    "Accept":            "*/*",
    "Origin":            THREADS_BASE,
    "Referer":           THREADS_BASE + "/",
}

# GraphQL doc IDs (may change; verified Mar 2025)
DOC_ID_USER_PROFILE = "23996318473300828"
DOC_ID_USER_THREADS = "7357547304353410"


# ── Session / token helpers ────────────────────────────────

def _extract_lsd(html: str) -> Optional[str]:
    """Extract lsd token from page HTML."""
    # Try JSON blobs in script tags
    matches = re.findall(r'"LSD"\s*,\s*\[\]\s*,\s*\{\s*"token"\s*:\s*"([^"]+)"', html)
    if matches:
        return matches[0]
    # Try data-lsd attribute
    m = re.search(r'data-lsd=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)
    # Try generic pattern
    m = re.search(r'"lsd"\s*:\s*"([^"]+)"', html)
    return m.group(1) if m else None


def _extract_user_id(html: str, handle: str) -> Optional[str]:
    """Extract numeric user ID from profile page."""
    # Look for userID in JSON blobs
    patterns = [
        r'"userID"\s*:\s*"(\d+)"',
        r'"user_id"\s*:\s*"(\d+)"',
        r'"pk"\s*:\s*"(\d+)"',
        r'"id"\s*:\s*"(\d+)"',
        rf'"@{re.escape(handle)}"[^{{}}]*?"id"\s*:\s*"(\d+)"',
    ]
    for p in patterns:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _parse_profile_from_html(html: str, handle: str) -> dict:
    """Parse account profile info from page HTML."""
    soup = BeautifulSoup(html, "lxml")
    profile = {
        "handle":             handle,
        "display_name":       None,
        "bio":                None,
        "follower_count":     None,
        "following_count":    None,
        "post_count":         None,
        "verified":           False,
        "avatar_url":         None,
        "external_link":      None,
        "profile_crawled_at": datetime.now(timezone.utc).isoformat(),
    }

    # Meta tags
    og_title = soup.find("meta", property="og:title")
    if og_title:
        profile["display_name"] = og_title.get("content", "").split("(@")[0].strip()

    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        profile["bio"] = og_desc.get("content", "")

    og_img = soup.find("meta", property="og:image")
    if og_img:
        profile["avatar_url"] = og_img.get("content", "")

    # Try JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                profile["display_name"] = profile["display_name"] or data.get("name")
                profile["bio"]          = profile["bio"] or data.get("description")
                if "interactionStatistic" in data:
                    for stat in data["interactionStatistic"]:
                        itype = stat.get("interactionType", "")
                        val   = stat.get("userInteractionCount", 0)
                        if "Follow" in itype:
                            profile["follower_count"] = int(val)
        except Exception:
            pass

    # Try parsing follower count from visible text
    follower_re = re.search(r'([\d,\.]+)\s*(?:followers|追蹤者)', html, re.IGNORECASE)
    if follower_re and not profile["follower_count"]:
        raw = follower_re.group(1).replace(",", "").replace(".", "")
        try:
            profile["follower_count"] = int(raw)
        except ValueError:
            pass

    # Parse followers from JSON blobs in page scripts
    follower_matches = re.findall(r'"follower_count"\s*:\s*(\d+)', html)
    if follower_matches and not profile["follower_count"]:
        profile["follower_count"] = int(follower_matches[0])

    following_matches = re.findall(r'"following_count"\s*:\s*(\d+)', html)
    if following_matches and not profile["following_count"]:
        profile["following_count"] = int(following_matches[0])

    # Bio fallback from JSON blob
    bio_match = re.search(r'"biography"\s*:\s*"([^"]{5,})"', html)
    if bio_match and not profile["bio"]:
        profile["bio"] = bio_match.group(1)

    return profile


def _parse_posts_from_html(html: str, handle: str) -> list:
    """Extract posts from embedded JSON in Threads HTML."""
    posts = []
    now_str = datetime.now(timezone.utc).isoformat()

    # Find all JSON blobs that look like thread/post data
    # Threads embeds data in window.__additionalData or similar
    json_blobs = re.findall(r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
                            html, re.DOTALL)

    for blob in json_blobs:
        try:
            data = json.loads(blob)
            _extract_posts_from_json(data, handle, now_str, posts)
        except Exception:
            pass

    # Also scan inline script tags for __bbox data
    script_blobs = re.findall(r'__bbox\s*=\s*(\{.*?\});?\s*(?:__bbox|$)', html, re.DOTALL)
    for blob in script_blobs:
        try:
            data = json.loads(blob)
            _extract_posts_from_json(data, handle, now_str, posts)
        except Exception:
            pass

    return posts


def _extract_posts_from_json(data, handle: str, now_str: str, posts: list, depth: int = 0):
    """Recursively search JSON structure for post objects."""
    if depth > 10:
        return

    if isinstance(data, dict):
        # Check if this looks like a Threads post node
        if _is_post_node(data):
            post = _parse_post_node(data, handle, now_str)
            if post and not any(p["post_id"] == post["post_id"] for p in posts):
                posts.append(post)
        else:
            for v in data.values():
                _extract_posts_from_json(v, handle, now_str, posts, depth + 1)

    elif isinstance(data, list):
        for item in data:
            _extract_posts_from_json(item, handle, now_str, posts, depth + 1)


def _is_post_node(node: dict) -> bool:
    """Heuristic check whether a JSON node is a Threads post."""
    post_keys = {"pk", "id", "taken_at", "like_count", "text_post_app_info"}
    has = set(node.keys())
    return bool(post_keys & has) and ("taken_at" in has or "timestamp" in has)


def _parse_post_node(node: dict, handle: str, now_str: str) -> Optional[dict]:
    """Parse a Threads post node into our schema."""
    try:
        post_id = str(node.get("pk") or node.get("id", ""))
        if not post_id:
            return None

        # Timestamp
        taken_at = node.get("taken_at") or node.get("timestamp")
        if taken_at:
            try:
                ts = datetime.fromtimestamp(int(taken_at), tz=timezone.utc).isoformat()
            except Exception:
                ts = str(taken_at)
        else:
            ts = now_str

        # Content text
        caption = node.get("caption") or {}
        if isinstance(caption, dict):
            text = caption.get("text", "")
        else:
            text = str(caption) if caption else ""

        # Also check text_post_app_info
        tpa = node.get("text_post_app_info", {}) or {}
        if not text:
            text = tpa.get("text", "")

        # Media type
        media_type_code = node.get("media_type", 1)
        media_map = {1: "image", 2: "video", 8: "carousel", 19: "text"}
        media_type_str = media_map.get(media_type_code, "text")
        if not text and media_type_code == 1:
            media_type_str = "image"
        if not any([node.get("image_versions2"), node.get("video_versions")]):
            media_type_str = "text"

        # Media URLs
        media_urls = []
        imgs = node.get("image_versions2", {}) or {}
        for c in imgs.get("candidates", [])[:1]:
            media_urls.append(c.get("url", ""))
        vids = node.get("video_versions", []) or []
        if vids:
            media_urls.append(vids[0].get("url", ""))

        # Metrics
        like_count    = int(node.get("like_count", 0) or 0)
        comment_count = int(node.get("text_post_app_info", {}).get("direct_reply_count", 0) or
                            node.get("comment_count", 0) or 0)
        repost_count  = int(node.get("text_post_app_info", {}).get("repost_count", 0) or 0)
        quote_count   = int(node.get("text_post_app_info", {}).get("quote_count", 0) or 0)
        view_count    = node.get("view_count") or node.get("play_count")

        # is_reply
        is_reply = bool(tpa.get("reply_to_author") or node.get("is_reply"))

        # URL
        post_url = f"https://www.threads.net/@{handle}/post/{post_id}"

        return {
            "post_id":       post_id,
            "handle":        handle,
            "post_url":      post_url,
            "content_text":  text,
            "media_type":    media_type_str,
            "media_urls":    json.dumps(media_urls, ensure_ascii=False),
            "post_timestamp": ts,
            "like_count":    like_count,
            "comment_count": comment_count,
            "repost_count":  repost_count,
            "quote_count":   quote_count,
            "view_count":    int(view_count) if view_count else None,
            "is_reply":      1 if is_reply else 0,
            "crawled_at":    now_str,
        }
    except Exception as e:
        logger.debug(f"Post parse error: {e}")
        return None


# ── GraphQL API fetch ──────────────────────────────────────

def _gql_fetch_user_threads(session: requests.Session, user_id: str,
                             lsd: str, max_posts: int = 50) -> list:
    """Fetch posts via unofficial Threads GraphQL API."""
    posts_raw = []
    cursor    = None

    for page in range(5):  # max 5 pages
        variables = {
            "userID": user_id,
            "count":  min(max_posts, 25),
            "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": False,
            "__relay_internal__pv__BarcelonaIsThreadContextHeaderEnabledrelayprovider": False,
            "__relay_internal__pv__BarcelonaOptionalCookiesEnabledrelayprovider": True,
        }
        if cursor:
            variables["after"] = cursor

        payload = {
            "lsd":       lsd,
            "variables": json.dumps(variables),
            "doc_id":    DOC_ID_USER_THREADS,
        }

        headers = {**HEADERS_BASE, **API_HEADERS_EXTRA,
                   "X-FB-LSD": lsd, "Referer": f"{THREADS_BASE}/@tmp"}

        try:
            resp = session.post(THREADS_GQL, data=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            edges = _deep_find_edges(data)
            if not edges:
                break

            for edge in edges:
                node = edge.get("node", {}) or {}
                thread_items = node.get("thread_items", []) or []
                for item in thread_items:
                    post_node = item.get("post", {}) or {}
                    if post_node:
                        posts_raw.append(post_node)

            # Pagination
            page_info = _deep_find_page_info(data)
            if page_info and page_info.get("has_next_page"):
                cursor = page_info.get("end_cursor")
            else:
                break

            if len(posts_raw) >= max_posts:
                break

            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            logger.warning(f"GraphQL fetch error (page {page}): {e}")
            break

    return posts_raw[:max_posts]


def _deep_find_edges(data, depth=0):
    if depth > 8 or not isinstance(data, dict):
        return []
    if "edges" in data and isinstance(data["edges"], list):
        return data["edges"]
    for v in data.values():
        result = _deep_find_edges(v, depth + 1)
        if result:
            return result
    return []


def _deep_find_page_info(data, depth=0):
    if depth > 8 or not isinstance(data, dict):
        return None
    if "page_info" in data and isinstance(data["page_info"], dict):
        return data["page_info"]
    for v in data.values():
        result = _deep_find_page_info(v, depth + 1)
        if result:
            return result
    return None


# ── Main scrape function ───────────────────────────────────

class ThreadsScraper:
    def __init__(self, delay_seconds: float = 4.0):
        self.delay = delay_seconds
        self.session = requests.Session()
        self.session.headers.update(HEADERS_BASE)

    def _sleep(self):
        time.sleep(self.delay + random.uniform(0, 2))

    def scrape_account(self, handle: str, max_posts: int = 50) -> dict:
        """
        Scrape one account. Returns:
        {
          "profile": {...},
          "posts":   [...],
          "error":   None or str
        }
        """
        url = f"{THREADS_BASE}/@{handle}"
        logger.info(f"[Scraper] Fetching {url}")

        try:
            resp = self.session.get(url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            logger.error(f"[Scraper] Failed to fetch {url}: {e}")
            return {"profile": {"handle": handle}, "posts": [], "error": str(e)}

        # Parse profile from HTML
        profile = _parse_profile_from_html(html, handle)
        logger.info(f"[Scraper] @{handle}: display_name={profile.get('display_name')}, "
                    f"followers={profile.get('follower_count')}")

        # Extract lsd + user_id for API calls
        lsd     = _extract_lsd(html)
        user_id = _extract_user_id(html, handle)

        posts = []

        # Strategy 1: GraphQL API
        if lsd and user_id:
            logger.info(f"[Scraper] @{handle}: trying GraphQL (user_id={user_id})")
            raw_nodes = _gql_fetch_user_threads(self.session, user_id, lsd, max_posts)
            now_str   = datetime.now(timezone.utc).isoformat()
            for node in raw_nodes:
                post = _parse_post_node(node, handle, now_str)
                if post:
                    posts.append(post)
            logger.info(f"[Scraper] @{handle}: GraphQL returned {len(posts)} posts")

        # Strategy 2: HTML parsing fallback
        if not posts:
            logger.info(f"[Scraper] @{handle}: falling back to HTML parsing")
            posts = _parse_posts_from_html(html, handle)
            logger.info(f"[Scraper] @{handle}: HTML parsing found {len(posts)} posts")

        self._sleep()
        return {"profile": profile, "posts": posts[:max_posts], "error": None}
