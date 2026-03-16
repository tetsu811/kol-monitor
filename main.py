#!/usr/bin/env python3
"""
Medical KOL Monitor — Main Runner
Usage:
  python main.py run          # Run once immediately
  python main.py schedule     # Run daily at configured hour (blocking)
  python main.py export       # Export latest.json without scraping
  python main.py init         # Initialize database only
"""

import sys
import json
import logging
import os
import time
import shutil
from datetime import datetime, timezone

# ── Bootstrap path ─────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from database.db import (
    init_db, upsert_account, upsert_post, upsert_specialty,
    record_metrics_history, compute_engagement_rates,
    get_viral_posts_needing_analysis, save_llm_analysis,
    get_accounts_without_specialty, write_crawl_log, export_full_data,
)
from scraper.threads_scraper import ThreadsScraper
from analyzer.llm_analyzer import LLMAnalyzer

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE_DIR, "data", "kol_monitor.log"), encoding="utf-8"),
    ]
)
logger = logging.getLogger("main")


def load_config() -> dict:
    path = os.path.join(BASE_DIR, "config.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def resolve_path(cfg: dict, key: str) -> str:
    return os.path.join(BASE_DIR, cfg[key])


# ── Core run logic ─────────────────────────────────────────

def run_once(cfg: dict):
    start = time.time()
    db_path    = resolve_path(cfg, "database_path")
    snap_dir   = resolve_path(cfg, "snapshots_dir")
    latest_path= resolve_path(cfg, "latest_json_path")

    os.makedirs(snap_dir,              exist_ok=True)
    os.makedirs(os.path.dirname(latest_path), exist_ok=True)

    init_db(db_path)

    accounts        = cfg["accounts"]
    delay           = cfg.get("request_delay_seconds", 4)
    max_posts       = cfg.get("max_posts_per_account", 50)
    max_llm         = cfg.get("max_llm_analyses_per_run", 50)
    api_key         = cfg.get("openai_api_key", "")

    scraper  = ThreadsScraper(delay_seconds=delay)
    analyzer = LLMAnalyzer(api_key=api_key, model=cfg.get("openai_model", "gpt-4o-mini")) \
               if api_key and not api_key.startswith("sk-YOUR") else None

    if not analyzer:
        logger.warning("No valid OpenAI API key — LLM analysis skipped.")

    accounts_ok   = 0
    accounts_fail = 0
    posts_new     = 0
    errors        = []

    # ── Step 1 & 2: Scrape all accounts ───────────────────
    for handle in accounts:
        logger.info(f"──── Scraping @{handle} ────")
        result = scraper.scrape_account(handle, max_posts=max_posts)

        if result["error"]:
            logger.error(f"@{handle} failed: {result['error']}")
            accounts_fail += 1
            errors.append(f"@{handle}: {result['error']}")
            upsert_account(db_path, {
                "handle": handle, "display_name": None, "bio": None,
                "follower_count": None, "following_count": None, "post_count": None,
                "verified": False, "avatar_url": None, "external_link": None,
                "profile_crawled_at": datetime.now(timezone.utc).isoformat(),
            })
            continue

        accounts_ok += 1
        upsert_account(db_path, result["profile"])

        for post in result["posts"]:
            is_new = upsert_post(db_path, post)
            if is_new:
                posts_new += 1
            record_metrics_history(db_path, post["post_id"], post)

        logger.info(f"@{handle}: {len(result['posts'])} posts scraped, {posts_new} new so far")

    # ── Step 3: Compute engagement rates + flag viral ──────
    logger.info("Computing engagement rates …")
    compute_engagement_rates(db_path)

    # ── Step 4: LLM viral analysis ─────────────────────────
    analyses_done = 0
    if analyzer:
        viral_posts = get_viral_posts_needing_analysis(db_path, limit=max_llm)
        logger.info(f"LLM: {len(viral_posts)} viral posts to analyze")
        for post in viral_posts:
            analysis = analyzer.analyze_viral_post(post)
            if analysis:
                save_llm_analysis(db_path, post["post_id"], analysis)
                analyses_done += 1
                logger.info(f"  Analyzed post {post['post_id']} → viral_score={analysis.get('viral_score')}")
            time.sleep(1)  # rate limit safety

    # ── Step 5: LLM specialty inference ───────────────────
    if analyzer:
        no_spec = get_accounts_without_specialty(db_path)
        logger.info(f"LLM: {len(no_spec)} accounts need specialty inference")
        for acc in no_spec:
            # Get recent posts for this account
            from database.db import get_conn
            conn = get_conn(db_path)
            recent = conn.execute("""
                SELECT content_text FROM posts
                WHERE handle=? ORDER BY post_timestamp DESC LIMIT 10
            """, (acc["handle"],)).fetchall()
            conn.close()
            recent_list = [dict(r) for r in recent]

            specialty = analyzer.infer_specialty(
                acc["handle"],
                acc.get("display_name", ""),
                acc.get("bio", ""),
                recent_list,
            )
            if specialty:
                upsert_specialty(db_path, acc["handle"], specialty)
                logger.info(f"  @{acc['handle']} → {specialty.get('primary_specialty')} "
                            f"(conf={specialty.get('confidence_score', 0):.2f})")
            time.sleep(1)

    # ── Step 6 & 7: Export JSON ────────────────────────────
    data = export_full_data(db_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_path = os.path.join(snap_dir, f"{today}.json")

    with open(snap_path,    "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    with open(latest_path,  "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Exported → {snap_path}")
    logger.info(f"Exported → {latest_path}")

    # Copy HTML dashboard next to data if not already there
    html_src = os.path.join(BASE_DIR, "dashboard", "index.html")
    html_dst = os.path.join(BASE_DIR, "data", "index.html")
    if os.path.exists(html_src) and not os.path.exists(html_dst):
        shutil.copy2(html_src, html_dst)

    # ── Step 8: Write crawl log ────────────────────────────
    elapsed = time.time() - start
    write_crawl_log(db_path, {
        "accounts_ok":   accounts_ok,
        "accounts_fail": accounts_fail,
        "posts_new":     posts_new,
        "analyses_done": analyses_done,
        "errors":        json.dumps(errors, ensure_ascii=False),
        "duration_secs": round(elapsed, 1),
    })

    logger.info(f"Done in {elapsed:.1f}s — "
                f"{accounts_ok} OK, {accounts_fail} fail, "
                f"{posts_new} new posts, {analyses_done} analyses")


def run_export_only(cfg: dict):
    db_path     = resolve_path(cfg, "database_path")
    latest_path = resolve_path(cfg, "latest_json_path")
    snap_dir    = resolve_path(cfg, "snapshots_dir")
    init_db(db_path)
    data = export_full_data(db_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(os.path.dirname(latest_path), exist_ok=True)
    with open(os.path.join(snap_dir, f"{today}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Exported {len(data['posts'])} posts, {len(data['accounts'])} accounts")


def run_scheduler(cfg: dict):
    """Blocking daily scheduler using time.sleep."""
    import schedule as sched_lib
    hour   = cfg.get("schedule_hour", 6)
    minute = cfg.get("schedule_minute", 0)
    time_str = f"{hour:02d}:{minute:02d}"

    try:
        import schedule
        schedule.every().day.at(time_str).do(run_once, cfg=cfg)
        logger.info(f"Scheduler started — daily at {time_str} (Asia/Taipei)")
        while True:
            schedule.run_pending()
            time.sleep(60)
    except ImportError:
        # Fallback: manual sleep loop
        logger.warning("'schedule' library not available; using manual loop.")
        while True:
            now = datetime.now()
            if now.hour == hour and now.minute == minute:
                run_once(cfg)
                time.sleep(61)  # avoid double-run within same minute
            time.sleep(30)


# ── Entry point ────────────────────────────────────────────

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    cfg = load_config()

    # Ensure log directory exists
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

    if cmd == "run":
        run_once(cfg)
    elif cmd == "schedule":
        run_scheduler(cfg)
    elif cmd == "export":
        run_export_only(cfg)
    elif cmd == "init":
        db_path = resolve_path(cfg, "database_path")
        init_db(db_path)
        logger.info("Database initialized.")
    else:
        print(__doc__)
        sys.exit(1)
