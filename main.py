#!/usr/bin/env python3
import sys, json, logging, os, time, shutil
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import db as database
from threads_scraper import ThreadsScraper
from llm_analyzer import LLMAnalyzer

os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE_DIR, "data", "kol_monitor.log"), encoding="utf-8"),
    ]
)
logger = logging.getLogger("main")

def load_config():
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    if os.environ.get("OPENAI_API_KEY"):
        cfg["openai_api_key"] = os.environ["OPENAI_API_KEY"]
    return cfg

def _db_path(cfg):
    raw = cfg.get("database_path", "data/kol_monitor.db")
    return raw if os.path.isabs(raw) else os.path.join(BASE_DIR, raw)

def run_once(cfg):
    start = time.time()
    db_path  = _db_path(cfg)
    data_dir = os.path.join(BASE_DIR, "data")
    snap_dir = os.path.join(BASE_DIR, "data", "snapshots")
    latest   = os.path.join(BASE_DIR, "data", "latest.json")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(snap_dir, exist_ok=True)
    database.init_db(db_path)

    api_key  = cfg.get("openai_api_key", "")
    scraper  = ThreadsScraper(delay_seconds=cfg.get("request_delay_seconds", 4))
    analyzer = LLMAnalyzer(api_key=api_key, model=cfg.get("openai_model","gpt-4o-mini")) \
               if api_key and not api_key.startswith("sk-YOUR") else None
    if not analyzer:
        logger.warning("No OpenAI key — LLM skipped.")

    accounts_ok = accounts_fail = posts_new = analyses_done = 0
    errors = []

    for handle in cfg["accounts"]:
        logger.info(f"── @{handle}")
        result = scraper.scrape_account(handle, max_posts=cfg.get("max_posts_per_account", 50))
        stub = {"handle":handle,"display_name":None,"bio":None,"follower_count":None,
                "following_count":None,"post_count":None,"verified":False,
                "avatar_url":None,"external_link":None,
                "profile_crawled_at":datetime.now(timezone.utc).isoformat()}
        if result["error"]:
            accounts_fail += 1
            errors.append(f"@{handle}: {result['error']}")
            database.upsert_account(db_path, stub)
            continue
        accounts_ok += 1
        database.upsert_account(db_path, result["profile"])
        for post in result["posts"]:
            if database.upsert_post(db_path, post): posts_new += 1
            database.record_metrics_history(db_path, post["post_id"], post)
        logger.info(f"@{handle}: {len(result['posts'])} posts")

    database.compute_engagement_rates(db_path)

    if analyzer:
        for post in database.get_viral_posts_needing_analysis(db_path, limit=cfg.get("max_llm_analyses_per_run",50)):
            a = analyzer.analyze_viral_post(post)
            if a:
                database.save_llm_analysis(db_path, post["post_id"], a)
                analyses_done += 1
            time.sleep(1)
        for acc in database.get_accounts_without_specialty(db_path):
            conn = database.get_conn(db_path)
            recent = [dict(r) for r in conn.execute(
                "SELECT content_text FROM posts WHERE handle=? ORDER BY post_timestamp DESC LIMIT 10",
                (acc["handle"],)).fetchall()]
            conn.close()
            spec = analyzer.infer_specialty(acc["handle"], acc.get("display_name",""), acc.get("bio",""), recent)
            if spec: database.upsert_specialty(db_path, acc["handle"], spec)
            time.sleep(1)

    data  = database.export_full_data(db_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for p in [os.path.join(snap_dir, f"{today}.json"), latest]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    for src in [os.path.join(BASE_DIR, "index.html")]:
        dst = os.path.join(data_dir, "index.html")
        if os.path.exists(src) and not os.path.exists(dst):
            shutil.copy2(src, dst)

    database
