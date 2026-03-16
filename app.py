"""
Flask web server for KOL Monitor.
Serves the HTML dashboard + JSON data, and runs the scraper on schedule.
"""
import os
import sys
import json
import logging
import threading
import time
from datetime import datetime

from flask import Flask, send_file, jsonify, abort

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("app")

app = Flask(__name__)

DATA_DIR    = os.path.join(BASE_DIR, "data")
LATEST_JSON = os.path.join(DATA_DIR, "latest.json")
INDEX_HTML  = os.path.join(DATA_DIR, "index.html")


# ── Env-var override for config ────────────────────────────
def load_config():
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    # Allow env var overrides (for Render / production)
    if os.environ.get("OPENAI_API_KEY"):
        cfg["openai_api_key"] = os.environ["OPENAI_API_KEY"]
    if os.environ.get("SCHEDULE_HOUR"):
        cfg["schedule_hour"] = int(os.environ["SCHEDULE_HOUR"])
    return cfg


# ── Routes ─────────────────────────────────────────────────
@app.route("/")
def index():
    if os.path.exists(INDEX_HTML):
        return send_file(INDEX_HTML)
    return "<h2>儀表板載入中，請稍後重整…</h2>", 200


@app.route("/data/latest.json")
def latest_json():
    if os.path.exists(LATEST_JSON):
        return send_file(LATEST_JSON, mimetype="application/json")
    return jsonify({"accounts": [], "posts": [], "generated_at": None}), 200


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "has_data": os.path.exists(LATEST_JSON),
        "time": datetime.utcnow().isoformat(),
    })


@app.route("/trigger", methods=["POST"])
def trigger():
    """Manually trigger a scrape run (for testing)."""
    token = os.environ.get("TRIGGER_SECRET", "")
    auth  = request.headers.get("X-Secret", "")
    if token and auth != token:
        abort(403)
    threading.Thread(target=_run_scraper, daemon=True).start()
    return jsonify({"message": "Scrape started in background"}), 202


# ── Background scraper ─────────────────────────────────────
_scraper_running = False

def _run_scraper():
    global _scraper_running
    if _scraper_running:
        logger.info("Scraper already running, skipping.")
        return
    _scraper_running = True
    try:
        from main import run_once
        cfg = load_config()
        run_once(cfg)
        # Copy dashboard HTML to data/
        import shutil
        src = os.path.join(BASE_DIR, "dashboard", "index.html")
        if os.path.exists(src):
            shutil.copy2(src, INDEX_HTML)
        logger.info("Scrape run complete.")
    except Exception as e:
        logger.error(f"Scraper error: {e}", exc_info=True)
    finally:
        _scraper_running = False


def _scheduler_loop(cfg):
    """Run scraper at configured hour daily."""
    import schedule as sched
    hour   = cfg.get("schedule_hour", 6)
    minute = cfg.get("schedule_minute", 0)
    sched.every().day.at(f"{hour:02d}:{minute:02d}").do(_run_scraper)
    logger.info(f"Scheduler: daily at {hour:02d}:{minute:02d} UTC")
    while True:
        sched.run_pending()
        time.sleep(60)


# ── Startup ────────────────────────────────────────────────
def startup():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Copy dashboard HTML on startup
    import shutil
    src = os.path.join(BASE_DIR, "dashboard", "index.html")
    if os.path.exists(src):
        shutil.copy2(src, INDEX_HTML)

    cfg = load_config()

    # Init DB
    from database.db import init_db
    init_db(os.path.join(BASE_DIR, cfg.get("database_path", "data/kol_monitor.db")))

    # Run scraper immediately on first start (in background)
    if not os.path.exists(LATEST_JSON):
        logger.info("No data found — running initial scrape in background…")
        threading.Thread(target=_run_scraper, daemon=True).start()
    else:
        logger.info("Existing data found — skipping initial scrape.")

    # Start daily scheduler in background
    threading.Thread(target=_scheduler_loop, args=(cfg,), daemon=True).start()


startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
