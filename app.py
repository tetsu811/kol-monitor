import os, sys, json, logging, threading, time, shutil
from datetime import datetime
from flask import Flask, send_file, jsonify

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("app")
app = Flask(__name__)

DATA_DIR    = os.path.join(BASE_DIR, "data")
LATEST_JSON = os.path.join(DATA_DIR, "latest.json")
DASHBOARD   = os.path.join(DATA_DIR, "index.html")
ROOT_HTML   = os.path.join(BASE_DIR, "index.html")

def load_config():
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    if os.environ.get("OPENAI_API_KEY"):
        cfg["openai_api_key"] = os.environ["OPENAI_API_KEY"]
    return cfg

@app.route("/")
def index():
    for p in [DASHBOARD, ROOT_HTML]:
        if os.path.exists(p): return send_file(p)
    return "<h2 style='font-family:sans-serif;padding:40px'>⏳ 初次爬取中，請5分鐘後重整…</h2>"

@app.route("/data/latest.json")
def latest_json():
    for p in [LATEST_JSON, os.path.join(BASE_DIR, "latest.json")]:
        if os.path.exists(p): return send_file(p, mimetype="application/json")
    return jsonify({"accounts":[],"posts":[],"generated_at":None})

@app.route("/health")
def health():
    return jsonify({"status":"ok","has_data":os.path.exists(LATEST_JSON),"time":datetime.utcnow().isoformat()})

_running = False
def _run_scraper():
    global _running
    if _running: return
    _running = True
    try:
        import main as m
        m.run_once(load_config())
        src = ROOT_HTML
        if os.path.exists(src): shutil.copy2(src, DASHBOARD)
    except Exception as e:
        logger.error(f"Scraper error: {e}", exc_info=True)
    finally:
        _running = False

def _scheduler(cfg):
    import schedule
    h, m = cfg.get("schedule_hour",6), cfg.get("schedule_minute",0)
    schedule.every().day.at(f"{h:02d}:{m:02d}").do(_run_scraper)
    while True:
        schedule.run_pending()
        time.sleep(60)

cfg = load_config()
if os.path.exists(ROOT_HTML) and not os.path.exists(DASHBOARD):
    shutil.copy2(ROOT_HTML, DASHBOARD)
try:
    import db as database
    database.init_db(os.path.join(BASE_DIR, cfg.get("database_path","data/kol_monitor.db")))
except Exception as e:
    logger.warning(f"DB init: {e}")
if not os.path.exists(LATEST_JSON):
    threading.Thread(target=_run_scraper, daemon=True).start()
threading.Thread(target=_scheduler, args=(cfg,), daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
