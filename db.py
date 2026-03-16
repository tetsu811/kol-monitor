"""
SQLite database layer for KOL Monitor.
All tables created on first run.
"""
import sqlite3
import json
import os
from datetime import datetime, timezone


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str):
    conn = get_conn(db_path)
    c = conn.cursor()

    # ── accounts ──────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        handle              TEXT PRIMARY KEY,
        display_name        TEXT,
        bio                 TEXT,
        follower_count      INTEGER,
        following_count     INTEGER,
        post_count          INTEGER,
        verified            INTEGER DEFAULT 0,
        avatar_url          TEXT,
        external_link       TEXT,
        profile_crawled_at  TEXT,
        created_at          TEXT DEFAULT (datetime('now'))
    )""")

    # ── account_specialty ─────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS account_specialty (
        handle              TEXT PRIMARY KEY,
        primary_specialty   TEXT,
        secondary_specialties TEXT,
        confidence_score    REAL,
        reasoning           TEXT,
        analyzed_at         TEXT,
        FOREIGN KEY(handle) REFERENCES accounts(handle)
    )""")

    # ── posts ─────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        post_id             TEXT PRIMARY KEY,
        handle              TEXT,
        post_url            TEXT,
        content_text        TEXT,
        media_type          TEXT,
        media_urls          TEXT,
        post_timestamp      TEXT,
        like_count          INTEGER DEFAULT 0,
        comment_count       INTEGER DEFAULT 0,
        repost_count        INTEGER DEFAULT 0,
        quote_count         INTEGER DEFAULT 0,
        view_count          INTEGER,
        is_reply            INTEGER DEFAULT 0,
        engagement_rate     REAL,
        is_viral_flag       INTEGER DEFAULT 0,
        crawled_at          TEXT,
        FOREIGN KEY(handle) REFERENCES accounts(handle)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posts_handle ON posts(handle)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posts_timestamp ON posts(post_timestamp)")

    # ── post_metrics_history ──────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS post_metrics_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id         TEXT,
        like_count      INTEGER,
        comment_count   INTEGER,
        repost_count    INTEGER,
        view_count      INTEGER,
        recorded_at     TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(post_id) REFERENCES posts(post_id)
    )""")

    # ── llm_analysis ──────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS llm_analysis (
        post_id             TEXT PRIMARY KEY,
        hook_type           TEXT,
        hook_sentence       TEXT,
        hook_analysis       TEXT,
        content_structure   TEXT,
        visual_strategy     TEXT,
        controversy_flag    INTEGER DEFAULT 0,
        emotional_resonance INTEGER DEFAULT 0,
        media_format        TEXT,
        viral_score         INTEGER,
        summary             TEXT,
        analyzed_at         TEXT,
        FOREIGN KEY(post_id) REFERENCES posts(post_id)
    )""")

    # ── crawl_log ─────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS crawl_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at          TEXT DEFAULT (datetime('now')),
        accounts_ok     INTEGER DEFAULT 0,
        accounts_fail   INTEGER DEFAULT 0,
        posts_new       INTEGER DEFAULT 0,
        analyses_done   INTEGER DEFAULT 0,
        errors          TEXT,
        duration_secs   REAL
    )""")

    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {db_path}")


# ── Account helpers ────────────────────────────────────────

def upsert_account(db_path: str, data: dict):
    conn = get_conn(db_path)
    conn.execute("""
    INSERT INTO accounts
        (handle, display_name, bio, follower_count, following_count,
         post_count, verified, avatar_url, external_link, profile_crawled_at)
    VALUES
        (:handle, :display_name, :bio, :follower_count, :following_count,
         :post_count, :verified, :avatar_url, :external_link, :profile_crawled_at)
    ON CONFLICT(handle) DO UPDATE SET
        display_name       = excluded.display_name,
        bio                = excluded.bio,
        follower_count     = excluded.follower_count,
        following_count    = excluded.following_count,
        post_count         = excluded.post_count,
        verified           = excluded.verified,
        avatar_url         = excluded.avatar_url,
        external_link      = excluded.external_link,
        profile_crawled_at = excluded.profile_crawled_at
    """, data)
    conn.commit()
    conn.close()


def upsert_specialty(db_path: str, handle: str, specialty: dict):
    conn = get_conn(db_path)
    conn.execute("""
    INSERT INTO account_specialty
        (handle, primary_specialty, secondary_specialties, confidence_score, reasoning, analyzed_at)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(handle) DO UPDATE SET
        primary_specialty     = excluded.primary_specialty,
        secondary_specialties = excluded.secondary_specialties,
        confidence_score      = excluded.confidence_score,
        reasoning             = excluded.reasoning,
        analyzed_at           = excluded.analyzed_at
    """, (
        handle,
        specialty.get("primary_specialty", ""),
        json.dumps(specialty.get("secondary_specialties", []), ensure_ascii=False),
        specialty.get("confidence_score", 0),
        specialty.get("reasoning", ""),
        specialty.get("analyzed_at", datetime.now(timezone.utc).isoformat()),
    ))
    conn.commit()
    conn.close()


def get_accounts_without_specialty(db_path: str) -> list:
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT a.handle, a.display_name, a.bio
        FROM accounts a
        LEFT JOIN account_specialty s ON a.handle = s.handle
        WHERE s.handle IS NULL OR s.analyzed_at IS NULL
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Post helpers ───────────────────────────────────────────

def upsert_post(db_path: str, post: dict) -> bool:
    """Returns True if post is new."""
    conn = get_conn(db_path)
    existing = conn.execute(
        "SELECT post_id FROM posts WHERE post_id=?", (post["post_id"],)
    ).fetchone()

    if existing:
        conn.execute("""
        UPDATE posts SET
            like_count=:like_count, comment_count=:comment_count,
            repost_count=:repost_count, quote_count=:quote_count,
            view_count=:view_count, crawled_at=:crawled_at
        WHERE post_id=:post_id
        """, post)
        conn.commit()
        conn.close()
        return False
    else:
        conn.execute("""
        INSERT OR IGNORE INTO posts
            (post_id, handle, post_url, content_text, media_type, media_urls,
             post_timestamp, like_count, comment_count, repost_count, quote_count,
             view_count, is_reply, crawled_at)
        VALUES
            (:post_id, :handle, :post_url, :content_text, :media_type, :media_urls,
             :post_timestamp, :like_count, :comment_count, :repost_count, :quote_count,
             :view_count, :is_reply, :crawled_at)
        """, post)
        conn.commit()
        conn.close()
        return True


def record_metrics_history(db_path: str, post_id: str, metrics: dict):
    conn = get_conn(db_path)
    conn.execute("""
    INSERT INTO post_metrics_history (post_id, like_count, comment_count, repost_count, view_count)
    VALUES (?,?,?,?,?)
    """, (
        post_id,
        metrics.get("like_count", 0),
        metrics.get("comment_count", 0),
        metrics.get("repost_count", 0),
        metrics.get("view_count"),
    ))
    conn.commit()
    conn.close()


def compute_engagement_rates(db_path: str):
    """Compute ER = (likes + comments + reposts) for all posts, then flag viral ones."""
    conn = get_conn(db_path)
    # Get all posts with their account's follower count
    rows = conn.execute("""
        SELECT p.post_id, p.handle,
               p.like_count, p.comment_count, p.repost_count,
               COALESCE(a.follower_count, 1000) AS followers
        FROM posts p
        JOIN accounts a ON p.handle = a.handle
    """).fetchall()

    for row in rows:
        er = (row["like_count"] + row["comment_count"] + row["repost_count"]) / max(row["followers"], 1) * 100
        conn.execute("UPDATE posts SET engagement_rate=? WHERE post_id=?", (er, row["post_id"]))

    conn.commit()

    # Flag viral: ER > 1.5x account average
    handles = conn.execute("SELECT DISTINCT handle FROM posts").fetchall()
    for h in handles:
        handle = h["handle"]
        avg_er = conn.execute(
            "SELECT AVG(engagement_rate) FROM posts WHERE handle=? AND engagement_rate IS NOT NULL",
            (handle,)
        ).fetchone()[0] or 0

        threshold = avg_er * 1.5
        conn.execute("""
        UPDATE posts SET is_viral_flag = CASE WHEN engagement_rate >= ? THEN 1 ELSE 0 END
        WHERE handle=?
        """, (threshold, handle))

    conn.commit()
    conn.close()


def get_viral_posts_needing_analysis(db_path: str, limit: int = 50) -> list:
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT p.post_id, p.handle, p.content_text, p.media_type,
               p.like_count, p.comment_count, p.repost_count, p.view_count,
               p.engagement_rate
        FROM posts p
        LEFT JOIN llm_analysis la ON p.post_id = la.post_id
        WHERE p.is_viral_flag = 1 AND la.post_id IS NULL
        ORDER BY p.engagement_rate DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_llm_analysis(db_path: str, post_id: str, analysis: dict):
    conn = get_conn(db_path)
    conn.execute("""
    INSERT OR REPLACE INTO llm_analysis
        (post_id, hook_type, hook_sentence, hook_analysis, content_structure,
         visual_strategy, controversy_flag, emotional_resonance, media_format,
         viral_score, summary, analyzed_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        post_id,
        analysis.get("hook_type", ""),
        analysis.get("hook_sentence", ""),
        analysis.get("hook_analysis", ""),
        analysis.get("content_structure", ""),
        analysis.get("visual_strategy", ""),
        1 if analysis.get("controversy_flag") else 0,
        1 if analysis.get("emotional_resonance") else 0,
        analysis.get("media_format", ""),
        analysis.get("viral_score", 0),
        analysis.get("summary", ""),
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def write_crawl_log(db_path: str, log: dict):
    conn = get_conn(db_path)
    conn.execute("""
    INSERT INTO crawl_log (run_at, accounts_ok, accounts_fail, posts_new, analyses_done, errors, duration_secs)
    VALUES (datetime('now'), :accounts_ok, :accounts_fail, :posts_new, :analyses_done, :errors, :duration_secs)
    """, log)
    conn.commit()
    conn.close()


# ── Export helpers ─────────────────────────────────────────

def export_full_data(db_path: str) -> dict:
    """Export everything needed for the HTML dashboard."""
    conn = get_conn(db_path)

    accounts = conn.execute("""
        SELECT a.*, s.primary_specialty, s.secondary_specialties,
               s.confidence_score, s.reasoning
        FROM accounts a
        LEFT JOIN account_specialty s ON a.handle = s.handle
        ORDER BY a.follower_count DESC NULLS LAST
    """).fetchall()

    posts = conn.execute("""
        SELECT p.*, la.hook_type, la.hook_sentence, la.hook_analysis,
               la.content_structure, la.visual_strategy, la.controversy_flag,
               la.emotional_resonance, la.media_format, la.viral_score, la.summary,
               la.analyzed_at as analysis_date
        FROM posts p
        LEFT JOIN llm_analysis la ON p.post_id = la.post_id
        ORDER BY p.like_count DESC
    """).fetchall()

    conn.close()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "accounts": [dict(r) for r in accounts],
        "posts": [dict(r) for r in posts],
    }
