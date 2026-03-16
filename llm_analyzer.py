"""
LLM analysis engine using OpenAI API (via requests, no SDK needed).
Handles:
  1. Viral post analysis (why did this post go viral?)
  2. Account specialty inference (what medical field?)
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"


class LLMAnalyzer:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model   = model
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        }

    def _call(self, system_prompt: str, user_prompt: str,
              retries: int = 3) -> Optional[str]:
        payload = {
            "model":       self.model,
            "temperature": 0.3,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
        }
        for attempt in range(retries):
            try:
                resp = requests.post(
                    OPENAI_API_URL, headers=self.headers,
                    json=payload, timeout=60
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]
            except requests.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 2)
                    logger.warning(f"Rate limit, waiting {wait}s …")
                    time.sleep(wait)
                else:
                    logger.error(f"OpenAI HTTP error: {e}")
                    break
            except Exception as e:
                logger.error(f"OpenAI call error (attempt {attempt+1}): {e}")
                time.sleep(3)
        return None

    # ── Viral post analysis ────────────────────────────────

    VIRAL_SYSTEM = """你是一位台灣醫療社群媒體內容策略專家，擅長分析 Threads 平台上醫師帳號的爆文模式。
請根據提供的文章內容與互動數據，輸出一份結構化的爆文診斷報告（JSON 格式，繁體中文）。

JSON 欄位說明：
- hook_type: 情緒鉤子類型，從以下選一：恐懼/反直覺/強烈共鳴/好奇/實用資訊/爭議性/溫情/其他
- hook_sentence: 第一句話原文（直接截取，若無則空字串）
- hook_analysis: 為何此開頭有效（100字內）
- content_structure: 內容結構類型，從以下選一：故事敘述/知識點乾貨/問答互動/案例分享/衛教警示/其他
- visual_strategy: 視覺或排版策略描述（50字內，若純文字則描述段落節奏）
- controversy_flag: 是否含爭議性議題（true/false）
- emotional_resonance: 是否運用溫情或共感故事（true/false）
- media_format: 媒體格式，從以下選一：純文字/圖文/影片/輪播/其他
- viral_score: 預測爆紅潛力分（整數 1-10，根據內容品質與情緒張力評估）
- summary: 爆文整體分析摘要（150字內，說明為何這篇會紅）"""

    def analyze_viral_post(self, post: dict) -> Optional[dict]:
        """Analyze why a post went viral."""
        content = post.get("content_text", "")[:800]
        if not content.strip():
            content = f"[{post.get('media_type', '圖片')}貼文，無文字內容]"

        user_prompt = f"""請分析以下 Threads 醫療帳號貼文：

帳號：@{post.get('handle', '')}
媒體類型：{post.get('media_type', '未知')}
讚數：{post.get('like_count', 0)}
留言數：{post.get('comment_count', 0)}
轉發數：{post.get('repost_count', 0)}
互動率：{post.get('engagement_rate', 0):.4f}%

文章內容：
{content}

請輸出 JSON 格式爆文診斷報告。"""

        raw = self._call(self.VIRAL_SYSTEM, user_prompt)
        if not raw:
            return None
        try:
            result = json.loads(raw)
            result["analyzed_at"] = datetime.now(timezone.utc).isoformat()
            return result
        except Exception as e:
            logger.error(f"Failed to parse viral analysis JSON: {e}")
            return None

    # ── Specialty inference ────────────────────────────────

    SPECIALTY_SYSTEM = """你是一位台灣醫療產業分析師，擅長從醫師的社群媒體資料推測其診療科別。
請根據提供的帳號資訊與近期貼文，推測該帳號主要的醫療專科。

JSON 欄位說明：
- primary_specialty: 主要科別（中文，例如：皮膚科、骨科、中醫、內科、婦產科、兒科、眼科、耳鼻喉科、精神科、心臟科、家醫科、復健科、營養醫學、一般外科、神經外科、泌尿科、整形外科）
- secondary_specialties: 次要科別陣列（可空）
- confidence_score: 信心分數（0.0-1.0）
- reasoning: 推測依據（50字內，說明從哪些線索判斷）"""

    def infer_specialty(self, handle: str, display_name: str,
                        bio: str, recent_posts: list) -> Optional[dict]:
        """Infer medical specialty from profile and posts."""
        posts_text = ""
        for p in recent_posts[:10]:
            text = (p.get("content_text") or "")[:200]
            if text:
                posts_text += f"- {text}\n"

        user_prompt = f"""請推測以下 Threads 醫療帳號的診療科別：

帳號：@{handle}
顯示名稱：{display_name or '（未知）'}
個人簡介：{bio or '（無）'}

近期貼文摘要：
{posts_text or '（無可用貼文）'}

請輸出 JSON 格式科別推測結果。"""

        raw = self._call(self.SPECIALTY_SYSTEM, user_prompt)
        if not raw:
            return None
        try:
            result = json.loads(raw)
            result["analyzed_at"] = datetime.now(timezone.utc).isoformat()
            return result
        except Exception as e:
            logger.error(f"Failed to parse specialty JSON: {e}")
            return None
