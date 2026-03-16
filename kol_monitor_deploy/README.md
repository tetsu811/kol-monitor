# 醫療 KOL 爆文監測系統

## 快速開始

### 1. 設定 config.json
```json
{
  "openai_api_key": "sk-YOUR_KEY_HERE",
  ...
}
```

### 2. 初始化資料庫
```bash
python main.py init
```

### 3. 手動執行一次（測試）
```bash
python main.py run
```

### 4. 開啟儀表板
用瀏覽器開啟 `data/index.html`（需先 run 一次產生 latest.json）

### 5. 設定每日自動執行（cron）
```bash
# 每天早上 6:00 執行
0 6 * * * cd /path/to/kol_monitor && python main.py run >> data/cron.log 2>&1
```

## 專案結構
```
kol_monitor/
├── config.json          # 設定檔（帳號清單、API key）
├── main.py              # 主程式入口
├── requirements.txt     # Python 套件需求
├── scraper/
│   └── threads_scraper.py   # Threads 爬蟲
├── database/
│   └── db.py                # SQLite 資料層
├── analyzer/
│   └── llm_analyzer.py      # OpenAI LLM 分析
├── dashboard/
│   └── index.html           # HTML 儀表板
└── data/
    ├── kol_monitor.db        # SQLite 資料庫
    ├── latest.json           # 最新資料（前端讀取）
    ├── index.html            # 儀表板（複製自 dashboard/）
    └── snapshots/            # 每日備份快照
```
