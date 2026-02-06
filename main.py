import os, re, json, time
import requests, feedparser
from dateutil import parser as dtparser

# -----------------------------
# Feeds / Rules
# -----------------------------
RSS = [
    ("경제", "https://www.hankyung.com/feed/economy"),
    ("국제", "https://www.hankyung.com/feed/international"),
    ("ai",  "https://www.hankyung.com/feed/it"),
]
CJ_PATTERN = re.compile(r"(?:\bCJ\b|씨제이|cj)", re.IGNORECASE)

# -----------------------------
# Secrets (GitHub Actions)
# -----------------------------
NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NEWS_DB_ID     = os.environ["NEWS_DB_ID"]     # database id (32 chars, hyphens ok)
TERMS_DB_ID    = os.environ["TERMS_DB_ID"]    # database id (32 chars, hyphens ok)
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Optional overrides (if DB has multiple data sources and you want to pick a specific one)
NEWS_DS_ID_ENV  = os.environ.get("NEWS_DS_ID", "").strip()
TERMS_DS_ID_ENV = os.environ.get("TERMS_DS_ID", "").strip()

# -----------------------------
# Notion API
# -----------------------------
NOTION_API = "https://api.notion.com/v1"
NOTION_VER = "2025-09-03"

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def openai_headers():
    return {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

def clean_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_data_source_id_from_database(database_id: str, pick: int = 0) -> str:
    r = requests.get(f"{NOTION_API}/databases/{database_id}", headers=notion_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion database retrieve error: {r.status_code} {r.text}")
    db = r.json()

    data_sources = db.get("data_sources") or []
    ids = [ds.get("id") for ds in data_sources if ds.get("id")]

    if not ids:
        # Fallback in case response shape changes
        ids = db.get("child_data_source_ids") or []

    if not ids:
        raise RuntimeError("No data_source_id found for this database.")

    if pick < 0 or pick >= len(ids):
        pick = 0

    # Print for debugging (Actions log)
    print(f"[Notion] database_id={database_id} data_source_ids={ids} picked={ids[pick]}")
    return ids[pick]

# Resolve data_source_id
NEWS_DS_ID  = NEWS_DS_ID_ENV  or get_data_source_id_from_database(NEWS_DB_ID, pick=0)
TERMS_DS_ID = TERMS_DS_ID_ENV or get_data_source_id_from_database(TERMS_DB_ID, pick=0)

# -----------------------------
# OpenAI: summary + terms(2)
# -----------------------------
def openai_summary_and_terms(title: str, desc: str):
    prompt = (
        "다음 기사 정보로 작업해라.\n"
        "1) summary: 한국어 2~3문장 요약\n"
        "2) terms: 핵심 용어 2개(너무 일반적인 단어 제외)\n"
        "JSON만 출력.\n\n"
        f"제목: {title}\n"
        f"내용: {desc}\n\n"
        '출력 형식: {"summary":"...","terms":["...","..."]}'
    )

    body = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=openai_headers(), json=body, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"OpenAI error: {r.status_code} {r.text}")

    text = r.json()["choices"][0]["message"]["content"].strip()
    m = re.search(r"\{.*\}", text, re.S)
    data = json.loads(m.group(0) if m else text)

    summary = (data.get("summary") or "").strip()
    terms = [str(t).strip() for t in (data.get("terms") or []) if str(t).strip()]
    terms = terms[:2]
    while len(terms) < 2:
        terms.append("")
    return summary, terms

# -----------------------------
# Notion Query / Create / Update (data_sources)
# -----------------------------
def notion_query_data_source(ds_id: str, payload: dict):
    url = f"{NOTION_API}/data_sources/{ds_id}/query"
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion query error: {r.status_code} {r.text}")
    return r.json()

def notion_query_news_by_url(url_value: str):
    payload = {"filter": {"property": "url", "url": {"equals": url_value}}}
    data = notion_query_data_source(NEWS_DS_ID, payload)
    results = data.get("results", [])
    return results[0]["id"] if results else None

def notion_find_term_page_id(term: str):
    payload = {"filter": {"property": "용어", "title": {"equals": term}}}
    data = notion_query_data_source(TERMS_DS_ID, payload)
    results = data.get("results", [])
    return results[0]["id"] if results else None

def notion_create_news_page(published_iso: str, title: str, author: str, category: str, summary: str, url_value: str, terms: list[str]):
    url = f"{NOTION_API}/pages"
    payload = {
        "parent": {"data_source_id": NEWS_DS_ID},
        "properties": {
            "게시일": {"date": {"start": published_iso}},
            "제목": {"title": [{"text": {"content": title}}]},
            "작성자": {"rich_text": [{"text": {"content": author}}]} if author else {"rich_text": []},
            "카테고리": {"select": {"name": category}},
            "요약": {"rich_text": [{"text": {"content": summary}}]} if summary else {"rich_text": []},
            "url": {"url": url_value},
            "용어": {"multi_select": [{"name": t} for t in terms if t]},
        },
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion create news page error: {r.status_code} {r.text}")
    return r.json()["id"]

def notion_create_term_page(term: str, meaning: str = ""):
    url = f"{NOTION_API}/pages"
    payload = {
        "parent": {"data_source_id": TERMS_DS_ID},
        "properties": {
            "용어": {"title": [{"text": {"content": term}}]},
            "의미": {"rich_text": [{"text": {"content": meaning}}]} if meaning else {"rich_text": []},
            "관련 기사": {"relation": []},
        },
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion create term page error: {r.status_code} {r.text}")
    return r.json()["id"]

def notion_get_page(page_id: str):
    url = f"{NOTION_API}/pages/{page_id}"
    r = requests.get(url, headers=notion_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion get page error: {r.status_code} {r.text}")
    return r.json()

def notion_append_relation(term_page_id: str, news_page_id: str):
    page = notion_get_page(term_page_id)
    rel = page["properties"]["관련 기사"]["relation"]
    existing = {x["id"] for x in rel}
    if news_page_id in existing:
        return

    new_rel = [{"id": rid} for rid in list(existing) + [news_page_id]]
    url = f"{NOTION_API}/pages/{term_page_id}"
    payload = {"properties": {"관련 기사": {"relation": new_rel}}}
    r = requests.patch(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion update relation error: {r.status_code} {r.text}")

# -----------------------------
# Main
# -----------------------------
def main():
    items = []
    for base_cat, feed_url in RSS:
        feed = feedparser.parse(feed_url)
        for e in (feed.entries or [])[:30]:
            title = (getattr(e, "title", "") or "").strip()
            link = (getattr(e, "link", "") or "").strip()
            published = (getattr(e, "published", "") or getattr(e, "updated", "") or "").strip()
            desc = getattr(e, "summary", "") or getattr(e, "description", "") or ""
            author = (getattr(e, "author", "") or "").strip()

            if not (title and link and published):
                continue

            try:
                dt = dtparser.parse(published)
            except Exception:
                continue

            items.append({
                "base_category": base_cat,
                "feed": feed_url,
                "title": title,
                "desc": clean_html(desc),
                "link": link,
                "author": author,
                "published_dt": dt,
            })

    if not items:
        print("No RSS items found.")
        return

    # 최신순 상위 3개
    items.sort(key=lambda x: x["published_dt"], reverse=True)
    picked = items[:3]

    for it in picked:
        # 중복 방지(url 기준)
        if notion_query_news_by_url(it["link"]):
            print("Skip duplicate:", it["link"])
            continue

        summary, terms = openai_summary_and_terms(it["title"], it["desc"])

        category = it["base_category"]
        if CJ_PATTERN.search(it["title"]) or CJ_PATTERN.search(summary) or CJ_PATTERN.search(it["link"]):
            category = "cj"

        published_iso = it["published_dt"].date().isoformat()
        author = it["author"] or "한국경제"

        news_page_id = notion_create_news_page(
            published_iso=published_iso,
            title=it["title"],
            author=author,
            category=category,
            summary=summary,
            url_value=it["link"],
            terms=terms,
        )
        print("Created news page:", news_page_id)

        # 용어 DB upsert + relation 연결
        for term in [t for t in terms if t]:
            term_page_id = notion_find_term_page_id(term)
            if not term_page_id:
                term_page_id = notion_create_term_page(term, meaning="")
                print("Created term page:", term, term_page_id)

            notion_append_relation(term_page_id, news_page_id)
            print("Linked:", term_page_id, "->", news_page_id)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
