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
NEWS_DB_ID     = os.environ["NEWS_DB_ID"]
TERMS_DB_ID    = os.environ["TERMS_DB_ID"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Optional: force specific data_source_id if you want
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

# -----------------------------
# Notion: retrieve database / data source
# -----------------------------
def notion_get(url: str):
    r = requests.get(url, headers=notion_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion GET error {r.status_code}: {r.text}")
    return r.json()

def notion_post(url: str, payload: dict):
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion POST error {r.status_code}: {r.text}")
    return r.json()

def notion_patch(url: str, payload: dict):
    r = requests.patch(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Notion PATCH error {r.status_code}: {r.text}")
    return r.json()

def list_data_source_ids(database_id: str) -> list[str]:
    db = notion_get(f"{NOTION_API}/databases/{database_id}")
    data_sources = db.get("data_sources") or []
    ids = [ds.get("id") for ds in data_sources if ds.get("id")]
    # fallback
    if not ids:
        ids = db.get("child_data_source_ids") or []
    if not ids:
        raise RuntimeError("No data sources found for database.")
    return ids

def retrieve_data_source(ds_id: str) -> dict:
    return notion_get(f"{NOTION_API}/data_sources/{ds_id}")

def build_prop_name_to_id(ds_obj: dict) -> dict:
    # ds_obj["properties"] = { "표시이름": { "id": "...", "type": "...", ... }, ... }
    props = ds_obj.get("properties") or {}
    out = {}
    for name, meta in props.items():
        pid = (meta or {}).get("id")
        if pid:
            out[name] = pid
    return out

def find_prop_id_case_insensitive(name_to_id: dict, want: str) -> str:
    # 정확히 일치하면 우선
    if want in name_to_id:
        return name_to_id[want]
    # 대소문자 무시 매칭
    wl = want.strip().lower()
    for k, v in name_to_id.items():
        if k.strip().lower() == wl:
            return v
    # 그래도 없으면 에러
    raise RuntimeError(f"Property not found in data source: {want}")

def pick_data_source_with_required_props(database_id: str, required_prop_names: list[str]) -> tuple[str, dict]:
    ids = list_data_source_ids(database_id)
    last_err = None

    for ds_id in ids:
        try:
            ds = retrieve_data_source(ds_id)
            name_to_id = build_prop_name_to_id(ds)

            # required_prop_names가 모두 존재하는 data source를 선택
            for need in required_prop_names:
                _ = find_prop_id_case_insensitive(name_to_id, need)

            print(f"[Notion] Picked data_source_id={ds_id} for database_id={database_id}")
            return ds_id, name_to_id
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Could not find a data source containing required properties. Last error: {last_err}")

# -----------------------------
# Resolve DS + property IDs
# -----------------------------
NEWS_REQUIRED = ["게시일", "제목", "작성자", "카테고리", "요약", "url", "용어"]
TERMS_REQUIRED = ["용어", "의미", "관련 기사"]

if NEWS_DS_ID_ENV:
    NEWS_DS_ID = NEWS_DS_ID_ENV
    NEWS_NAME_TO_ID = build_prop_name_to_id(retrieve_data_source(NEWS_DS_ID))
else:
    NEWS_DS_ID, NEWS_NAME_TO_ID = pick_data_source_with_required_props(NEWS_DB_ID, NEWS_REQUIRED)

if TERMS_DS_ID_ENV:
    TERMS_DS_ID = TERMS_DS_ID_ENV
    TERMS_NAME_TO_ID = build_prop_name_to_id(retrieve_data_source(TERMS_DS_ID))
else:
    TERMS_DS_ID, TERMS_NAME_TO_ID = pick_data_source_with_required_props(TERMS_DB_ID, TERMS_REQUIRED)

# property ids (use ids everywhere)
NEWS_PROP = {k: find_prop_id_case_insensitive(NEWS_NAME_TO_ID, k) for k in NEWS_REQUIRED}
TERMS_PROP = {k: find_prop_id_case_insensitive(TERMS_NAME_TO_ID, k) for k in TERMS_REQUIRED}

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
        raise RuntimeError(f"OpenAI error {r.status_code}: {r.text}")

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
# Notion: query / create / update (data_sources)
# -----------------------------
def notion_query_data_source(ds_id: str, payload: dict):
    return notion_post(f"{NOTION_API}/data_sources/{ds_id}/query", payload)

def notion_get_page(page_id: str):
    return notion_get(f"{NOTION_API}/pages/{page_id}")

def notion_query_news_by_url(url_value: str):
    payload = {
        "filter": {
            "property": NEWS_PROP["url"],   # property ID
            "url": {"equals": url_value}
        }
    }
    data = notion_query_data_source(NEWS_DS_ID, payload)
    results = data.get("results", [])
    return results[0]["id"] if results else None

def notion_find_term_page_id(term: str):
    payload = {
        "filter": {
            "property": TERMS_PROP["용어"],  # title property ID
            "title": {"equals": term}
        }
    }
    data = notion_query_data_source(TERMS_DS_ID, payload)
    results = data.get("results", [])
    return results[0]["id"] if results else None

def notion_create_news_page(published_iso: str, title: str, author: str, category: str, summary: str, url_value: str, terms: list[str]):
    payload = {
        "parent": {"data_source_id": NEWS_DS_ID},
        "properties": {
            NEWS_PROP["게시일"]: {"date": {"start": published_iso}},
            NEWS_PROP["제목"]: {"title": [{"text": {"content": title}}]},
            NEWS_PROP["작성자"]: {"rich_text": [{"text": {"content": author}}]} if author else {"rich_text": []},
            NEWS_PROP["카테고리"]: {"select": {"name": category}},
            NEWS_PROP["요약"]: {"rich_text": [{"text": {"content": summary}}]} if summary else {"rich_text": []},
            NEWS_PROP["url"]: {"url": url_value},
            NEWS_PROP["용어"]: {"multi_select": [{"name": t} for t in terms if t]},
        },
    }
    res = notion_post(f"{NOTION_API}/pages", payload)
    return res["id"]

def notion_create_term_page(term: str, meaning: str = ""):
    payload = {
        "parent": {"data_source_id": TERMS_DS_ID},
        "properties": {
            TERMS_PROP["용어"]: {"title": [{"text": {"content": term}}]},
            TERMS_PROP["의미"]: {"rich_text": [{"text": {"content": meaning}}]} if meaning else {"rich_text": []},
            TERMS_PROP["관련 기사"]: {"relation": []},
        },
    }
    res = notion_post(f"{NOTION_API}/pages", payload)
    return res["id"]

def notion_append_relation(term_page_id: str, news_page_id: str):
    page = notion_get_page(term_page_id)
    rel = page["properties"][TERMS_PROP["관련 기사"]]["relation"]
    existing = {x["id"] for x in rel}
    if news_page_id in existing:
        return

    new_rel = [{"id": rid} for rid in list(existing) + [news_page_id]]
    payload = {
        "properties": {
            TERMS_PROP["관련 기사"]: {"relation": new_rel}
        }
    }
    notion_patch(f"{NOTION_API}/pages/{term_page_id}", payload)

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
                "title": title,
                "desc": clean_html(desc),
                "link": link,
                "author": author,
                "published_dt": dt,
            })

    if not items:
        print("No RSS items found.")
        return

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

        for term in [t for t in terms if t]:
            term_page_id = notion_find_term_page_id(term)
            if not term_page_id:
                term_page_id = notion_create_term_page(term, meaning="")
                print("Created term page:", term, term_page_id)

            notion_append_relation(term_page_id, news_page_id)
            print("Linked term -> news:", term, term_page_id, news_page_id)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
