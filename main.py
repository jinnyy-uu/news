import os, re, json, time
import requests, feedparser
from dateutil import parser as dtparser

RSS = [
    ("경제", "https://www.hankyung.com/feed/economy"),
    ("국제", "https://www.hankyung.com/feed/international"),
    ("ai",  "https://www.hankyung.com/feed/it"),
]
CJ_PATTERN = re.compile(r"(?:\bCJ\b|씨제이|cj)", re.IGNORECASE)

NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NEWS_DB_ID     = os.environ["NEWS_DB_ID"]
TERMS_DB_ID    = os.environ["TERMS_DB_ID"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

NEWS_DS_ID_ENV  = os.environ.get("NEWS_DS_ID", "").strip()
TERMS_DS_ID_ENV = os.environ.get("TERMS_DS_ID", "").strip()

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
    ds = db.get("data_sources") or []
    ids = [x.get("id") for x in ds if x.get("id")]
    if not ids:
        ids = db.get("child_data_source_ids") or []
    if not ids:
        raise RuntimeError("No data sources found for database.")
    return ids

def retrieve_data_source(ds_id: str) -> dict:
    return notion_get(f"{NOTION_API}/data_sources/{ds_id}")

def build_prop_meta(ds_obj: dict) -> dict:
    # returns: {prop_name: {"id": "...", "type": "..."}}
    props = ds_obj.get("properties") or {}
    meta = {}
    for name, p in props.items():
        if not p:
            continue
        meta[name] = {"id": p.get("id"), "type": p.get("type")}
    return meta

def find_prop_by_name(meta: dict, want_name: str):
    if want_name in meta:
        return want_name, meta[want_name]
    wl = want_name.strip().lower()
    for k, v in meta.items():
        if k.strip().lower() == wl:
            return k, v
    return None, None

def find_prop_by_type(meta: dict, want_type: str):
    for k, v in meta.items():
        if (v or {}).get("type") == want_type:
            return k, v
    return None, None

def pick_data_source_with_names(database_id: str, required_names: list[str]) -> tuple[str, dict]:
    ids = list_data_source_ids(database_id)
    last_err = None
    for ds_id in ids:
        try:
            ds = retrieve_data_source(ds_id)
            meta = build_prop_meta(ds)
            for n in required_names:
                k, _ = find_prop_by_name(meta, n)
                if not k:
                    raise RuntimeError(f"missing property name: {n}")
            print(f"[Notion] Picked data_source_id={ds_id} for database_id={database_id}")
            return ds_id, meta
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not pick data source. last_error={last_err}")

# -----------------------------
# Resolve DS + property names/types
# -----------------------------
NEWS_REQUIRED_NAMES = ["게시일", "제목", "작성자", "카테고리", "요약", "url", "용어"]
TERMS_REQUIRED_NAMES = ["용어", "의미", "관련 기사"]

if NEWS_DS_ID_ENV:
    NEWS_DS_ID = NEWS_DS_ID_ENV
    NEWS_META = build_prop_meta(retrieve_data_source(NEWS_DS_ID))
else:
    NEWS_DS_ID, NEWS_META = pick_data_source_with_names(NEWS_DB_ID, NEWS_REQUIRED_NAMES)

if TERMS_DS_ID_ENV:
    TERMS_DS_ID = TERMS_DS_ID_ENV
    TERMS_META = build_prop_meta(retrieve_data_source(TERMS_DS_ID))
else:
    TERMS_DS_ID, TERMS_META = pick_data_source_with_names(TERMS_DB_ID, TERMS_REQUIRED_NAMES)

# 실제 프로퍼티 "이름"을 확정 (생성/업데이트는 이름을 키로 써야 함)
NEWS_NAME = {}
for n in NEWS_REQUIRED_NAMES:
    k, v = find_prop_by_name(NEWS_META, n)
    if not k:
        raise RuntimeError(f"[News] property not found by name: {n}")
    NEWS_NAME[n] = k

TERMS_NAME = {}
for n in TERMS_REQUIRED_NAMES:
    k, v = find_prop_by_name(TERMS_META, n)
    if not k:
        raise RuntimeError(f"[Terms] property not found by name: {n}")
    TERMS_NAME[n] = k

# 타입 체크(제목은 title이어야 정상. 아니면 DB에서 제목이 title이 아닌 상태)
title_prop_name, title_meta = find_prop_by_name(NEWS_META, NEWS_NAME["제목"])
if (title_meta or {}).get("type") != "title":
    # fallback: DB의 title 프로퍼티를 찾아 "제목" 대신 사용
    k, v = find_prop_by_type(NEWS_META, "title")
    if not k:
        raise RuntimeError("News DB has no title property.")
    print(f"[Notion] '제목' is not title. Using title property '{k}' instead.")
    NEWS_NAME["제목"] = k

term_title_name, term_title_meta = find_prop_by_name(TERMS_META, TERMS_NAME["용어"])
if (term_title_meta or {}).get("type") != "title":
    k, v = find_prop_by_type(TERMS_META, "title")
    if not k:
        raise RuntimeError("Terms DB has no title property.")
    print(f"[Notion] '용어' is not title. Using title property '{k}' instead.")
    TERMS_NAME["용어"] = k

# 필터는 id 또는 name 둘 다 가능하지만, 안정적으로 id 사용
NEWS_URL_PROP_ID = (NEWS_META[NEWS_NAME["url"]] or {}).get("id")
TERMS_TERM_PROP_ID = (TERMS_META[TERMS_NAME["용어"]] or {}).get("id")

# -----------------------------
# OpenAI
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
    terms = [str(t).strip() for t in (data.get("terms") or []) if str(t).strip()][:2]
    while len(terms) < 2:
        terms.append("")
    return summary, terms

# -----------------------------
# Notion operations
# -----------------------------
def notion_query_data_source(ds_id: str, payload: dict):
    return notion_post(f"{NOTION_API}/data_sources/{ds_id}/query", payload)

def notion_get_page(page_id: str):
    return notion_get(f"{NOTION_API}/pages/{page_id}")

def notion_query_news_by_url(url_value: str):
    payload = {
        "filter": {
            "property": NEWS_URL_PROP_ID or NEWS_NAME["url"],
            "url": {"equals": url_value}
        }
    }
    data = notion_query_data_source(NEWS_DS_ID, payload)
    results = data.get("results", [])
    return results[0]["id"] if results else None

def notion_find_term_page_id(term: str):
    payload = {
        "filter": {
            "property": TERMS_TERM_PROP_ID or TERMS_NAME["용어"],
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
            NEWS_NAME["게시일"]: {"date": {"start": published_iso}},
            NEWS_NAME["제목"]: {"title": [{"text": {"content": title}}]},
            NEWS_NAME["작성자"]: {"rich_text": [{"text": {"content": author}}]} if author else {"rich_text": []},
            NEWS_NAME["카테고리"]: {"select": {"name": category}},
            NEWS_NAME["요약"]: {"rich_text": [{"text": {"content": summary}}]} if summary else {"rich_text": []},
            NEWS_NAME["url"]: {"url": url_value},
            NEWS_NAME["용어"]: {"multi_select": [{"name": t} for t in terms if t]},
        },
    }
    res = notion_post(f"{NOTION_API}/pages", payload)
    return res["id"]

def notion_create_term_page(term: str, meaning: str = ""):
    payload = {
        "parent": {"data_source_id": TERMS_DS_ID},
        "properties": {
            TERMS_NAME["용어"]: {"title": [{"text": {"content": term}}]},
            TERMS_NAME["의미"]: {"rich_text": [{"text": {"content": meaning}}]} if meaning else {"rich_text": []},
            TERMS_NAME["관련 기사"]: {"relation": []},
        },
    }
    res = notion_post(f"{NOTION_API}/pages", payload)
    return res["id"]

def notion_append_relation(term_page_id: str, news_page_id: str):
    page = notion_get_page(term_page_id)
    rel = page["properties"][TERMS_NAME["관련 기사"]]["relation"]
    existing = {x["id"] for x in rel}
    if news_page_id in existing:
        return
    new_rel = [{"id": rid} for rid in list(existing) + [news_page_id]]
    payload = {"properties": {TERMS_NAME["관련 기사"]: {"relation": new_rel}}}
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

    items.sort(key=lambda x: x["published_dt"], reverse=True)
    picked = items[:3]

    for it in picked:
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
            print("Linked:", term, term_page_id, "->", news_page_id)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
