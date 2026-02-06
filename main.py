import os, re, json, time
import requests, feedparser
from dateutil import parser as dtparser

RSS = [
  ("경제", "https://www.hankyung.com/feed/economy"),
  ("국제", "https://www.hankyung.com/feed/international"),
  ("ai", "https://www.hankyung.com/feed/it"),
]

CJ_PATTERN = re.compile(r"(?:\bCJ\b|씨제이|cj)", re.IGNORECASE)

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NEWS_DB_ID = os.environ["NEWS_DB_ID"]
TERMS_DB_ID = os.environ["TERMS_DB_ID"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

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

def openai_extract_summary_and_terms(title, desc):
    # 최소 입력(제목+RSS 설명)으로 요약/용어 2개를 한 번에 뽑는다.
    prompt = f"""
다음 내용을 바탕으로 한국어로만 답해라.
1) 요약: 2~3문장
2) 용어: 핵심 용어 2개(너무 일반어 제외)
JSON만 출력.
입력:
제목: {title}
내용: {desc}
출력 형식:
{{"summary":"...","terms":["...","..."]}}
""".strip()

    # Chat Completions (호환성 높은 형태)
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
    }
    r = requests.post("https://api.openai.com/v1/chat/completions", headers=openai_headers(), json=body, timeout=60)
    r.raise_for_status()
    text = r.json()["choices"][0]["message"]["content"].strip()

    # JSON만 나오게 강제했지만 방어적으로 파싱
    m = re.search(r"\{.*\}", text, re.S)
    data = json.loads(m.group(0) if m else text)
    terms = [t.strip() for t in data.get("terms", []) if t and t.strip()][:2]
    while len(terms) < 2:
        terms.append("")

    return data.get("summary", "").strip(), terms

def notion_search_page_by_title(db_id, title):
    url = f"{NOTION_API}/databases/{db_id}/query"
    payload = {
        "filter": {
            "property": "용어",
            "title": {"equals": title}
        }
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None

def notion_query_news_by_url(url_value):
    url = f"{NOTION_API}/databases/{NEWS_DB_ID}/query"
    payload = {
        "filter": {
            "property": "url",
            "url": {"equals": url_value}
        }
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        print("NOTION 400 BODY:", r.text)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None

def notion_create_news_page(published_iso, title, author, category, summary, url_value, term_names):
    url = f"{NOTION_API}/pages"
    payload = {
        "parent": {"database_id": NEWS_DB_ID},
        "properties": {
            "게시일": {"date": {"start": published_iso}},
            "제목": {"title": [{"text": {"content": title}}]},
            "작성자": {"rich_text": [{"text": {"content": author}}]} if author else {"rich_text": []},
            "카테고리": {"select": {"name": category}},
            "요약": {"rich_text": [{"text": {"content": summary}}]} if summary else {"rich_text": []},
            "url": {"url": url_value},
            "용어": {"multi_select": [{"name": t} for t in term_names if t]},
        }
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["id"]

def notion_create_term_page(term, meaning=""):
    url = f"{NOTION_API}/pages"
    payload = {
        "parent": {"database_id": TERMS_DB_ID},
        "properties": {
            "용어": {"title": [{"text": {"content": term}}]},
            "의미": {"rich_text": [{"text": {"content": meaning}}]} if meaning else {"rich_text": []},
            "관련 기사": {"relation": []},
        }
    }
    r = requests.post(url, headers=notion_headers(), json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["id"]

def notion_append_relation(term_page_id, news_page_id):
    url = f"{NOTION_API}/pages/{term_page_id}"
    # relation 추가는 "전체를 새로 지정" 방식이라 기존 relation을 읽어와 합치는게 안전.
    # 단순화를 위해: term 페이지를 읽고 기존 relation 가져온 뒤 중복 없이 추가.
    page = requests.get(url, headers=notion_headers(), timeout=60)
    page.raise_for_status()
    props = page.json()["properties"]["관련 기사"]["relation"]
    existing = {x["id"] for x in props}
    if news_page_id in existing:
        return

    new_rel = [{"id": rid} for rid in list(existing) + [news_page_id]]
    payload = {"properties": {"관련 기사": {"relation": new_rel}}}
    r = requests.patch(url, headers=notion_headers(), json=payload, timeout=60)
    r.raise_for_status()

def main():
    items = []
    for cat, feed_url in RSS:
        feed = feedparser.parse(feed_url)
        for e in feed.entries[:20]:
            link = getattr(e, "link", "")
            title = getattr(e, "title", "").strip()
            desc = getattr(e, "summary", "") or getattr(e, "description", "")
            published = getattr(e, "published", "") or getattr(e, "updated", "")
            if not (title and link and published):
                continue
            dt = dtparser.parse(published)
            items.append({
                "base_category": cat,
                "feed": feed_url,
                "title": title,
                "desc": re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", desc)).strip(),
                "link": link,
                "published_dt": dt,
            })

    # 최신순 정렬 후 상위 3개
    items.sort(key=lambda x: x["published_dt"], reverse=True)
    picked = items[:3]

    for it in picked:
        # 중복 방지(뉴스 DB에 url이 이미 있으면 스킵)
        if notion_query_news_by_url(it["link"]):
            continue

        summary, terms = openai_extract_summary_and_terms(it["title"], it["desc"])

        # 카테고리 결정: CJ 우선
        category = it["base_category"]
        if CJ_PATTERN.search(it["title"]) or CJ_PATTERN.search(summary) or CJ_PATTERN.search(it["link"]):
            category = "cj"

        published_iso = it["published_dt"].date().isoformat()
        author = "한국경제"

        news_page_id = notion_create_news_page(
            published_iso=published_iso,
            title=it["title"],
            author=author,
            category=category,
            summary=summary,
            url_value=it["link"],
            term_names=terms,
        )

        # 용어 DB upsert + relation 연결
        for term in [t for t in terms if t]:
            term_page_id = notion_search_page_by_title(TERMS_DB_ID, term)
            if not term_page_id:
                term_page_id = notion_create_term_page(term, meaning="")
            notion_append_relation(term_page_id, news_page_id)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
