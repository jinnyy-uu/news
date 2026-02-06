#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GitHub Actions (cron)용 Notion 뉴스 자동 업로드 파이프라인 - 단일 파일 최종본(main.py)

요구사항:
- Notion API 최신 버전(2025-09-03) + data_sources 기반 처리
- DB에 data source가 여러 개일 때, "프로퍼티 스키마 매칭"으로 올바른 data_source_id 자동 선택
- 한국경제 RSS(경제/국제/IT) 최신 기사 3개 수집
- 기사별 요약(2~3문장) + 핵심 용어 2개(OpenAI 사용, 없으면 폴백)
- 뉴스 DB에 저장 + 용어 DB upsert + relation 연결

필수 ENV:
- NOTION_TOKEN

선택 ENV:
- OPENAI_API_KEY (있으면 요약/용어 추출 품질 향상)
- OPENAI_MODEL (기본: gpt-4o-mini)

고정 DB ID(요청값):
- 뉴스 DB: 2ff62df4842180b6944df052819a8872
- 용어 DB: 2ff62df48421808ea7cbdbd4935f5b6b
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import traceback
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# -----------------------------
# Config
# -----------------------------

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
if not NOTION_TOKEN:
    print("ERROR: NOTION_TOKEN is required.", file=sys.stderr)
    sys.exit(1)

NOTION_VERSION = "2025-09-03"
NOTION_API_BASE = "https://api.notion.com/v1"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

NEWS_DATABASE_ID = "2ff62df4842180b6944df052819a8872"
TERMS_DATABASE_ID = "2ff62df48421808ea7cbdbd4935f5b6b"

RSS_FEEDS = [
    ("경제", "https://www.hankyung.com/feed/economy"),
    ("국제", "https://www.hankyung.com/feed/international"),
    ("ai", "https://www.hankyung.com/feed/it"),  # IT·과학 → DB 선택지에 맞춰 ai로 매핑
]

# News DB expected properties (names must match Notion exactly)
NEWS_REQUIRED_PROPS = ["게시일", "제목", "작성자", "카테고리", "요약", "url", "용어"]

# Terms DB expected properties
TERMS_REQUIRED_PROPS = ["용어", "의미", "관련 기사"]

# Notion select allowed values for 카테고리
NEWS_CATEGORY_ALLOWED = {"경제", "국제", "ai", "cj"}

# Basic HTTP timeouts/retries
HTTP_TIMEOUT = 30
NOTION_RETRY = 4
OPENAI_RETRY = 3


# -----------------------------
# Utilities
# -----------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compact(s: str, limit: int = 5000) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    if len(s) > limit:
        return s[:limit] + "…"
    return s


def _safe_get(d: Dict[str, Any], *keys: str, default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _dedupe_preserve(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _parse_rfc822_date(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        dt = parsedate_to_datetime(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _to_date_iso(dt: Optional[datetime]) -> str:
    if not dt:
        return datetime.now(timezone.utc).date().isoformat()
    return dt.date().isoformat()


def _sleep_backoff(attempt: int):
    # 0,1,2,3 -> 0.6, 1.2, 2.4, 4.8 (+ jitter)
    base = 0.6 * (2 ** attempt)
    time.sleep(min(6.0, base) + (0.05 * attempt))


# -----------------------------
# Notion client (data_sources-first)
# -----------------------------


class NotionHTTPError(RuntimeError):
    pass


def notion_request(method: str, path: str, body: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{NOTION_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    last_err = None
    for attempt in range(NOTION_RETRY):
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=body if body is not None else None,
                params=params if params is not None else None,
                timeout=HTTP_TIMEOUT,
            )
            if 200 <= resp.status_code < 300:
                if resp.text.strip():
                    return resp.json()
                return {}
            # Notion error body is JSON (usually)
            try:
                err_json = resp.json()
            except Exception:
                err_json = {"status": resp.status_code, "text": resp.text}

            # Retry on rate limit / transient
            if resp.status_code in (429, 500, 502, 503, 504):
                last_err = err_json
                _sleep_backoff(attempt)
                continue

            raise NotionHTTPError(f"Notion API error {resp.status_code}: {json.dumps(err_json, ensure_ascii=False)}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = {"error": str(e)}
            _sleep_backoff(attempt)
        except Exception:
            raise

    raise NotionHTTPError(f"Notion API failed after retries: {json.dumps(last_err, ensure_ascii=False)}")


def notion_retrieve_database(database_id: str) -> Dict[str, Any]:
    return notion_request("GET", f"/databases/{database_id}")


def notion_retrieve_data_source(data_source_id: str) -> Dict[str, Any]:
    return notion_request("GET", f"/data_sources/{data_source_id}")


def notion_query_data_source(
    data_source_id: str,
    *,
    filter_obj: Optional[Dict[str, Any]] = None,
    sorts: Optional[List[Dict[str, Any]]] = None,
    page_size: int = 10,
    start_cursor: Optional[str] = None,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {"page_size": page_size}
    if filter_obj:
        body["filter"] = filter_obj
    if sorts:
        body["sorts"] = sorts
    if start_cursor:
        body["start_cursor"] = start_cursor
    return notion_request("POST", f"/data_sources/{data_source_id}/query", body)


def notion_create_page(parent_data_source_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    body = {
        "parent": {"data_source_id": parent_data_source_id},
        "properties": properties,
    }
    return notion_request("POST", "/pages", body)


def notion_update_page(page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    body = {"properties": properties}
    return notion_request("PATCH", f"/pages/{page_id}", body)


def notion_retrieve_page(page_id: str) -> Dict[str, Any]:
    return notion_request("GET", f"/pages/{page_id}")


def resolve_data_source_id_by_schema(database_id: str, required_prop_names: List[str]) -> str:
    """
    DB가 여러 data source를 가질 수 있으므로:
    1) database.retrieve로 data_sources 목록 획득
    2) 각 data_source.retrieve로 properties(schema) 확인
    3) required_prop_names를 모두 포함하는 data_source_id 선택
    """
    db = notion_retrieve_database(database_id)
    data_sources = db.get("data_sources") or []
    if not data_sources:
        raise NotionHTTPError(f"Database has no data_sources array. database_id={database_id}")

    candidates: List[Tuple[str, str]] = []
    for ds in data_sources:
        ds_id = ds.get("id")
        ds_name = ds.get("name") or ""
        if ds_id:
            candidates.append((ds_id, ds_name))

    if len(candidates) == 1:
        # 단일이면 그대로 사용하되, 스키마 검증은 수행
        ds_id, ds_name = candidates[0]
        ds_obj = notion_retrieve_data_source(ds_id)
        ds_props = (ds_obj.get("properties") or {}).keys()
        missing = [p for p in required_prop_names if p not in ds_props]
        if missing:
            raise NotionHTTPError(
                f"Single data_source found but schema mismatch. data_source={ds_name}({ds_id}), missing={missing}"
            )
        return ds_id

    # multiple: schema match
    scored: List[Tuple[int, str, str, List[str]]] = []
    for ds_id, ds_name in candidates:
        ds_obj = notion_retrieve_data_source(ds_id)
        ds_props = set((ds_obj.get("properties") or {}).keys())
        missing = [p for p in required_prop_names if p not in ds_props]
        score = len(required_prop_names) - len(missing)
        scored.append((score, ds_id, ds_name, missing))

    # pick best full match first
    full = [x for x in scored if x[0] == len(required_prop_names)]
    if len(full) == 1:
        return full[0][1]
    if len(full) > 1:
        # 여러 개가 완전 일치면 이름이 database 이름과 유사한 것을 우선 (그래도 안전하게 첫 번째)
        full_sorted = sorted(full, key=lambda t: (len(t[2]), t[2]))
        return full_sorted[0][1]

    # no full match: fail with diagnostics
    scored_sorted = sorted(scored, key=lambda t: (-t[0], t[2]))
    diag = [
        {"data_source_id": ds_id, "name": ds_name, "matched": score, "missing": missing}
        for score, ds_id, ds_name, missing in scored_sorted
    ]
    raise NotionHTTPError(
        "Could not resolve a data_source_id by schema. "
        f"database_id={database_id}, required={required_prop_names}, candidates={json.dumps(diag, ensure_ascii=False)}"
    )


# -----------------------------
# RSS fetch/parse (stdlib only)
# -----------------------------


@dataclass
class RSSItem:
    title: str
    link: str
    published: Optional[datetime]
    author: str
    category: str  # mapped to Notion select
    description: str


def fetch_rss(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="replace")


def parse_rss(xml_text: str, category: str) -> List[RSSItem]:
    """
    한경 RSS는 item/title, item/link, item/pubDate, item/description 등을 제공.
    author는 dc:creator/author 등에서 최대한 추출.
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []

    # namespaces handling
    ns = {
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    items: List[RSSItem] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = _parse_rfc822_date((item.findtext("pubDate") or "").strip())
        desc = (item.findtext("description") or "").strip()

        author = (item.findtext("author") or "").strip()
        if not author:
            author = (item.findtext("dc:creator", namespaces=ns) or "").strip()

        # HTML 태그 제거(가벼운 수준)
        desc_plain = re.sub(r"<[^>]+>", " ", desc)
        desc_plain = _compact(desc_plain, 1200)

        if title and link:
            items.append(
                RSSItem(
                    title=_compact(title, 300),
                    link=link,
                    published=pub,
                    author=_compact(author, 100),
                    category=category,
                    description=desc_plain,
                )
            )
    return items


# -----------------------------
# OpenAI summarize/terms (optional)
# -----------------------------


class OpenAIHTTPError(RuntimeError):
    pass


def openai_chat_json(prompt: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise OpenAIHTTPError("OPENAI_API_KEY not set")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "You are a precise assistant that returns ONLY valid JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "response_format": {"type": "json_object"},
    }

    last_err = None
    for attempt in range(OPENAI_RETRY):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
            if 200 <= resp.status_code < 300:
                data = resp.json()
                content = _safe_get(data, "choices", 0, "message", "content", default="") or ""
                try:
                    return json.loads(content)
                except Exception:
                    raise OpenAIHTTPError(f"OpenAI returned non-JSON content: {content}")

            try:
                last_err = resp.json()
            except Exception:
                last_err = {"status": resp.status_code, "text": resp.text}

            if resp.status_code in (429, 500, 502, 503, 504):
                _sleep_backoff(attempt)
                continue

            raise OpenAIHTTPError(f"OpenAI error {resp.status_code}: {json.dumps(last_err, ensure_ascii=False)}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = {"error": str(e)}
            _sleep_backoff(attempt)

    raise OpenAIHTTPError(f"OpenAI failed after retries: {json.dumps(last_err, ensure_ascii=False)}")


def summarize_and_extract_terms(title: str, url: str, snippet: str, category: str) -> Tuple[str, List[str]]:
    """
    returns: (summary, [term1, term2])
    """
    # Fallback (no OpenAI)
    if not OPENAI_API_KEY:
        summary = _compact(snippet, 300)
        if not summary:
            summary = _compact(title, 300)
        # very naive term guess: pick 2 keywords-like tokens
        toks = re.findall(r"[A-Za-z가-힣0-9·\-]{2,}", f"{title} {snippet}")
        toks = [t for t in toks if len(t) >= 2]
        toks = _dedupe_preserve(toks)
        terms = (toks[:2] if len(toks) >= 2 else (toks + [""] * 2)[:2])
        terms = [t.strip() for t in terms if t.strip()]
        if len(terms) < 2:
            terms = (terms + ["핵심용어"])[:2]
        return summary, terms[:2]

    prompt = f"""
다음은 한국경제 RSS 기사 정보다.

- 카테고리: {category}
- 제목: {title}
- URL: {url}
- RSS 요약/설명(참고): {snippet}

요구사항:
1) 기사 내용을 추정해 2~3문장 한국어 요약을 작성한다(과장 금지, 불확실하면 "~로 전해졌다"처럼 표현).
2) 기사에서 중요한 "핵심 용어" 2개를 뽑는다.
   - 사람 이름만 2개 뽑는 것 금지
   - 너무 일반적인 단어(예: 경제, 국제, 뉴스) 금지
   - 가능한 한 명사/개념/기업/정책/기술 키워드

아래 JSON 형식으로만 답해라:
{{
  "summary": "요약(2~3문장)",
  "terms": ["용어1", "용어2"]
}}
""".strip()

    out = openai_chat_json(prompt)
    summary = _compact(str(out.get("summary", "") or ""), 600)
    terms_raw = out.get("terms") or []
    if not isinstance(terms_raw, list):
        terms_raw = []
    terms = []
    for t in terms_raw:
        t = _compact(str(t or ""), 60)
        if t:
            terms.append(t)
    terms = _dedupe_preserve(terms)[:2]
    while len(terms) < 2:
        terms.append("핵심용어")
    if not summary:
        summary = _compact(snippet, 300) or _compact(title, 300)
    return summary, terms[:2]


# -----------------------------
# Notion property builders
# -----------------------------


def prop_title(text: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": text or ""}}]}


def prop_rich_text(text: str) -> Dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": text or ""}}]}


def prop_date(date_iso: str) -> Dict[str, Any]:
    return {"date": {"start": date_iso}}


def prop_select(name: str) -> Dict[str, Any]:
    return {"select": {"name": name}}


def prop_url(u: str) -> Dict[str, Any]:
    return {"url": u}


def prop_relation(ids: List[str]) -> Dict[str, Any]:
    return {"relation": [{"id": i} for i in ids]}


# -----------------------------
# Notion operations for this pipeline
# -----------------------------


def notion_find_news_by_url(news_data_source_id: str, url: str) -> Optional[str]:
    filter_obj = {"property": "url", "url": {"equals": url}}
    res = notion_query_data_source(news_data_source_id, filter_obj=filter_obj, page_size=1)
    results = res.get("results") or []
    if results:
        return results[0].get("id")
    return None


def notion_find_term_page(term_data_source_id: str, term: str) -> Optional[str]:
    # title property filter
    filter_obj = {"property": "용어", "title": {"equals": term}}
    res = notion_query_data_source(term_data_source_id, filter_obj=filter_obj, page_size=1)
    results = res.get("results") or []
    if results:
        return results[0].get("id")
    return None


def notion_get_existing_relation_ids(page_obj: Dict[str, Any], relation_prop_name: str) -> List[str]:
    rel = _safe_get(page_obj, "properties", relation_prop_name, "relation", default=[])
    if not isinstance(rel, list):
        return []
    ids = []
    for x in rel:
        pid = x.get("id") if isinstance(x, dict) else None
        if pid:
            ids.append(pid)
    return _dedupe_preserve(ids)


def upsert_term_and_link(
    term_data_source_id: str,
    term: str,
    news_page_id: str,
):
    existing_id = notion_find_term_page(term_data_source_id, term)
    if not existing_id:
        props = {
            "용어": prop_title(term),
            "의미": prop_rich_text(""),
            "관련 기사": prop_relation([news_page_id]),
        }
        created = notion_create_page(term_data_source_id, props)
        print(f"TERM created: {term} -> {created.get('id')}")
        return

    # update relation (merge)
    page = notion_retrieve_page(existing_id)
    existing_rel = notion_get_existing_relation_ids(page, "관련 기사")
    if news_page_id in existing_rel:
        print(f"TERM exists (already linked): {term} -> {existing_id}")
        return

    merged = existing_rel + [news_page_id]
    notion_update_page(existing_id, {"관련 기사": prop_relation(merged)})
    print(f"TERM updated (linked): {term} -> {existing_id}")


# -----------------------------
# Main pipeline
# -----------------------------


def main():
    print(f"[{_now_utc_iso()}] Start pipeline")

    # 1) Resolve data_source_id (schema-based) to avoid multi data source errors
    news_ds_id = resolve_data_source_id_by_schema(NEWS_DATABASE_ID, NEWS_REQUIRED_PROPS)
    term_ds_id = resolve_data_source_id_by_schema(TERMS_DATABASE_ID, TERMS_REQUIRED_PROPS)
    print(f"Resolved news_data_source_id={news_ds_id}")
    print(f"Resolved term_data_source_id={term_ds_id}")

    # 2) Fetch RSS items from 3 feeds
    all_items: List[RSSItem] = []
    for cat, feed_url in RSS_FEEDS:
        if cat not in NEWS_CATEGORY_ALLOWED:
            print(f"Skip feed (invalid category mapping): {cat} {feed_url}")
            continue
        xml_text = fetch_rss(feed_url)
        items = parse_rss(xml_text, cat)
        all_items.extend(items)

    # sort by published desc, fallback to now
    all_items.sort(key=lambda x: x.published or datetime.now(timezone.utc), reverse=True)

    # 3) Pick latest 3 unique links
    picked: List[RSSItem] = []
    seen_links = set()
    for it in all_items:
        if it.link in seen_links:
            continue
        seen_links.add(it.link)
        picked.append(it)
        if len(picked) >= 3:
            break

    print(f"Picked {len(picked)} items")

    # 4) For each, dedupe by url in Notion and insert
    for it in picked:
        try:
            existing_news_id = notion_find_news_by_url(news_ds_id, it.link)
            if existing_news_id:
                print(f"NEWS exists, skip: {it.link} ({existing_news_id})")
                continue

            summary, terms = summarize_and_extract_terms(
                title=it.title,
                url=it.link,
                snippet=it.description,
                category=it.category,
            )
            term_str = ", ".join(terms[:2])

            publish_date_iso = _to_date_iso(it.published)

            # Create news page
            news_props = {
                "게시일": prop_date(publish_date_iso),
                "제목": prop_title(it.title),
                "작성자": prop_rich_text(it.author),
                "카테고리": prop_select(it.category),
                "요약": prop_rich_text(summary),
                "url": prop_url(it.link),
                "용어": prop_rich_text(term_str),
            }

            created_news = notion_create_page(news_ds_id, news_props)
            news_page_id = created_news.get("id")
            if not news_page_id:
                raise NotionHTTPError(f"News page create returned no id: {created_news}")

            print(f"NEWS created: {it.title} -> {news_page_id}")

            # Upsert terms and link relation
            for t in terms[:2]:
                t = (t or "").strip()
                if not t:
                    continue
                upsert_term_and_link(term_ds_id, t, news_page_id)

        except Exception as e:
            print("ERROR processing item:", it.link, file=sys.stderr)
            print(str(e), file=sys.stderr)
            traceback.print_exc()
            # continue next item

    print(f"[{_now_utc_iso()}] Done")


if __name__ == "__main__":
    main()
