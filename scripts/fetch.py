#!/usr/bin/env python3
# scripts/fetch.py  — faster via (a) content cache (b) small parallel fetch

import json, os, re, hashlib, time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import feedparser
import yaml
from dateutil import parser as dtparser
import pytz
import trafilatura
from urllib import robotparser as urobot
from readability import Document

# ------------ Tunables (adjust if needed) -------------------------------------
MAX_ARTICLES_TOTAL = 120           # hard cap of items considered after filtering
MAX_NEW_FETCHES = 30               # max number of *new* URLs to fetch per run
MAX_WORKERS = 5                    # parallel content fetchers (be polite)
FETCH_TIMEOUT = 20
CACHE_MAX_ENTRIES = 3000           # size cap of content cache
CACHE_STALE_DAYS = 14              # re-fetch if older than this
# -----------------------------------------------------------------------------

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "web", "data")
os.makedirs(DATA_DIR, exist_ok=True)
SOURCES_FILE = os.path.join(ROOT, "sources.yaml")
CACHE_FILE = os.path.join(DATA_DIR, "content_cache.json")

USER_AGENT = "AusGovAnnouncementsBot/0.5 (+github)"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9"
})

ROBOTS_CACHE = {}

def robots_can_fetch(url: str) -> bool:
    try:
        p = urlparse(url); base = f"{p.scheme}://{p.netloc}"
        rp = ROBOTS_CACHE.get(base)
        if rp is None:
            rp = urobot.RobotFileParser()
            rp.set_url(urljoin(base, "/robots.txt"))
            try: rp.read()
            except Exception:
                ROBOTS_CACHE[base] = None
                return True
            ROBOTS_CACHE[base] = rp
        if rp is None: return True
        return rp.can_fetch(USER_AGENT, p.path or "/")
    except Exception:
        return True

def load_sources():
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    tzname = doc.get("timezone", "Australia/Brisbane")
    tz = pytz.timezone(tzname)
    return tz, doc.get("sources", [])

def strip_ws(s): return re.sub(r"\s+", " ", str(s or "")).strip()
def make_id(url): return hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]
def to_iso(dt, tz):
    if dt is None: return None
    if dt.tzinfo is None: dt = tz.localize(dt)
    return dt.astimezone(tz).isoformat()
def clean_url(u):
    if not u: return u
    p = urlparse(u)
    return p._replace(query="", fragment="").geturl()

def fetch_url(url, timeout=FETCH_TIMEOUT):
    # Use a plain GET per call to avoid threading issues with one Session
    return requests.get(url, headers=SESSION.headers, timeout=timeout)

def find_feed_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    feeds = []
    for link in soup.find_all("link"):
        rel = link.get("rel")
        rel = (rel[0].lower() if isinstance(rel, list) and rel else (rel or "")).lower()
        t = (link.get("type") or "").lower()
        if "alternate" in rel and ("rss" in t or "atom" in t or "xml" in t):
            href = link.get("href")
            if href: feeds.append(urljoin(base_url, href))
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if href and any(x in href.lower() for x in ["/feed", "rss", "atom", ".xml"]):
            feeds.append(urljoin(base_url, href))
    seen, uniq = set(), []
    for u in feeds:
        if u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

def scrape_items_from_page(base_url, html, selector):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for el in soup.select(selector or "a"):
        href = el.get("href"); title = strip_ws(el.get_text())
        if not href or not title: continue
        url = urljoin(base_url, href)
        items.append({"title": title, "url": clean_url(url), "summary": "", "published_at": None})
    return items

def parse_entry_datetime(entry, tz):
    for key in ("published", "updated", "created"):
        v = entry.get(key)
        if not v: continue
        try:
            dt = dtparser.parse(v)
            if dt.tzinfo is None: dt = tz.localize(dt)
            return dt.astimezone(tz)
        except Exception:
            continue
    return None

def fetch_source(src, tz):
    name = src.get("name", "Unknown Source")
    feed_override = src.get("feed")
    homepage = src.get("homepage")
    selector = src.get("selector")

    try:
        if feed_override:
            items = []
            parsed = feedparser.parse(feed_override)
            for e in parsed.entries[:80]:
                url = clean_url(e.get("link") or ""); 
                if not url: continue
                title = strip_ws(e.get("title") or ""); 
                if not title: continue
                dt = parse_entry_datetime(e, tz)
                summary_html = e.get("summary") or e.get("description") or ""
                summary = strip_ws(BeautifulSoup(summary_html, "html.parser").get_text())
                items.append({
                    "title": title, "url": url, "summary": summary,
                    "published_at": to_iso(dt, tz) if dt else None
                })
            return name, items, None

        if not homepage:
            return name, [], "No feed or homepage provided"

        resp = fetch_url(homepage); resp.raise_for_status()
        html = resp.text
        feed_links = find_feed_links(html, homepage)
        if feed_links:
            items = []
            for feed_url in feed_links[:2]:
                parsed = feedparser.parse(feed_url)
                for e in parsed.entries[:80]:
                    url = clean_url(e.get("link") or "")
                    if not url: continue
                    title = strip_ws(e.get("title") or "")
                    if not title: continue
                    dt = parse_entry_datetime(e, tz)
                    summary_html = e.get("summary") or e.get("description") or ""
                    summary = strip_ws(BeautifulSoup(summary_html, "html.parser").get_text())
                    items.append({
                        "title": title, "url": url, "summary": summary,
                        "published_at": to_iso(dt, tz) if dt else None
                    })
            return name, items, None

        items = scrape_items_from_page(homepage, html, selector)
        return name, items, None

    except Exception as e:
        return name, [], f"{type(e).__name__}: {e}"

# ---- HTML pruning (same as before) -------------------------------------------
REMOVE_TAGS = {"script","style","noscript","template","iframe","canvas","svg",
               "form","input","button","select","textarea","label","nav","header","footer","aside"}
ROLE_BLOCKLIST = {"navigation","banner","contentinfo","complementary","search","menu","menubar","toolbar","dialog","alert","alertdialog"}
CLASS_PAT = re.compile(r"(breadcrumb|nav|menu|header|footer|sidebar|share|social|subscribe|pagination|toolbar|skip|cookie|consent|related|widget)", re.I)

def absolutize_links(soup: BeautifulSoup, base_url: str):
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#"):
            a.decompose(); continue
        a["href"] = urljoin(base_url, href)

def prune_html(html: str, base_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in list(REMOVE_TAGS):
        for el in soup.find_all(t): el.decompose()
    for el in soup.find_all(attrs={"role": True}):
        role = str(el.get("role","")).lower()
        if role in ROLE_BLOCKLIST: el.decompose()
    for el in soup.find_all(True, class_=True):
        classes = " ".join([c for c in el.get("class", []) if isinstance(c,str)])
        if classes and CLASS_PAT.search(classes): el.decompose()
    for el in soup.find_all(["ul","ol","div"]):
        links = el.find_all("a")
        text = strip_ws(el.get_text(" ", strip=True))
        if len(links) >= 10 or (len(links) >= 5 and len(text) < 200):
            el.decompose()
    absolutize_links(soup, base_url)
    for img in soup.find_all("img"): img.decompose()
    cleaned = strip_ws(soup.get_text(" ", strip=False))
    if len(cleaned) < 120: return ""
    return str(soup)

def extract_main_content(html: str, url: str) -> str:
    try:
        out = trafilatura.extract(html, url=url, include_images=False, include_tables=True, favor_recall=True, output_format="html")
        if out:
            pruned = prune_html(out, url)
            if pruned: return pruned
    except Exception: pass
    try:
        doc = Document(html)
        article_html = doc.summary(html_partial=True)
        if article_html:
            pruned = prune_html(article_html, url)
            if pruned: return pruned
    except Exception: pass
    try:
        soup = BeautifulSoup(html, "html.parser")
        candidates = soup.select("article") or soup.select("main")
        for node in candidates:
            pruned = prune_html(str(node), url)
            if pruned: return pruned
    except Exception: pass
    return ""

def truncate_html(html: str, max_chars: int = 8000) -> str:
    if not html: return ""
    return html if len(html) <= max_chars else (html[:max_chars] + "…")

# ---- Content cache -----------------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE): return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(cache: dict):
    # trim if too large (keep most recent by fetched_at)
    if len(cache) > CACHE_MAX_ENTRIES:
        items = sorted(cache.items(), key=lambda kv: kv[1].get("fetched_at",""), reverse=True)[:CACHE_MAX_ENTRIES]
        cache = dict(items)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    return cache

def cache_get(cache, url, tz):
    entry = cache.get(url)
    if not entry: return None
    fetched_at = entry.get("fetched_at")
    try:
        dt = dtparser.parse(fetched_at) if fetched_at else None
    except Exception:
        dt = None
    if dt and (datetime.now(tz) - dt) <= timedelta(days=CACHE_STALE_DAYS):
        return entry.get("content_html")
    return None

def cache_put(cache, url, content_html, tz):
    cache[url] = {"content_html": content_html, "fetched_at": datetime.now(tz).isoformat()}

# ---- Filtering (last 24h + undated) -----------------------------------------
def normalize_and_filter(all_items, tz):
    seen = set(); deduped = []
    for it in all_items:
        url = it.get("url")
        if not url or url in seen: continue
        seen.add(url)
        it["title"] = strip_ws(it.get("title"))
        it["summary"] = strip_ws(it.get("summary"))
        deduped.append(it)

    now = datetime.now(tz); cutoff = now - timedelta(hours=24)
    keep = []
    for it in deduped:
        iso = it.get("published_at")
        if not iso: keep.append(it); continue
        try:
            dt = dtparser.parse(iso)
            if dt.tzinfo is None: dt = tz.localize(dt)
            dt = dt.astimezone(tz)
        except Exception:
            keep.append(it); continue
        if dt >= cutoff: keep.append(it)
    return keep[:MAX_ARTICLES_TOTAL]

# ---- Main --------------------------------------------------------------------
def main():
    tz, sources = load_sources()
    errors, collected = [], []
    cache = load_cache()

    print(f"Loaded {len(sources)} sources; cache entries: {len(cache)}")

    # Collect items
    for src in sources:
        name, items, err = fetch_source(src, tz)
        if err:
            errors.append({"source": name, "error": err})
            print(f"[WARN] {name}: {err}")
        for it in items: it["source"] = name
        collected.extend(items)

    kept = normalize_and_filter(collected, tz)

    # Decide what needs fetching
    to_fetch = []
    for it in kept:
        url = it.get("url")
        if not url: continue
        cached = cache_get(cache, url, tz)
        if cached:
            it["content_html"] = cached
            if not it.get("summary"):
                text = strip_ws(BeautifulSoup(cached, "html.parser").get_text())
                it["summary"] = text[:280]
        else:
            if robots_can_fetch(url):
                to_fetch.append(it)
    to_fetch = to_fetch[:MAX_NEW_FETCHES]

    # Parallel fetch for new URLs
    def worker(item):
        try:
            r = fetch_url(item["url"]); r.raise_for_status()
            html = extract_main_content(r.text, item["url"])
            if html:
                html = truncate_html(html)
                return (item["url"], html, None)
            return (item["url"], "", None)
        except Exception as e:
            return (item["url"], "", f"{type(e).__name__}: {e}")

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(worker, it): it for it in to_fetch}
            for fut in as_completed(futures):
                u, html, err = fut.result()
                it = futures[fut]
                if err:
                    errors.append({"source": it.get("source","?"), "url": u, "error": err})
                    continue
                if html:
                    it["content_html"] = html
                    # derive summary if missing
                    if not it.get("summary"):
                        text = strip_ws(BeautifulSoup(html, "html.parser").get_text())
                        it["summary"] = text[:280]
                    cache_put(cache, u, html, tz)

    # Sort newest first
    def sort_key(it):
        iso = it.get("published_at")
        if not iso: return datetime.fromtimestamp(0, tz)
        try:
            dt = dtparser.parse(iso)
            if dt.tzinfo is None: dt = tz.localize(dt)
            return dt.astimezone(tz)
        except Exception:
            return datetime.fromtimestamp(0, tz)
    kept.sort(key=sort_key, reverse=True)

    # Write data files
    now = datetime.now(tz)
    date_str = now.strftime("%Y-%m-%d")
    daily_path = os.path.join(DATA_DIR, f"{date_str}.json")
    latest_path = os.path.join(DATA_DIR, "latest.json")
    rss_path = os.path.join(DATA_DIR, "unified.xml")

    payload = {
        "date": date_str,
        "timezone": str(tz),
        "generated_at": to_iso(now, tz),
        "count": len(kept),
        "items": kept,
        "errors": errors
    }

    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Save/update cache (after writing data so a failure here won't block)
    save_cache(cache)

    # Minimal unified RSS
    def rss(items):
        from xml.sax.saxutils import escape
        nowh = now.strftime("%a, %d %b %Y %H:%M:%S %z")
        out = ['<?xml version="1.0" encoding="UTF-8"?>','<rss version="2.0">','<channel>',
               "<title>AU Gov Announcements (Daily)</title>","<link>https://example.com/</link>",
               "<description>Unified feed generated daily</description>",f"<lastBuildDate>{nowh}</lastBuildDate>"]
        for it in items[:200]:
            t = escape(it.get("title") or ""); l = escape(it.get("url") or "")
            d = escape(it.get("summary") or ""); pub = it.get("published_at") or now.isoformat()
            guid = make_id(it.get("url") or (t+pub))
            out += ["<item>",f"<title>{t}</title>",f"<link>{l}</link>",
                    f"<guid isPermaLink='false'>{guid}</guid>",f"<pubDate>{nowh}</pubDate>",
                    f"<description>{d}</description>" if d else "", "</item>"]
        out += ["</channel>","</rss>"]
        return "\n".join(x for x in out if x)
    with open(rss_path, "w", encoding="utf-8") as f:
        f.write(rss(kept))

    print(f"Wrote {len(kept)} items → {latest_path} (new content fetched: {len(to_fetch)})")
    if errors:
        print("Source/content errors:")
        for e in errors:
            print(" -", e)

if __name__ == "__main__":
    main()
