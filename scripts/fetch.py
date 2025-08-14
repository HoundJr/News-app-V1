#!/usr/bin/env python3
# scripts/fetch.py
#
# Fetches announcements, normalises them, and (NEW) extracts full article content.
# Writes JSON and a unified RSS into web/data/ so GitHub Pages can serve them.

import json, os, re, hashlib, time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import feedparser
import yaml
from dateutil import parser as dtparser
import pytz
import trafilatura
from urllib import robotparser as urobot

# --- Paths --------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "web", "data")  # publish under /web
os.makedirs(DATA_DIR, exist_ok=True)
SOURCES_FILE = os.path.join(ROOT, "sources.yaml")

# --- HTTP session --------------------------------------------------------------
USER_AGENT = "AusGovAnnouncementsBot/0.3 (+github)"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9"
})

# --- Robots cache --------------------------------------------------------------
ROBOTS_CACHE = {}  # base -> RobotFileParser

def robots_can_fetch(url: str) -> bool:
    """Respect robots.txt; default to allow if robots not reachable."""
    try:
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        rp = ROBOTS_CACHE.get(base)
        if rp is None:
            rp = urobot.RobotFileParser()
            rp.set_url(urljoin(base, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                ROBOTS_CACHE[base] = None
                return True  # don't block if robots can't be read
            ROBOTS_CACHE[base] = rp
        if rp is None:
            return True
        return rp.can_fetch(USER_AGENT, p.path or "/")
    except Exception:
        return True

# --- Utils ---------------------------------------------------------------------
def load_sources():
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    tzname = doc.get("timezone", "Australia/Brisbane")
    tz = pytz.timezone(tzname)
    return tz, doc.get("sources", [])

def strip_ws(s):
    if not s: return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def make_id(url):
    return hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]

def to_iso(dt, tz):
    if dt is None: return None
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    return dt.astimezone(tz).isoformat()

def clean_url(u):
    if not u: return u
    parsed = urlparse(u)
    clean = parsed._replace(query="", fragment="")
    return clean.geturl()

def fetch_url(url, timeout=25):
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r

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
    # dedup keep order
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

# --- Source fetcher ------------------------------------------------------------
def fetch_source(src, tz):
    """
    Supports:
      - feed: <rss/atom URL>  (preferred)
      - homepage: <URL> [+ selector:]  (auto-detect feeds; else scrape)
    """
    name = src.get("name", "Unknown Source")
    feed_override = src.get("feed")
    homepage = src.get("homepage")
    selector = src.get("selector")

    try:
        if feed_override:
            items = []
            parsed = feedparser.parse(feed_override)
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

        if not homepage:
            return name, [], "No feed or homepage provided"

        resp = fetch_url(homepage)
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

# --- Content extraction (NEW) --------------------------------------------------
def extract_main_content(html: str, url: str) -> str:
    """Return main article HTML (sanitized by trafilatura)."""
    try:
        out = trafilatura.extract(
            html,
            url=url,
            include_images=False,  # keep it text-focused
            include_tables=True,
            favor_recall=True,
            output_format="html"
        )
        if out:
            return out
    except Exception:
        pass

    # Fallback: crude main/article guess
    soup = BeautifulSoup(html, "html.parser")
    candidates = soup.select("article") or soup.select("main") or soup.select('section[class*="content"],div[class*="content"]')
    for node in candidates:
        text = strip_ws(node.get_text(" ", strip=True))
        if text and len(text) > 200:
            return str(node)
    return ""

def truncate_html(html: str, max_chars: int = 8000) -> str:
    if not html: return ""
    return html if len(html) <= max_chars else (html[:max_chars] + "…")

# --- Filtering: last 24h + undated --------------------------------------------
def normalize_and_filter(all_items, tz):
    seen = set(); deduped = []
    for it in all_items:
        url = it.get("url")
        if not url or url in seen: continue
        seen.add(url)
        it["title"] = strip_ws(it.get("title"))
        it["summary"] = strip_ws(it.get("summary"))
        deduped.append(it)

    now = datetime.now(tz)
    cutoff = now - timedelta(hours=24)

    keep = []
    for it in deduped:
        iso = it.get("published_at")
        if not iso:
            keep.append(it); continue
        try:
            dt = dtparser.parse(iso)
            if dt.tzinfo is None: dt = tz.localize(dt)
            dt = dt.astimezone(tz)
        except Exception:
            keep.append(it); continue
        if dt >= cutoff:
            keep.append(it)
    return keep

# --- Unified RSS ---------------------------------------------------------------
def generate_unified_rss(items, tz, site_title="AU Gov Announcements (Daily)"):
    from xml.sax.saxutils import escape
    now = datetime.now(tz).strftime("%a, %d %b %Y %H:%M:%S %z")
    rss = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">', '<channel>',
        f"<title>{escape(site_title)}</title>",
        "<link>https://example.com/</link>",
        "<description>Unified feed generated daily</description>",
        f"<lastBuildDate>{now}</lastBuildDate>",
    ]
    for it in items[:200]:
        title = escape(it.get("title") or "")
        link = escape(it.get("url") or "")
        desc = escape(it.get("summary") or "")
        pub = it.get("published_at")
        if pub:
            try:
                dt = dtparser.parse(pub)
                if dt.tzinfo is None: dt = pytz.UTC.localize(dt)
                pubDate = dt.strftime("%a, %d %b %Y %H:%M:%S %z")
            except Exception:
                pubDate = now
        else:
            pubDate = now
        guid = make_id(it.get("url") or (it.get("title","") + pubDate))
        rss.extend([
            "<item>",
            f"<title>{title}</title>",
            f"<link>{link}</link>",
            f"<guid isPermaLink='false'>{guid}</guid>",
            f"<pubDate>{pubDate}</pubDate>",
            f"<description>{desc}</description>" if desc else "",
            "</item>"
        ])
    rss.extend(["</channel>", "</rss>"])
    return "\n".join(x for x in rss if x)

# --- Main ----------------------------------------------------------------------
def main():
    tz, sources = load_sources()
    errors = []
    collected = []

    print(f"Loaded {len(sources)} sources")

    # Phase 1: collect items
    for src in sources:
        name, items, err = fetch_source(src, tz)
        if err:
            errors.append({"source": name, "error": err})
            print(f"[WARN] {name}: {err}")
        for it in items:
            it["source"] = name
        collected.extend(items)

    # Phase 2: filter (last 24h + undated)
    kept = normalize_and_filter(collected, tz)

    # Phase 3 (NEW): fetch full content for a reasonable number of items
    MAX_ARTICLES = 60  # safety cap per run
    fetched = 0
    for it in kept:
        if fetched >= MAX_ARTICLES:
            break
        url = it.get("url")
        if not url: continue
        if not robots_can_fetch(url):
            continue
        try:
            resp = fetch_url(url, timeout=25)
            content_html = extract_main_content(resp.text, url)
            if content_html:
                content_html = truncate_html(content_html, max_chars=8000)
                it["content_html"] = content_html
                # derive summary if missing
                if not it.get("summary"):
                    text = strip_ws(BeautifulSoup(content_html, "html.parser").get_text())
                    it["summary"] = text[:280]
                fetched += 1
                time.sleep(0.5)  # be polite
        except Exception as e:
            errors.append({"source": it.get("source","?"), "url": url, "error": f"{type(e).__name__}: {e}"})
            continue

    # Sort newest first (if dated)
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

    # Write files
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

    rss_text = generate_unified_rss(kept, tz)
    with open(rss_path, "w", encoding="utf-8") as f:
        f.write(rss_text)

    print(f"Wrote {len(kept)} items → {latest_path} (content fetched for ~{fetched})")
    if errors:
        print("Source/content errors:")
        for e in errors:
            print(" -", e)

if __name__ == "__main__":
    main()
