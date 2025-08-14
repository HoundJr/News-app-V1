#!/usr/bin/env python3
import json, os, re, hashlib, time, sys, traceback
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import feedparser
import yaml
from dateutil import parser as dtparser
import pytz

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
SOURCES_FILE = os.path.join(ROOT, "sources.yaml")

USER_AGENT = "AusGovAnnouncementsBot/0.1 (+github)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-AU,en;q=0.9"})

def load_sources():
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    tzname = doc.get("timezone", "Australia/Brisbane")
    tz = pytz.timezone(tzname)
    return tz, doc.get("sources", [])

def strip_ws(s):
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def make_id(url):
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def to_iso(dt, tz):
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    return dt.astimezone(tz).isoformat()

def find_feed_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    feeds = []
    for link in soup.find_all("link", attrs={"rel": ["alternate", "ALTERNATE"]}):
        t = (link.get("type") or "").lower()
        if "rss" in t or "atom" in t or "xml" in t:
            href = link.get("href")
            if href:
                feeds.append(urljoin(base_url, href))
    # Some sites only expose feed in <a>
    for a in soup.find_all("a"):
        href = a.get("href","")
        if href and any(x in href.lower() for x in ["/feed", "rss", "atom", ".xml"]):
            feeds.append(urljoin(base_url, href))
    # de-dup preserving order
    seen = set()
    uniq = []
    for u in feeds:
        if u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

def parse_datetime_guess(v, tz):
    if not v:
        return None
    try:
        dt = dtparser.parse(v)
        if not dt.tzinfo:
            dt = tz.localize(dt)
        return dt.astimezone(tz)
    except Exception:
        return None

def within_today_brisbane(dt, tz):
    now = datetime.now(tz)
    start = tz.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    end = start + timedelta(days=1)
    return (dt >= start) and (dt < end)

def clean_url(u):
    # Strip fragments/tracking
    if not u: return u
    parsed = urlparse(u)
    clean = parsed._replace(query="", fragment="")
    return clean.geturl()

def fetch_url(url, timeout=20):
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r

def scrape_items_from_page(base_url, html, selector):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for el in soup.select(selector or "a"):
        href = el.get("href")
        title = strip_ws(el.get_text())
        if not href or not title: 
            continue
        url = urljoin(base_url, href)
        items.append({
            "title": title,
            "url": clean_url(url),
            "summary": "",
            "published_at": None,  # unknown at page-list level
        })
    return items

def fetch_source(src, tz):
    name = src["name"]
    home = src["homepage"]
    selector = src.get("selector")

    try:
        # 1) load homepage
        resp = fetch_url(home)
        html = resp.text

        # 2) try to find RSS/Atom
        feed_links = find_feed_links(html, home)
        if feed_links:
            items = []
            for feed_url in feed_links[:2]:  # try first 1-2 feeds max
                parsed = feedparser.parse(feed_url)
                for e in parsed.entries[:50]:
                    url = clean_url(e.get("link") or "")
                    if not url: 
                        continue
                    title = strip_ws(e.get("title") or "")
                    if not title:
                        continue
                    # published date guess
                    dt = None
                    for key in ("published", "updated", "created"):
                        dt = parse_datetime_guess(e.get(key), tz)
                        if dt: break
                    summary = strip_ws(BeautifulSoup(e.get("summary",""), "html.parser").get_text()) if e.get("summary") else ""
                    items.append({
                        "title": title,
                        "url": url,
                        "summary": summary,
                        "published_at": to_iso(dt, tz) if dt else None
                    })
            return name, items, None
        else:
            # 3) fall back to simple page scrape
            items = scrape_items_from_page(home, html, selector)
            return name, items, None
    except Exception as e:
        return name, [], f"{type(e).__name__}: {e}"

def normalize_and_filter(all_items, tz):
    # Deduplicate by URL, keep the first occurrence, and filter to "today" (if date known).
    by_url = {}
    today = []
    now = datetime.now(tz)
    start = tz.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    end = start + timedelta(days=1)

    for it in all_items:
        url = it["url"]
        if not url: 
            continue
        if url in by_url:
            continue
        by_url[url] = True

        # If no published_at, include it (we'll show without time).
        if it["published_at"]:
            try:
                dt = dtparser.parse(it["published_at"])
                dt = dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
            except Exception:
                dt = None
        else:
            dt = None

        if (dt is None) or (start <= dt < end):
            today.append(it)

    return today

def generate_unified_rss(items, tz, site_title="AU Gov Announcements (Daily)"):
    # Extremely small RSS 2.0 writer (no extra deps)
    from xml.sax.saxutils import escape
    now = datetime.now(tz).strftime("%a, %d %b %Y %H:%M:%S %z")
    rss = []
    rss.append('<?xml version="1.0" encoding="UTF-8"?>')
    rss.append('<rss version="2.0">')
    rss.append("<channel>")
    rss.append(f"<title>{escape(site_title)}</title>")
    rss.append(f"<link>https://example.com/</link>")
    rss.append(f"<description>Unified feed generated daily</description>")
    rss.append(f"<lastBuildDate>{now}</lastBuildDate>")

    for it in items[:200]:
        title = escape(it["title"] or "")
        link = escape(it["url"] or "")
        desc = escape(it.get("summary","") or "")
        pub = it.get("published_at")
        if pub:
            # RSS date format
            dt = dtparser.parse(pub)
            dt = dt.astimezone(tz) if dt.tzinfo else tz.localize(dt)
            pubDate = dt.strftime("%a, %d %b %Y %H:%M:%S %z")
        else:
            pubDate = now
        guid = make_id(it["url"] or (it["title"]+pubDate))
        rss.append("<item>")
        rss.append(f"<title>{title}</title>")
        rss.append(f"<link>{link}</link>")
        rss.append(f"<guid isPermaLink='false'>{guid}</guid>")
        rss.append(f"<pubDate>{pubDate}</pubDate>")
        if desc:
            rss.append(f"<description>{desc}</description>")
        rss.append("</item>")
    rss.append("</channel>")
    rss.append("</rss>")
    return "\n".join(rss)

def main():
    tz, sources = load_sources()
    errors = []
    collected = []

    for src in sources:
        name, items, err = fetch_source(src, tz)
        if err:
            errors.append({"source": name, "error": err})
        for it in items:
            it["source"] = name
            it["url"] = clean_url(it["url"])
            it["title"] = strip_ws(it["title"])
            it["summary"] = strip_ws(it.get("summary",""))
        collected.extend(items)

    today_items = normalize_and_filter(collected, tz)

    # Sort by datetime desc if available, else title
    def sort_key(it):
        dt = it.get("published_at")
        if dt:
            try:
                d = dtparser.parse(dt)
                return d
            except Exception:
                return datetime.fromtimestamp(0)
        return datetime.fromtimestamp(0)
    today_items.sort(key=sort_key, reverse=True)

    # Write JSON
    now = datetime.now(tz)
    date_str = now.strftime("%Y-%m-%d")
    daily_path = os.path.join(DATA_DIR, f"{date_str}.json")
    latest_path = os.path.join(DATA_DIR, "latest.json")

    payload = {
        "date": date_str,
        "timezone": str(tz),
        "generated_at": to_iso(now, tz),
        "count": len(today_items),
        "items": today_items,
        "errors": errors
    }

    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Unified RSS
    rss_text = generate_unified_rss(today_items, tz)
    with open(os.path.join(DATA_DIR, "unified.xml"), "w", encoding="utf-8") as f:
        f.write(rss_text)

    print(f"Wrote {len(today_items)} items to {daily_path} and latest.json")
    if errors:
        print("Some sources failed or partially parsed:")
        for e in errors:
            print(" -", e["source"], "=>", e["error"])

if __name__ == "__main__":
    main()
