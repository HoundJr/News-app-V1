#!/usr/bin/env python3
# scripts/fetch.py — fast fetcher with cache + parallel content extraction
# Changes in this version:
#  - Feeds are fetched with requests (custom UA), then parsed from bytes.
#  - Per-source timeouts: "timeout" and "feed_timeout" in sources.yaml.
#  - Recent window is configurable via RECENT_HOURS env var (default 168h).

import json, os, re, hashlib, time, sys, argparse, gc
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

import requests
from bs4 import BeautifulSoup
import feedparser
import yaml
from dateutil import parser as dtparser
import pytz
import trafilatura
from urllib import robotparser as urobot
from readability import Document

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ------------ Tunables ---------------------------------------------------------
MAX_ARTICLES_TOTAL = 120
MAX_NEW_FETCHES    = 30
MAX_WORKERS        = 5                 # be polite to origin sites
FETCH_TIMEOUT      = 20                # default timeout (seconds) unless overridden per source
CACHE_MAX_ENTRIES  = 3000
CACHE_STALE_DAYS   = 14
MEMORY_WARNING_THRESHOLD = 500  # MB
# ------------------------------------------------------------------------------

ROOT         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(ROOT, "web", "data")
os.makedirs(DATA_DIR, exist_ok=True)
SOURCES_FILE = os.path.join(ROOT, "sources.yaml")
CACHE_FILE   = os.path.join(DATA_DIR, "content_cache.json")

USER_AGENT = "AusGovAnnouncementsBot/1.1 (+https://github.com/yourusername/yourrepo)"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9"
})

ROBOTS_CACHE = {}
logger = None  # set in main()

# -------------------- Logging --------------------------------------------------
def setup_logging(debug_mode=False):
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(DATA_DIR, 'fetch.log')),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def _lw(msg):  # warn even if logger not ready
    (logger.warning(msg) if logger else print(f"[WARN] {msg}"))

def _le(msg):  # error even if logger not ready
    (logger.error(msg) if logger else print(f"[ERROR] {msg}"))

def log_memory_usage(stage):
    if not PSUTIL_AVAILABLE:
        return
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        (logger.info if logger else print)(f"Memory usage at {stage}: {memory_mb:.1f} MB")
        if memory_mb > MEMORY_WARNING_THRESHOLD:
            _lw("High memory usage detected, triggering garbage collection")
            gc.collect()
    except Exception as e:
        if logger: logger.debug(f"Memory monitoring failed: {e}")

# -------------------- Helpers --------------------------------------------------
def retry(max_attempts=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    if attempt == max_attempts - 1:
                        _le(f"Failed after {max_attempts} attempts: {e}")
                        raise
                    wait = delay * (2 ** attempt)
                    _lw(f"Attempt {attempt + 1} failed, retrying in {wait}s: {e}")
                    time.sleep(wait)
        return wrapper
    return decorator

def robots_can_fetch(url: str) -> bool:
    try:
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        rp = ROBOTS_CACHE.get(base)
        if rp is None:
            rp = urobot.RobotFileParser()
            rp.set_url(urljoin(base, "/robots.txt"))
            try:
                rp.read()
            except Exception as e:
                if logger: logger.debug(f"Could not read robots.txt for {base}: {e}")
                ROBOTS_CACHE[base] = None
                return True
            ROBOTS_CACHE[base] = rp
        if rp is None:
            return True
        return rp.can_fetch(USER_AGENT, p.path or "/")
    except Exception as e:
        if logger: logger.debug(f"Robots check failed for {url}: {e}")
        return True

def load_sources(sources_file=None):
    path = sources_file or SOURCES_FILE
    if not os.path.exists(path):
        _le(f"Sources file not found: {path}")
        print("Please create a sources.yaml file with your government news sources")
        sys.exit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
    except Exception as e:
        _le(f"Failed to load sources file {path}: {e}")
        sys.exit(1)

    tzname = doc.get("timezone", "Australia/Brisbane")
    try:
        tz = pytz.timezone(tzname)
    except pytz.UnknownTimeZoneError:
        _lw(f"Unknown timezone {tzname}, using Australia/Brisbane")
        tz = pytz.timezone("Australia/Brisbane")

    sources = doc.get("sources", [])
    if logger: logger.info(f"Loaded {len(sources)} sources from {path}")
    return tz, sources

def strip_ws(s): 
    return re.sub(r"\s+", " ", str(s or "")).strip()

def make_id(url): 
    return hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]

def to_iso(dt, tz):
    if dt is None: 
        return None
    if dt.tzinfo is None: 
        dt = tz.localize(dt)
    return dt.astimezone(tz).isoformat()

def clean_url(u):
    if not u: 
        return u
    p = urlparse(u)
    return p._replace(query="", fragment="").geturl()

def format_rss_date(iso_date, tz):
    if not iso_date:
        return datetime.now(tz).strftime("%a, %d %b %Y %H:%M:%S %z")
    try:
        dt = dtparser.parse(iso_date)
        if dt.tzinfo is None:
            dt = tz.localize(dt)
        return dt.strftime("%a, %d %b %Y %H:%M:%S %z")
    except Exception as e:
        if logger: logger.debug(f"Date parsing failed for {iso_date}: {e}")
        return datetime.now(tz).strftime("%a, %d %b %Y %H:%M:%S %z")

@retry(max_attempts=3)
def fetch_url(url, timeout=FETCH_TIMEOUT):
    return requests.get(url, headers=SESSION.headers, timeout=timeout)

def find_feed_links(html, base_url):
    try:
        soup = BeautifulSoup(html, "html.parser")
        feeds = []
        for link in soup.find_all("link"):
            rel = link.get("rel")
            rel = (rel[0].lower() if isinstance(rel, list) and rel else (rel or "")).lower()
            t = (link.get("type") or "").lower()
            if "alternate" in rel and ("rss" in t or "atom" in t or "xml" in t):
                href = link.get("href")
                if href:
                    feeds.append(urljoin(base_url, href))
        for a in soup.find_all("a"):
            href = a.get("href", "")
            if href and any(x in href.lower() for x in ["/feed", "rss", "atom", ".xml"]):
                feeds.append(urljoin(base_url, href))
        seen, unique = set(), []
        for u in feeds:
            if u not in seen:
                unique.append(u); seen.add(u)
        if logger: logger.debug(f"Found {len(unique)} potential feeds for {base_url}")
        return unique
    except Exception as e:
        _le(f"Error finding feed links in {base_url}: {e}")
        return []

def scrape_items_from_page(base_url, html, selector):
    try:
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for el in soup.select(selector or "a"):
            href = el.get("href")
            title = strip_ws(el.get_text())
            if not href or not title:
                continue
            url = urljoin(base_url, href)
            items.append({"title": title, "url": clean_url(url), "summary": "", "published_at": None})
        if logger: logger.debug(f"Scraped {len(items)} items from {base_url}")
        return items
    except Exception as e:
        _le(f"Error scraping items from {base_url}: {e}")
        return []

def parse_entry_datetime(entry, tz):
    for key in ("published", "updated", "created"):
        v = entry.get(key)
        if not v:
            continue
        try:
            dt = dtparser.parse(v)
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            return dt.astimezone(tz)
        except Exception as e:
            if logger: logger.debug(f"Failed to parse date {v} from key {key}: {e}")
            continue
    return None

def _parse_feed_with_headers(url, timeout, ua=None, extra_headers=None):
    resp = _get(url, timeout=timeout, ua=ua, extra_headers=extra_headers)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def fetch_source(src, tz):
    name = src.get("name", "Unknown Source")
    feed_override = src.get("feed")
    homepage = src.get("homepage")
    selector = src.get("selector")
    src_timeout = int(src.get("timeout", FETCH_TIMEOUT))
    feed_timeout = int(src.get("feed_timeout", src_timeout))
    src_ua = src.get("user_agent")
    src_headers = src.get("headers", {})

    if logger: logger.info(f"Processing source: {name}")

    try:
        # Preferred: explicit feed
        if feed_override:
            if logger: logger.debug(f"Using direct feed: {feed_override}")
            items = []
            parsed = _parse_feed_with_headers(feed_override, timeout=feed_timeout, ua=src_ua, extra_headers=src_headers)
            if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
                _lw(f"Feed parsing warning for {name}: {parsed.bozo_exception}")
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
            if logger: logger.info(f"Fetched {len(items)} items from feed for {name}")
            return name, items, None

        if not homepage:
            return name, [], "No feed or homepage provided"

        # Homepage fetch with per-source timeout
        resp = _get(homepage, timeout=src_timeout, ua=src_ua, extra_headers=src_headers); resp.raise_for_status()
        html = resp.text

        # Try to discover feeds first (fetch with headers)
        feed_links = find_feed_links(html, homepage)
        if feed_links:
            items = []
            for feed_url in feed_links[:2]:
                try:
                    parsed = _parse_feed_with_headers(feed_url, timeout=feed_timeout, ua=src_ua, extra_headers=src_headers)
                    if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
                        _lw(f"Feed parsing warning for {feed_url}: {parsed.bozo_exception}")
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
                except Exception as e:
                    _lw(f"Failed to parse feed {feed_url}: {e}")
                    continue
            if items:
                if logger: logger.info(f"Fetched {len(items)} items from discovered feeds for {name}")
                return name, items, None

        # Fallback to scraping the homepage with selector
        items = scrape_items_from_page(homepage, html, selector)
        if logger: logger.info(f"Scraped {len(items)} items from HTML for {name}")
        return name, items, None

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        _le(f"Source processing failed for {name}: {error_msg}")
        return name, [], error_msg

# -------------------- HTML pruning --------------------------------------------
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
    try:
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
    except Exception as e:
        if logger: logger.debug(f"HTML pruning failed for {base_url}: {e}")
        return ""

def extract_main_content(html: str, url: str) -> str:
    try:
        out = trafilatura.extract(html, url=url, include_images=False, include_tables=True, favor_recall=True, output_format="html")
        if out:
            pruned = prune_html(out, url)
            if pruned: 
                if logger: logger.debug(f"Content extracted via trafilatura for {url}")
                return pruned
    except Exception as e:
        if logger: logger.debug(f"Trafilatura extraction failed for {url}: {e}")
    try:
        doc = Document(html)
        article_html = doc.summary(html_partial=True)
        if article_html:
            pruned = prune_html(article_html, url)
            if pruned:
                if logger: logger.debug(f"Content extracted via readability for {url}")
                return pruned
    except Exception as e:
        if logger: logger.debug(f"Readability extraction failed for {url}: {e}")
    try:
        soup = BeautifulSoup(html, "html.parser")
        candidates = soup.select("article") or soup.select("main")
        for node in candidates:
            pruned = prune_html(str(node), url)
            if pruned:
                if logger: logger.debug(f"Content extracted via HTML tags for {url}")
                return pruned
    except Exception as e:
        if logger: logger.debug(f"HTML tag extraction failed for {url}: {e}")
    if logger: logger.debug(f"No content extracted for {url}")
    return ""

def truncate_html(html: str, max_chars: int = 8000) -> str:
    if not html: return ""
    return html if len(html) <= max_chars else (html[:max_chars] + "…")

# -------------------- Cache ----------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        if logger: logger.info("No existing cache file found, starting fresh")
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        if logger: logger.info(f"Loaded cache with {len(cache)} entries")
        return cache
    except Exception as e:
        _lw(f"Failed to load cache file: {e}, starting fresh")
        return {}

def save_cache(cache: dict):
    if len(cache) > CACHE_MAX_ENTRIES:
        if logger: logger.info(f"Cache size {len(cache)} exceeds limit {CACHE_MAX_ENTRIES}, trimming")
        items = sorted(cache.items(), key=lambda kv: kv[1].get("fetched_at",""), reverse=True)[:CACHE_MAX_ENTRIES]
        cache = dict(items)
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        if logger: logger.debug(f"Saved cache with {len(cache)} entries")
    except Exception as e:
        _le(f"Failed to save cache: {e}")
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

# -------------------- Filter ---------------------------------------------------
def normalize_and_filter(all_items, tz):
    seen = set(); deduped = []
    for it in all_items:
        url = it.get("url")
        if not url or url in seen: continue
        seen.add(url)
        it["title"] = strip_ws(it.get("title"))
        it["summary"] = strip_ws(it.get("summary"))
        deduped.append(it)
    if logger: logger.info(f"After deduplication: {len(deduped)} items")

    # Configurable window (default 168h = 7 days)
    WINDOW_HOURS = int(os.environ.get("RECENT_HOURS", "168"))
    now = datetime.now(tz); cutoff = now - timedelta(hours=WINDOW_HOURS)

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
    result = keep[:MAX_ARTICLES_TOTAL]
    if logger: logger.info(f"After filtering (≤{WINDOW_HOURS}h + undated): {len(result)} items")
    return result

def log_performance_stats(kept, cache, errors, start_time):
    stats = {
        "articles_collected": len(kept),
        "cache_entries": len(cache),
        "errors": len(errors),
        "execution_time": round(time.time() - start_time, 2)
    }
    if logger: logger.info(f"Performance stats: {stats}")
    try:
        with open(os.path.join(DATA_DIR, "stats.json"), "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        _lw(f"Failed to save stats: {e}")

# -------------------- Main -----------------------------------------------------
def main():
    global logger
    parser = argparse.ArgumentParser(description="Fetch Australian government news")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--test-sources', help='Use alternative sources file for testing')
    args = parser.parse_args()

    logger = setup_logging(args.debug)
    start_time = time.time()

    try:
        if logger: logger.info("=== Starting Australian Government News Fetch ===")
        log_memory_usage("start")

        tz, sources = load_sources(args.test_sources)
        errors, collected = [], []
        cache = load_cache()
        if logger: logger.info(f"Loaded {len(sources)} sources; cache entries: {len(cache)}")

        # Collect
        for src in sources:
            try:
                name, items, err = fetch_source(src, tz)
                if err:
                    errors.append({"source": name, "error": err})
                    _lw(f"Source error for {name}: {err}")
                else:
                    for it in items: it["source"] = name
                    collected.extend(items)
                    if logger: logger.debug(f"Collected {len(items)} items from {name}")
            except Exception as e:
                error_msg = f"Unexpected error processing source: {e}"
                errors.append({"source": src.get("name", "Unknown"), "error": error_msg})
                _le(error_msg)

        log_memory_usage("after source collection")

        kept = normalize_and_filter(collected, tz)

        # Decide what to fetch
        to_fetch, cache_hits = [], 0
        for it in kept:
            url = it.get("url")
            if not url: continue
            cached = cache_get(cache, url, tz)
            if cached:
                it["content_html"] = cached
                cache_hits += 1
                if not it.get("summary"):
                    text = strip_ws(BeautifulSoup(cached, "html.parser").get_text())
                    it["summary"] = text[:280]
            else:
                if robots_can_fetch(url):
                    to_fetch.append(it)
                else:
                    if logger: logger.debug(f"Robots.txt blocks fetching: {url}")

        to_fetch = to_fetch[:MAX_NEW_FETCHES]
        if logger: logger.info(f"Cache hits: {cache_hits}, New fetches needed: {len(to_fetch)}")

        # Parallel fetch
        def worker(item):
            try:
                url = item["url"]
                if logger: logger.debug(f"Fetching content for: {url}")
                r = fetch_url(url); r.raise_for_status()
                html = extract_main_content(r.text, url)
                if html:
                    html = truncate_html(html)
                    return (url, html, None)
                return (url, "", None)
            except Exception as e:
                return (item["url"], "", f"{type(e).__name__}: {e}")

        if to_fetch:
            if logger: logger.info(f"Starting parallel content fetch for {len(to_fetch)} URLs")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(worker, it): it for it in to_fetch}
                for fut in as_completed(futures):
                    url, html, err = fut.result()
                    it = futures[fut]
                    if err:
                        errors.append({"source": it.get("source","?"), "url": url, "error": err})
                        _lw(f"Content fetch failed for {url}: {err}")
                        continue
                    if html:
                        it["content_html"] = html
                        if not it.get("summary"):
                            text = strip_ws(BeautifulSoup(html, "html.parser").get_text())
                            it["summary"] = text[:280]
                        cache_put(cache, url, html, tz)

        log_memory_usage("after content fetch")

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

        # Write JSON
        now = datetime.now(tz)
        date_str = now.strftime("%Y-%m-%d")
        daily_path  = os.path.join(DATA_DIR, f"{date_str}.json")
        latest_path = os.path.join(DATA_DIR, "latest.json")
        rss_path    = os.path.join(DATA_DIR, "unified.xml")

        payload = {
            "date": date_str,
            "timezone": str(tz),
            "generated_at": to_iso(now, tz),
            "count": len(kept),
            "items": kept,
            "errors": errors
        }

        for path in (daily_path, latest_path):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            if logger: logger.debug(f"Written {path}")

        # RSS (standalone)
        def generate_rss(items):
            from xml.sax.saxutils import escape
            last_build = now.strftime("%a, %d %b %Y %H:%M:%S %z")
            out = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<rss version="2.0">', '<channel>',
                "<title>Australian Government Announcements (Daily)</title>",
                "<link>https://example.com/</link>",
                "<description>Unified feed of Australian government announcements generated daily</description>",
                f"<lastBuildDate>{last_build}</lastBuildDate>",
            ]
            for it in items[:200]:
                title = escape(it.get("title") or "")
                link  = escape(it.get("url") or "")
                desc  = escape(it.get("summary") or "")
                pub_date = format_rss_date(it.get("published_at"), tz)
                guid = make_id(it.get("url") or (title + pub_date))
                out += [
                    "<item>",
                    f"<title>{title}</title>",
                    f"<link>{link}</link>",
                    f"<guid isPermaLink='false'>{guid}</guid>",
                    f"<pubDate>{pub_date}</pubDate>",
                    f"<description>{desc}</description>" if desc else "",
                    "</item>"
                ]
            out += ["</channel>", "</rss>"]
            return "\n".join(x for x in out if x)

        with open(rss_path, "w", encoding="utf-8") as f:
            f.write(generate_rss(kept))

        # Save cache & stats
        save_cache(cache)
        log_performance_stats(kept, cache, errors, start_time)

        if logger: logger.info(f"Wrote {len(kept)} items → {latest_path}")

    except Exception as e:
        _le(f"UNHANDLED ERROR: {e}")
        # If you prefer the workflow to fail on fatal errors, uncomment:
        # sys.exit(1)

if __name__ == "__main__":
    main()
