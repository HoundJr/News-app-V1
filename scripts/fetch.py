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

def _parse_feed_with_headers(url, timeout):
    """Fetch feed with our UA/headers, then parse from bytes."""
    resp = fetch_url(url, timeout=timeout)
    resp.raise_for_status()
    return feedparser.parse(resp.content)

def fetch_source(src, tz):
    name = src.get("name", "Unknown Source")
    feed_override = src.get("feed")
    homepage = src.get("homepage")
    selector = src.get("selector")
    src_timeout = int(src.get("timeout", FETCH_TIMEOUT))
    feed_timeout = int(src.get("feed_timeout", src_timeout))

    if logger: logger.info(f"Processing source: {name}")

    try:
        # Preferred: explicit feed
        if feed_override:
            if logger: logger.debug(f"Using direct feed: {feed_override}")
            items = []
            parsed = _parse_feed_with_headers(feed_override, timeout=feed_timeout)
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
        resp = fetch_url(homepage, timeout=src_timeout); resp.raise_for_status()
        html = resp.text

        # Try to discover feeds first (fetch with headers)
        feed_links = find_feed_links(html, homepage)
        if feed_links:
            items = []
            for feed_url in feed_links[:2]:
                try:
                    parsed = _parse_feed_with_headers(feed_url, timeout=feed_timeout)
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

def extract_main_content(ht
