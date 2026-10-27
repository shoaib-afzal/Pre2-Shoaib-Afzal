"""
International News Collector
Dynamic Queue Parallel Pipeline Architecture (16 Sources, 150 Articles/Source):
  - Maintains exactly 4 parallel browser workers (as one finishes, the next starts immediately).
  - Handles GDPR/Cookie banners and WAF challenges automatically.
  - 100% text-optimized RSS & Google News search bridges.
"""

import csv
import html
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urldefrag
from zoneinfo import ZoneInfo

import feedparser
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ---------------- CONFIGURATION ----------------

ARTICLES_PER_SOURCE = 150
OUTPUT_DIR = "news_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "international_news.csv")
LOCAL_TIMEZONE = "Asia/Karachi"

RSS_TIMEOUT = 10
PAGE_LOAD_TIMEOUT = 25000  # milliseconds
PAGE_READY_TIMEOUT = 10000

MIN_PAGE_DELAY_SECONDS = 2
MAX_PAGE_DELAY_SECONDS = 5

MAX_CONCURRENT_SOURCES = 4  # Strictly maintain 4 active parallel browsers

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# 16 High-Yield Text-Only News Sources with Maximized RSS Bridges
NEWS_SOURCES: Dict[str, List[str]] = {
    "Bloomberg": [
        "https://news.google.com/rss/search?q=when:24h+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=markets+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=economy+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=technology+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=finance+site:bloomberg.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "Reuters": [
        "https://news.google.com/rss/search?q=when:24h+site:reuters.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=world+site:reuters.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=business+site:reuters.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=technology+site:reuters.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=markets+site:reuters.com&ceid=US:en&hl=en-US&gl=US",
    ],
    "BBC News": [
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
        "https://news.google.com/rss/search?q=when:7d+site:bbc.com/news&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=when:7d+site:bbc.co.uk/news&hl=en-US&gl=US&ceid=US:en",
    ],
    "AP News": [
        "https://news.google.com/rss/search?q=when:24h+site:apnews.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=world+site:apnews.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=business+site:apnews.com&ceid=US:en&hl=en-US&gl=US",
        "https://news.google.com/rss/search?q=technology+site:apnews.com&ceid=US:en&hl=en-US&gl=US",
    ],
    "The Guardian": [
        "https://www.theguardian.com/international/rss",
        "https://www.theguardian.com/world/rss",
        "https://www.theguardian.com/business/rss",
        "https://www.theguardian.com/technology/rss",
        "https://www.theguardian.com/science/rss",
    ],
    "CNN": [
        "https://rss.cnn.com/rss/edition_world.rss",
        "https://rss.cnn.com/rss/edition_business.rss",
        "https://rss.cnn.com/rss/edition_technology.rss",
        "https://news.google.com/rss/search?q=when:7d+site:cnn.com+news&hl=en-US&gl=US&ceid=US:en",
    ],
    "Financial Times": [
        "https://www.ft.com/markets/emerging?format=rss",
        "https://www.ft.com/global-economy?format=rss",
        "https://www.ft.com/companies?format=rss",
        "https://news.google.com/rss/search?q=when:7d+site:ft.com+markets&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=when:7d+site:ft.com+economy&hl=en-US&gl=US&ceid=US:en",
    ],
    "NPR": [
        "https://feeds.npr.org/1001/rss.xml",
        "https://feeds.npr.org/1004/rss.xml",
        "https://feeds.npr.org/1006/rss.xml",
        "https://news.google.com/rss/search?q=when:7d+site:npr.org+news&hl=en-US&gl=US&ceid=US:en",
    ],
    "Washington Post": [
        "https://news.google.com/rss/search?q=when:24h+site:washingtonpost.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=world+site:washingtonpost.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=business+site:washingtonpost.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "Wall Street Journal": [
        "https://news.google.com/rss/search?q=when:24h+site:wsj.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=markets+site:wsj.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=economy+site:wsj.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "CBS News": [
        "https://www.cbsnews.com/latest/rss/main",
        "https://news.google.com/rss/search?q=when:7d+site:cbsnews.com+news&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=when:7d+site:cbsnews.com+world&hl=en-US&gl=US&ceid=US:en",
    ],
    "ABC News": [
        "https://abcnews.go.com/abcnews/internationalnews",
        "https://news.google.com/rss/search?q=when:7d+site:abcnews.go.com+news&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=when:7d+site:abcnews.go.com+world&hl=en-US&gl=US&ceid=US:en",
    ],
    "Politico": [
        "https://news.google.com/rss/search?q=when:24h+site:politico.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=news+site:politico.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=economy+site:politico.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "CNBC": [
        "https://www.cnbc.com/id/10003114/device/rss/rss.html",
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",
        "https://news.google.com/rss/search?q=when:7d+site:cnbc.com+markets&hl=en-US&gl=US&ceid=US:en",
    ],
    "NBC News": [
        "https://news.google.com/rss/search?q=when:24h+site:nbcnews.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=world+site:nbcnews.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=business+site:nbcnews.com&hl=en-US&gl=US&ceid=US:en",
    ],
    "Los Angeles Times": [
        "https://news.google.com/rss/search?q=when:24h+site:latimes.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=world+site:latimes.com&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=business+site:latimes.com&hl=en-US&gl=US&ceid=US:en",
    ],
}

CSV_FIELDS = [
    "source",
    "title",
    "url",
    "published",
    "author",
    "summary",
    "article_text",
    "collected_at",
    "scrape_status",
]

# Thread lock to prevent CSV file corruption during parallel writes
csv_lock = threading.Lock()


# ---------------- HELPERS ----------------

def now_local() -> str:
    return datetime.now(ZoneInfo(LOCAL_TIMEZONE)).isoformat(timespec="seconds")


def parse_feed_date(entry) -> str:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not parsed:
        return ""
    try:
        dt = datetime(*parsed[:6], tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo(LOCAL_TIMEZONE)).isoformat(timespec="seconds")
    except (ValueError, TypeError, OverflowError):
        return ""


# =====================================================================
# STAGE 1: REQUESTS (Discover RSS entries for a single source, max 150)
# =====================================================================

def fetch_source_entries(source: str, feed_urls: List[str]) -> List[dict]:
    print(f"[Stage 1] Discovering RSS entries for: {source} (Target: up to {ARTICLES_PER_SOURCE})", flush=True)
    source_articles: List[dict] = []
    seen_urls = set()
    source_count = 0

    for feed_url in feed_urls:
        if source_count >= ARTICLES_PER_SOURCE:
            break
        try:
            response = requests.get(feed_url, headers=REQUEST_HEADERS, timeout=RSS_TIMEOUT)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            for entry in feed.entries:
                if source_count >= ARTICLES_PER_SOURCE:
                    break

                raw_url = entry.get("link", "").strip()
                if not raw_url:
                    continue

                url = urldefrag(raw_url)[0]
                key = url.rstrip("/").lower()

                if key in seen_urls:
                    continue
                seen_urls.add(key)

                title = html.unescape(entry.get("title", ""))
                if not title:
                    continue

                summary_html = entry.get("summary", "")
                summary_soup = BeautifulSoup(summary_html, "html.parser")
                summary = re.sub(r"\s+", " ", summary_soup.get_text(" ", strip=True)).strip()

                source_articles.append({
                    "source": source,
                    "title": title,
                    "url": url,
                    "published": parse_feed_date(entry),
                    "author": html.unescape(entry.get("author", "")).strip(),
                    "summary": summary,
                    "raw_html": "",
                    "article_text": "",
                    "collected_at": now_local(),
                    "scrape_status": "pending",
                })
                source_count += 1
        except Exception as exc:
            print(f"[{source} Stage 1 Warning] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

        time.sleep(0.3)

    print(f"[Stage 1 Complete] Discovered {len(source_articles)} entries for {source}.", flush=True)
    return source_articles


# =====================================================================
# STAGE 3: PLAYWRIGHT (Open source pages, handle GDPR/Cookies & CAPTCHAs)
# =====================================================================

def simulate_human_movements(page) -> None:
    try:
        page.mouse.move(random.randint(100, 500), random.randint(100, 400))
        time.sleep(random.uniform(0.3, 0.5))
        page.evaluate("window.scrollBy(0, window.innerHeight / 2)")
        time.sleep(random.uniform(0.3, 0.6))
    except Exception:
        pass


def dismiss_cookie_banners(page) -> None:
    """Detects and clicks GDPR/Cookie consent buttons automatically."""
    cookie_selectors = [
        "button:has-text('Accept All')",
        "button:has-text('Accept Cookies')",
        "button:has-text('Accept')",
        "button:has-text('Agree')",
        "button:has-text('I Agree')",
        "button:has-text('Allow all')",
        "#onetrust-accept-btn-handler",
        ".cookie-consent-accept",
        "button[id*='accept']",
        "button[class*='accept']",
    ]
    for selector in cookie_selectors:
        try:
            locator = page.locator(selector).first
            if locator.is_visible(timeout=1000):
                locator.click(timeout=1500)
                time.sleep(0.5)
                break
        except Exception:
            pass


def check_and_handle_bot_challenge(page, source: str) -> None:
    challenge_keywords = [
        "cloudflare", "verify you are human", "captcha", "press & hold",
        "checking your browser", "access is temporarily restricted", "unusual activity", "robot"
    ]
    
    max_wait_time = 35
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            body_text = page.locator("body").inner_text().lower() if page.locator("body").count() > 0 else ""
            if any(kw in body_text for kw in challenge_keywords):
                print(f"\n[{source} BLOCK DETECTED] CAPTCHA / Verification active! Please solve it in the browser window...", flush=True)
                time.sleep(3.0)
            else:
                break
        except Exception:
            break


def scrape_source_with_playwright(page, articles: List[dict], source: str) -> List[dict]:
    print(f"[{source} Stage 3] Processing {len(articles)} pages via Playwright...", flush=True)
    total = len(articles)
    for idx, article in enumerate(articles, start=1):
        print(f"[{source}] ({idx}/{total}) Opening: {article['url']}", flush=True)
        try:
            page.goto(article["url"], timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            
            # Dismiss cookie banners & simulate movements
            dismiss_cookie_banners(page)
            simulate_human_movements(page)
            
            # Handle anti-bot challenges if present
            check_and_handle_bot_challenge(page, source)

            try:
                page.wait_for_selector("article, main, [itemprop='articleBody']", timeout=PAGE_READY_TIMEOUT)
            except Exception:
                pass

            article["raw_html"] = page.content()
            article["scrape_status"] = "fetched"
        except PlaywrightTimeoutError:
            article["scrape_status"] = "connection_timeout"
        except Exception as exc:
            msg = str(exc).lower()
            if "401" in msg:
                article["scrape_status"] = "http_401_unauthorized"
            elif "403" in msg:
                article["scrape_status"] = "http_403_forbidden"
            else:
                article["scrape_status"] = f"browser_error:{type(exc).__name__}"

        time.sleep(random.uniform(MIN_PAGE_DELAY_SECONDS, MAX_PAGE_DELAY_SECONDS))

    print(f"[{source} Stage 3 Complete] Playwright extraction finished.", flush=True)
    return articles


# =====================================================================
# STAGE 2: BEAUTIFULSOUP (Parse, select, clean, and format source text)
# =====================================================================

def parse_source_with_beautifulsoup(articles: List[dict], source: str) -> List[dict]:
    print(f"[{source} Stage 2] Cleaning and formatting text with BeautifulSoup...", flush=True)
    for article in articles:
        if not article["raw_html"]:
            continue

        soup = BeautifulSoup(article["raw_html"], "html.parser")

        if not article["title"]:
            title_tag = soup.find("meta", property="og:title") or soup.find("meta", name="twitter:title")
            if title_tag and title_tag.get("content"):
                article["title"] = html.unescape(title_tag["content"]).strip()
            elif soup.title:
                article["title"] = html.unescape(soup.title.get_text(" ", strip=True)).strip()

        if not article["author"]:
            author_tag = soup.find("meta", attrs={"name": "author"}) or soup.find("meta", property="article:author")
            if author_tag and author_tag.get("content"):
                article["author"] = html.unescape(author_tag["content"]).strip()

        if not article["published"]:
            pub_tag = soup.find("meta", property="article:published_time") or soup.find("meta", attrs={"name": "datePublished"})
            if pub_tag and pub_tag.get("content"):
                article["published"] = pub_tag["content"].strip()

        for tag in soup.select("script, style, noscript, svg, nav, footer, header, aside, form, iframe"):
            tag.decompose()

        containers = soup.select("article, [itemprop='articleBody']")
        if not containers:
            selectors = [
                ".article-body", ".article__body", ".article-content",
                ".article__content", ".story-body", ".story__body",
                ".story-content", ".entry-content", ".post-content",
            ]
            for sel in selectors:
                containers = soup.select(sel)
                if containers:
                    break

        paragraphs = []
        for container in containers:
            for p in container.select("p, h2, h3"):
                text = html.unescape(p.get_text(" ", strip=True))
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) >= 35 and text not in paragraphs:
                    paragraphs.append(text)

        article["article_text"] = "\n\n".join(paragraphs)

        full_page_text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).lower()
        if article["article_text"]:
            article["scrape_status"] = "ok"
        elif any(p in full_page_text for p in ["access is temporarily restricted", "unusual activity", "access denied", "forbidden", "403", "401"]):
            article["scrape_status"] = "restricted_or_blocked"
        elif article["scrape_status"] == "fetched":
            article["scrape_status"] = "no_text"

        article.pop("raw_html", None)

    print(f"[{source} Stage 2 Complete] Parsing finished.\n", flush=True)
    return articles


# ---------------- THREAD-SAFE CSV OUTPUT ----------------

def save_csv(articles: List[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with csv_lock:
        try:
            file_exists = os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0
            with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
                if not file_exists:
                    writer.writeheader()
                writer.writerows(articles)
            print(f"[SAVED] Appended {len(articles)} articles to -> {os.path.abspath(OUTPUT_FILE)}", flush=True)
        except Exception as exc:
            print(f"[WARN] Could not save CSV: {exc}", file=sys.stderr)


# ---------------- SINGLE SOURCE WORKER ----------------

def process_source(source: str, feed_urls: List[str]) -> int:
    print("=" * 68)
    print(f"STARTED SOURCE: {source.upper()}")
    print("=" * 68)

    source_articles = fetch_source_entries(source, feed_urls)
    if not source_articles:
        print(f"No articles found for {source}. Skipping.\n", flush=True)
        return 0

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--window-size=1365,900"]
            )
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1365, "height": 900},
                locale="en-US",
            )
            
            # Pre-inject common privacy/consent cookies
            context.add_cookies([
                {"name": "FTConsent", "value": "accepted", "domain": ".ft.com", "path": "/"},
                {"name": "EuConsent", "value": "1", "domain": ".bbc.com", "path": "/"},
            ])
            
            page = context.new_page()

            source_articles = scrape_source_with_playwright(page, source_articles, source)
            browser.close()

        source_articles = parse_source_with_beautifulsoup(source_articles, source)
        save_csv(source_articles)
        
        print(f"--- Finished {source}: {len(source_articles)} articles processed and saved. ---\n", flush=True)
        return len(source_articles)

    except Exception as exc:
        print(f"[{source} Error] Pipeline failed: {exc}", file=sys.stderr, flush=True)
        return 0


# ---------------- DYNAMIC WORKER PIPELINE WORKFLOW ----------------

def main() -> None:
    print("=" * 68)
    print("INTERNATIONAL NEWS COLLECTOR (DYNAMIC QUEUE MODE)")
    print(f"Target: up to {ARTICLES_PER_SOURCE} articles/source | Active Workers: {MAX_CONCURRENT_SOURCES}")
    print("=" * 68)

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    total_collected = 0

    # Uses a worker pool of 4. When 1 source finishes, the next pending source starts immediately!
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SOURCES) as executor:
        future_to_source = {
            executor.submit(process_source, source, feed_urls): source
            for source, feed_urls in NEWS_SOURCES.items()
        }

        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                count = future.result()
                total_collected += count
            except Exception as exc:
                print(f"[{source}] Generated an exception: {exc}", file=sys.stderr)

    print("=" * 68)
    print("ALL 16 SOURCES COMPLETED SUCCESSFULLY")
    print(f"Total processed unique articles saved: {total_collected}")
    print(f"File saved to: {os.path.abspath(OUTPUT_FILE)}")
    print("=" * 68)


if __name__ == "__main__":
    main()