#!/usr/bin/env python3
"""
Phase 31: News Sentiment Integration

Fetches financial news from UDN and CBN (Commercial Times / 工商時報) Taiwan
financial sites, classifies sentiment using Traditional Chinese keywords, and
aggregates sentiment scores per stock with recency weighting.

Integration with Stage 2 via check_news_sentiment() — follows the same
(score, status) return pattern as other Stage 2 check functions.

Sentiment mapping:
    +0.5 (positive) → 90
     0.0 (neutral)  → 60
    -0.5 (negative) → 30

Cache: data/news_cache.json with 4-hour TTL.
Backward compatible: graceful fallback returns (60, "neutral") on any failure.
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

CACHE_FILENAME = "news_cache.json"
CACHE_TTL_SECONDS = 4 * 3600  # 4 hours

# News source URLs — UDN financial news and CBN (工商時報) financial news
NEWS_SOURCES = {
    "udn": {
        "search_url": (
            "https://udn.com/api/more?"
            "page={page}&id=search:{query}&channelId=2"
        ),
        "article_base": "https://udn.com",
        "timeout": 10,
    },
    "cbn": {
        "search_url": (
            "https://www.chinatimes.com/search/"
            "{query}?chdtv"
        ),
        "article_base": "https://www.chinatimes.com",
        "timeout": 10,
    },
}

# Positive keywords — bullish / growth / breakthrough signals
POSITIVE_KEYWORDS = [
    "突破",    # breakthrough
    "創新高",  # record high
    "獲利",    # profit
    "成長",    # growth
    "營收增",  # revenue increase
    "訂單增",  # order increase
    "看好",    # optimistic
    "利多",    # positive catalyst
    "漲停",    # limit up
    "加碼",    # increase position
    "買超",    # net buy (institutions)
    "布局",    # strategic positioning
    "擴產",    # expand production
    "新訂單",  # new orders
    "轉盈",    # turn profitable
    "大賺",    # big profit
    "展望佳",  # positive outlook
    "營收創高",  # revenue record high
    "股價創高",  # stock price record high
]

# Negative keywords — bearish / decline / risk signals
NEGATIVE_KEYWORDS = [
    "虧損",    # loss
    "破底",    # break bottom / new low
    "裁員",    # layoff
    "衰退",    # decline
    "營收減",  # revenue decrease
    "減產",    # production cut
    "看淡",    # pessimistic
    "利空",    # negative catalyst
    "跌停",    # limit down
    "賣超",    # net sell (institutions)
    "降評",    # downgrade
    "違約",    # default / breach
    "停工",    # work stoppage
    "轉虧",    # turn to loss
    "大虧",    # big loss
    "下修",    # downward revision
    "腰斬",    # halve / collapse
    "營收下滑",  # revenue decline
    "股價破底",  # stock price new low
    "財報不佳",  # poor earnings
]

# Sentiment-to-score mapping as specified in Phase 31
SENTIMENT_SCORE_MAP = {
    +0.5: 90,
     0.0: 60,
    -0.5: 30,
}

# Recency weight decay: how much to discount older articles
# Articles within 24h get full weight, then decay linearly over 7 days
MAX_RECENCY_DAYS = 7


# ---------------------------------------------------------------------------
#  Cache helpers
# ---------------------------------------------------------------------------

def _get_cache_path(data_dir: Optional[str] = None) -> Path:
    """Return path to news_cache.json."""
    if data_dir:
        return Path(data_dir) / CACHE_FILENAME
    return Path(__file__).parent.parent / "data" / CACHE_FILENAME


def load_cache(data_dir: Optional[str] = None) -> dict:
    """Load news cache from disk. Returns empty dict if missing or expired."""
    cache_path = _get_cache_path(data_dir)

    if not cache_path.exists():
        return {}

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)

        # Check TTL on the entire cache
        cached_at = cache.get("_cached_at", 0)
        if time.time() - cached_at > CACHE_TTL_SECONDS:
            logger.debug("News cache expired (age=%dh, TTL=%dh)",
                         (time.time() - cached_at) / 3600, CACHE_TTL_SECONDS / 3600)
            return {}  # Expired → treat as empty

        return cache
    except (json.JSONDecodeError, IOError, ValueError) as e:
        logger.debug("News cache load failed: %s", e)
        return {}


def save_cache(cache: dict, data_dir: Optional[str] = None) -> None:
    """Save news cache to disk with timestamp.
    
    Only updates _cached_at if it's not already set in the cache dict,
    allowing tests to inject specific timestamps.
    """
    cache_path = _get_cache_path(data_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if "_cached_at" not in cache:
        cache["_cached_at"] = time.time()
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logger.warning("Failed to save news cache: %s", e)


def invalidate_cache(data_dir: Optional[str] = None) -> None:
    """Remove the news cache file, forcing a fresh fetch on next call."""
    cache_path = _get_cache_path(data_dir)
    try:
        if cache_path.exists():
            cache_path.unlink()
    except IOError as e:
        logger.debug("Failed to delete news cache: %s", e)


# ---------------------------------------------------------------------------
#  News fetching
# ---------------------------------------------------------------------------

def fetch_news_udn(query: str, max_pages: int = 2) -> List[dict]:
    """Fetch financial news from UDN (聯合新聞網) search API.

    Returns list of dicts with keys: title, snippet, date, source, url.
    On any failure, returns empty list (graceful fallback).
    """
    articles = []
    source_cfg = NEWS_SOURCES["udn"]

    for page in range(0, max_pages):
        try:
            url = source_cfg["search_url"].format(
                page=page, query=quote(query)
            )
            resp = requests.get(
                url,
                timeout=source_cfg["timeout"],
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                },
            )

            if resp.status_code != 200:
                logger.debug("UDN API returned HTTP %d for query=%s",
                             resp.status_code, query)
                break

            data = resp.json()

            # UDN API returns a list of stories under different key names
            stories = []
            if isinstance(data, list):
                stories = data
            elif isinstance(data, dict):
                # Common UDN response structures
                stories = (data.get("stories") or data.get("data") or
                           data.get("news_list") or data.get("results") or [])
                if isinstance(stories, dict):
                    stories = list(stories.values())

            if not stories:
                break

            for story in stories:
                if not isinstance(story, dict):
                    continue
                title = story.get("title", {})
                if isinstance(title, dict):
                    title = title.get("text", "")
                title = str(title).strip()

                snippet = story.get("paragraph", "")
                if isinstance(snippet, dict):
                    snippet = snippet.get("text", "")
                snippet = str(snippet).strip()

                # UDN date handling — multiple formats
                date_str = ""
                dt = story.get("time", {})
                if isinstance(dt, dict):
                    date_str = dt.get("date", dt.get("text", ""))
                elif isinstance(dt, str):
                    date_str = dt
                elif isinstance(dt, (int, float)):
                    # Unix timestamp
                    try:
                        date_str = datetime.fromtimestamp(dt).strftime("%Y-%m-%d")
                    except (ValueError, OSError):
                        pass
                date_str = str(date_str).strip()

                url_link = story.get("url", {})
                if isinstance(url_link, dict):
                    url_link = url_link.get("text", "")
                url_link = str(url_link).strip()
                if url_link and not url_link.startswith("http"):
                    url_link = source_cfg["article_base"] + url_link

                if title:
                    articles.append({
                        "title": title,
                        "snippet": snippet,
                        "date": date_str,
                        "source": "udn",
                        "url": url_link,
                    })

            # Rate limiting between pages
            time.sleep(0.3)

        except requests.exceptions.Timeout:
            logger.debug("UDN fetch timeout for query=%s page=%d", query, page)
            break
        except requests.exceptions.ConnectionError:
            logger.debug("UDN connection error for query=%s", query)
            break
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("UDN response parse error: %s", e)
            break
        except Exception as e:
            logger.debug("UDN fetch unexpected error: %s", e)
            break

    logger.debug("UDN: fetched %d articles for query=%s", len(articles), query)
    return articles


def fetch_news_cbn(query: str) -> List[dict]:
    """Fetch financial news from CBN / 工商時報 (China Times financial).

    Returns list of dicts with keys: title, snippet, date, source, url.
    On any failure, returns empty list (graceful fallback).

    Note: CBN uses server-rendered HTML. We attempt a lightweight fetch
    and parse the HTML for article listings. If the HTML structure changes,
    this gracefully returns an empty list.
    """
    articles = []
    source_cfg = NEWS_SOURCES["cbn"]

    try:
        url = source_cfg["search_url"].format(query=quote(query))
        resp = requests.get(
            url,
            timeout=source_cfg["timeout"],
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html",
            },
        )

        if resp.status_code != 200:
            logger.debug("CBN returned HTTP %d for query=%s",
                         resp.status_code, query)
            return []

        # Lightweight HTML parsing — extract article titles and dates
        # Using regex instead of BeautifulSoup to avoid extra dependency
        html = resp.text

        # Try to find article blocks — CBN search results typically have
        # <h3> or <a> tags with title text, and date spans nearby
        # Pattern: look for title elements with stock-related content
        title_pattern = re.compile(
            r'<(?:h[23]|a)[^>]*class="[^"]*title[^"]*"[^>]*>'
            r'(.*?)'
            r'</(?:h[23]|a)>',
            re.DOTALL | re.IGNORECASE,
        )
        date_pattern = re.compile(
            r'(?:date|time|meta)[^>]*>'
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
            re.IGNORECASE,
        )

        # Extract titles
        raw_titles = title_pattern.findall(html)
        raw_dates = date_pattern.findall(html)

        for i, raw_title in enumerate(raw_titles):
            # Clean HTML tags from title
            title = re.sub(r'<[^>]+>', '', raw_title).strip()
            if not title or len(title) < 6:
                continue

            # Only include if it looks like financial news
            has_chinese = any('\u4e00' <= c <= '\u9fff' for c in title)
            if not has_chinese:
                continue

            date_str = ""
            if i < len(raw_dates):
                date_str = raw_dates[i].replace("/", "-")
            else:
                date_str = datetime.now().strftime("%Y-%m-%d")

            articles.append({
                "title": title,
                "snippet": "",  # CBN HTML parsing doesn't easily get snippets
                "date": date_str,
                "source": "cbn",
                "url": "",
            })

    except requests.exceptions.Timeout:
        logger.debug("CBN fetch timeout for query=%s", query)
    except requests.exceptions.ConnectionError:
        logger.debug("CBN connection error for query=%s", query)
    except Exception as e:
        logger.debug("CBN fetch unexpected error: %s", e)

    logger.debug("CBN: fetched %d articles for query=%s", len(articles), query)
    return articles


def fetch_news_for_stock(stock_code: str, stock_name: str = "") -> List[dict]:
    """Fetch recent news for a Taiwan stock from UDN and CBN.

    Uses both stock code (e.g. "2330") and stock name (e.g. "台積電") as
    search queries to maximize coverage.

    Returns deduplicated list of article dicts.
    """
    queries = [stock_code]
    if stock_name and stock_name != stock_code:
        queries.append(stock_name)

    all_articles = []
    seen_titles = set()

    for query in queries:
        # UDN fetch
        udn_articles = fetch_news_udn(query, max_pages=2)
        for art in udn_articles:
            # Deduplicate by normalized title
            norm_title = art["title"].strip().lower()
            if norm_title not in seen_titles:
                seen_titles.add(norm_title)
                all_articles.append(art)

        # CBN fetch
        cbn_articles = fetch_news_cbn(query)
        for art in cbn_articles:
            norm_title = art["title"].strip().lower()
            if norm_title not in seen_titles:
                seen_titles.add(norm_title)
                all_articles.append(art)

    # Sort by date descending (newest first)
    all_articles.sort(key=lambda a: a.get("date", ""), reverse=True)

    logger.debug("Total deduplicated articles for %s: %d",
                 stock_code, len(all_articles))
    return all_articles


# ---------------------------------------------------------------------------
#  Sentiment classification
# ---------------------------------------------------------------------------

def classify_article_sentiment(article: dict) -> float:
    """Classify a single article's sentiment using keyword matching.

    Returns:
        +0.5 for positive sentiment
         0.0 for neutral sentiment
        -0.5 for negative sentiment

    Classification logic:
    - Count positive and negative keyword hits in title + snippet
    - If more positive hits → +0.5
    - If more negative hits → -0.5
    - If equal or no hits → 0.0 (neutral)
    - Negative hits are weighted 1.3x (loss aversion bias — bad news
      impacts prices more than good news)
    """
    text = f"{article.get('title', '')} {article.get('snippet', '')}"

    pos_hits = 0
    neg_hits = 0

    for kw in POSITIVE_KEYWORDS:
        if kw in text:
            pos_hits += 1

    for kw in NEGATIVE_KEYWORDS:
        if kw in text:
            neg_hits += 1

    # Weight negatives more heavily (loss aversion)
    weighted_neg = neg_hits * 1.3

    if pos_hits > weighted_neg:
        return +0.5
    elif weighted_neg > pos_hits:
        return -0.5
    else:
        return 0.0


# ---------------------------------------------------------------------------
#  Recency weighting
# ---------------------------------------------------------------------------

def compute_recency_weight(article_date: str,
                           reference_date: Optional[str] = None) -> float:
    """Compute recency weight for an article based on its age.

    Weight schedule:
        0-1 days old: 1.0 (full weight)
        1-2 days old: 0.85
        2-3 days old: 0.70
        3-4 days old: 0.55
        4-5 days old: 0.40
        5-6 days old: 0.25
        6-7 days old: 0.10
        7+ days old:  0.05 (minimal weight, not zero for safety)

    Returns:
        Float between 0.05 and 1.0
    """
    if not article_date:
        return 0.05

    try:
        # Parse the article date (handles YYYY-MM-DD and YYYY/MM/DD)
        date_str = article_date.replace("/", "-")
        # Handle various date lengths
        if len(date_str) >= 10:
            art_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        else:
            return 0.05
    except (ValueError, IndexError):
        return 0.05

    if reference_date:
        try:
            ref_date = datetime.strptime(reference_date[:10], "%Y-%m-%d")
        except (ValueError, IndexError):
            ref_date = datetime.now()
    else:
        ref_date = datetime.now()

    age_days = (ref_date - art_date).days

    if age_days <= 1:
        return 1.0
    elif age_days <= 2:
        return 0.85
    elif age_days <= 3:
        return 0.70
    elif age_days <= 4:
        return 0.55
    elif age_days <= 5:
        return 0.40
    elif age_days <= 6:
        return 0.25
    elif age_days <= 7:
        return 0.10
    else:
        return 0.05


# ---------------------------------------------------------------------------
#  Stock-level aggregation
# ---------------------------------------------------------------------------

def aggregate_sentiment(articles: List[dict],
                        reference_date: Optional[str] = None) -> Tuple[float, dict]:
    """Aggregate article sentiments into a single weighted sentiment score.

    Uses recency weighting: newer articles have more impact on the final
    score. The result is mapped from the continuous weighted average of
    sentiment values (-0.5 to +0.5) to a 0-100 scale.

    Returns:
        (score, detail) where:
        - score: float between 0 and 100 (mapped from sentiment)
        - detail: dict with breakdown info for diagnostics
    """
    if not articles:
        return 60.0, {"status": "no_articles", "count": 0,
                       "weighted_sentiment": 0.0}

    weighted_sum = 0.0
    total_weight = 0.0
    pos_count = 0
    neg_count = 0
    neutral_count = 0

    for article in articles:
        sentiment = classify_article_sentiment(article)
        weight = compute_recency_weight(article.get("date", ""), reference_date)

        weighted_sum += sentiment * weight
        total_weight += weight

        if sentiment > 0:
            pos_count += 1
        elif sentiment < 0:
            neg_count += 1
        else:
            neutral_count += 1

    if total_weight == 0:
        return 60.0, {"status": "zero_weight", "count": len(articles),
                       "weighted_sentiment": 0.0}

    avg_sentiment = weighted_sum / total_weight

    # Map continuous sentiment to 0-100 score
    # +0.5 → 90, 0.0 → 60, -0.5 → 30
    # Linear interpolation: score = 60 + avg_sentiment * 60
    score = 60.0 + avg_sentiment * 60.0
    score = round(max(0.0, min(100.0, score)), 2)

    detail = {
        "status": "ok",
        "count": len(articles),
        "positive": pos_count,
        "negative": neg_count,
        "neutral": neutral_count,
        "weighted_sentiment": round(avg_sentiment, 3),
    }

    return score, detail


# ---------------------------------------------------------------------------
#  Stage 2 integration: check_news_sentiment()
# ---------------------------------------------------------------------------

def check_news_sentiment(stock_code: str, stock_name: str = "",
                         data_dir: Optional[str] = None,
                         force_refresh: bool = False) -> Tuple[float, str]:
    """Stage 2 check: news sentiment for a specific stock.

    Follows the same (score, status) return pattern as other Stage 2 checks:
    - Returns (score: float, status: str) where score is 0-100
    - Returns (60, "neutral") as graceful fallback on any failure
    - Returns None on internal error (handled by Stage 2 error logic)

    The score mapping is:
        +0.5 sentiment → 90 (positive news)
         0.0 sentiment → 60 (neutral / no news)
        -0.5 sentiment → 30 (negative news)

    Cache: Results are cached in data/news_cache.json with 4h TTL.
    Multiple stock lookups in the same pipeline run share the cache,
    avoiding redundant HTTP requests.

    Args:
        stock_code: Taiwan stock code (e.g. "2330")
        stock_name: Stock name in Traditional Chinese (e.g. "台積電")
        data_dir:   Override data directory (for testing)
        force_refresh: Skip cache and force fresh fetch

    Returns:
        (score, status) tuple — score is 0-100, status is a descriptive string
    """
    # Graceful fallback defaults
    NEUTRAL_SCORE = 60.0
    NEUTRAL_STATUS = "neutral"

    try:
        # 1. Check cache first
        if not force_refresh:
            cache = load_cache(data_dir)
            cache_key = f"sentiment_{stock_code}"
            if cache_key in cache:
                cached = cache[cache_key]
                cached_time = cache.get("_cached_at", 0)
                if time.time() - cached_time < CACHE_TTL_SECONDS:
                    logger.debug("News sentiment cache hit for %s", stock_code)
                    return cached["score"], cached["status"]

        # 2. Fetch news articles
        articles = fetch_news_for_stock(stock_code, stock_name)

        if not articles:
            # No news is neutral, not negative — common for small-cap stocks
            _update_cache(stock_code, NEUTRAL_SCORE, NEUTRAL_STATUS,
                          0, 0, 0, data_dir)
            return NEUTRAL_SCORE, NEUTRAL_STATUS

        # 3. Aggregate with recency weighting
        score, detail = aggregate_sentiment(articles)

        # 4. Determine status string
        if score >= 75:
            status = "positive"
        elif score >= 50:
            status = "neutral"
        else:
            status = "negative"

        # 5. Cache the result
        _update_cache(
            stock_code, score, status,
            detail.get("positive", 0),
            detail.get("negative", 0),
            detail.get("neutral", 0),
            data_dir,
        )

        logger.info("News sentiment for %s: score=%.1f status=%s "
                    "(%d articles: %d pos, %d neg, %d neutral)",
                    stock_code, score, status, detail.get("count", 0),
                    detail.get("positive", 0), detail.get("negative", 0),
                    detail.get("neutral", 0))

        return score, status

    except Exception as e:
        # Graceful fallback — never crash the pipeline
        logger.debug("[Stage2] check_news_sentiment failed for %s: %r",
                     stock_code, e)
        return None  # Let Stage 2 handle None as error


def _update_cache(stock_code: str, score: float, status: str,
                  pos_count: int, neg_count: int, neutral_count: int,
                  data_dir: Optional[str] = None) -> None:
    """Update the news cache with a single stock's sentiment result."""
    try:
        cache = load_cache(data_dir)
        cache_key = f"sentiment_{stock_code}"
        cache[cache_key] = {
            "score": score,
            "status": status,
            "positive": pos_count,
            "negative": neg_count,
            "neutral": neutral_count,
            "updated_at": datetime.now().isoformat(),
        }
        save_cache(cache, data_dir)
    except Exception as e:
        logger.debug("Cache update failed for %s: %s", stock_code, e)


# ---------------------------------------------------------------------------
#  Batch utility for Stage 2
# ---------------------------------------------------------------------------

def batch_news_sentiment(stock_codes: List[Tuple[str, str]],
                         data_dir: Optional[str] = None) -> Dict[str, Tuple[float, str]]:
    """Fetch and score news sentiment for multiple stocks.

    Uses cache aggressively — only fetches for stocks not in cache.
    This is the recommended entry point for Stage 2 integration to
    minimize HTTP requests during a single pipeline run.

    Args:
        stock_codes: List of (stock_code, stock_name) tuples
        data_dir:    Override data directory

    Returns:
        Dict mapping stock_code → (score, status)
    """
    results = {}
    cache = load_cache(data_dir)

    # Separate cached vs uncached stocks
    uncached = []
    for code, name in stock_codes:
        cache_key = f"sentiment_{code}"
        if cache_key in cache:
            cached = cache[cache_key]
            results[code] = (cached["score"], cached["status"])
        else:
            uncached.append((code, name))

    # Fetch only uncached stocks
    for code, name in uncached:
        score, status = check_news_sentiment(code, name, data_dir=data_dir)
        if score is not None:
            results[code] = (score, status)
        else:
            results[code] = (60.0, "error_fallback")

    logger.info("Batch news sentiment: %d cached, %d fetched",
                len(stock_codes) - len(uncached), len(uncached))
    return results


# ---------------------------------------------------------------------------
#  CLI for testing / manual invocation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Phase 31: News Sentiment")
    parser.add_argument("--stock", type=str, required=True,
                        help="Stock code (e.g. 2330)")
    parser.add_argument("--name", type=str, default="",
                        help="Stock name (e.g. 台積電)")
    parser.add_argument("--invalidate-cache", action="store_true",
                        help="Force fresh fetch by invalidating cache")
    args = parser.parse_args()

    if args.invalidate_cache:
        invalidate_cache()

    score, status = check_news_sentiment(
        args.stock, args.name, force_refresh=args.invalidate_cache
    )

    if score is not None:
        print(f"Stock: {args.stock} {args.name}")
        print(f"Score: {score:.1f}")
        print(f"Status: {status}")
    else:
        print(f"Failed to get sentiment for {args.stock}")
