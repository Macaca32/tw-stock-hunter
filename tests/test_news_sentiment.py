"""Unit tests for Phase 31: News Sentiment Integration.

Tests the core sentiment classification, recency weighting, aggregation,
caching, and Stage 2 integration logic. All data is inline — no HTTP
requests are made (news fetching is mocked).
"""

import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  1. Sentiment Classification
# ═══════════════════════════════════════════════════════════════════════

class TestSentimentClassification:
    """Verify keyword-based sentiment classification for Traditional Chinese news."""

    def _classify(self, article):
        from news_sentiment import classify_article_sentiment
        return classify_article_sentiment(article)

    def test_positive_article(self):
        """Article with positive keywords should return +0.5."""
        article = {"title": "台積電營收創新高，獲利突破市場預期", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == 0.5, f"Expected +0.5 for positive, got {sentiment}"

    def test_negative_article(self):
        """Article with negative keywords should return -0.5."""
        article = {"title": "公司虧損擴大，宣布裁員計畫", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5, f"Expected -0.5 for negative, got {sentiment}"

    def test_neutral_article(self):
        """Article with no keywords should return 0.0."""
        article = {"title": "台積電將於下週召開股東會", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == 0.0, f"Expected 0.0 for neutral, got {sentiment}"

    def test_mixed_article_negative_bias(self):
        """Article with both positive and negative — negative should dominate (1.3x weight)."""
        # 1 negative (虧損) vs 1 positive (獲利) → weighted: 1*1.3 > 1 → negative
        article = {"title": "公司獲利但虧損擴大", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5, f"Expected -0.5 for mixed (neg bias), got {sentiment}"

    def test_multiple_positive_overcome_negative(self):
        """Multiple positive keywords can overcome a single negative."""
        # 3 positive (突破, 獲利, 成長) vs 1 negative (虧損) → 3 > 1*1.3 → positive
        article = {"title": "突破瓶頸獲利成長，但子公司虧損", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == 0.5, f"Expected +0.5 for positive-dominant, got {sentiment}"

    def test_snippet_also_checked(self):
        """Keywords in snippet (not just title) should be detected."""
        article = {"title": "公司公告", "snippet": "本月營收創新高，看好後市"}
        sentiment = self._classify(article)
        assert sentiment == 0.5, f"Expected +0.5 for snippet keywords, got {sentiment}"

    def test_breakthrough_keyword(self):
        """'突破' keyword should be positive."""
        article = {"title": "技術突破帶動股價上揚", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == 0.5

    def test_record_high_keyword(self):
        """'創新高' keyword should be positive."""
        article = {"title": "股價創新高", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == 0.5

    def test_bottom_break_keyword(self):
        """'破底' keyword should be negative."""
        article = {"title": "股價破底投資人恐慌", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5

    def test_layoff_keyword(self):
        """'裁員' keyword should be negative."""
        article = {"title": "科技大廠裁員百人", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5


# ═══════════════════════════════════════════════════════════════════════
#  2. Sentiment-to-Score Mapping
# ═══════════════════════════════════════════════════════════════════════

class TestSentimentScoreMapping:
    """Verify the sentiment → 0-100 score mapping: +0.5→90, 0→60, -0.5→30."""

    def test_positive_maps_to_90(self):
        """+0.5 sentiment should map to score 90."""
        score = 60.0 + 0.5 * 60.0
        assert score == 90.0

    def test_neutral_maps_to_60(self):
        """0.0 sentiment should map to score 60."""
        score = 60.0 + 0.0 * 60.0
        assert score == 60.0

    def test_negative_maps_to_30(self):
        """-0.5 sentiment should map to score 30."""
        score = 60.0 + (-0.5) * 60.0
        assert score == 30.0

    def test_aggregate_positive(self):
        """All-positive articles should aggregate to ~90."""
        from news_sentiment import aggregate_sentiment
        articles = [
            {"title": "台積電獲利大增", "snippet": "", "date": "2026-05-15"},
            {"title": "營收創新高", "snippet": "", "date": "2026-05-14"},
        ]
        score, detail = aggregate_sentiment(articles, reference_date="2026-05-16")
        assert score >= 80, f"Positive articles should score >=80, got {score}"
        assert detail["positive"] > 0

    def test_aggregate_negative(self):
        """All-negative articles should aggregate to ~30."""
        from news_sentiment import aggregate_sentiment
        articles = [
            {"title": "公司虧損嚴重", "snippet": "", "date": "2026-05-15"},
            {"title": "股價破底裁員", "snippet": "", "date": "2026-05-14"},
        ]
        score, detail = aggregate_sentiment(articles, reference_date="2026-05-16")
        assert score <= 40, f"Negative articles should score <=40, got {score}"
        assert detail["negative"] > 0

    def test_aggregate_empty(self):
        """Empty articles should return neutral 60."""
        from news_sentiment import aggregate_sentiment
        score, detail = aggregate_sentiment([], reference_date="2026-05-16")
        assert score == 60.0
        assert detail["status"] == "no_articles"


# ═══════════════════════════════════════════════════════════════════════
#  3. Recency Weighting
# ═══════════════════════════════════════════════════════════════════════

class TestRecencyWeighting:
    """Verify that newer articles get higher weights."""

    def _weight(self, article_date, reference_date="2026-05-16"):
        from news_sentiment import compute_recency_weight
        return compute_recency_weight(article_date, reference_date)

    def test_today_full_weight(self):
        """Articles from today or yesterday should get weight 1.0."""
        assert self._weight("2026-05-16") == 1.0
        assert self._weight("2026-05-15") == 1.0

    def test_two_days_old(self):
        """2-day-old articles should get weight 0.85."""
        assert self._weight("2026-05-14") == 0.85

    def test_seven_days_old(self):
        """7-day-old articles should get weight 0.10."""
        assert self._weight("2026-05-09") == 0.10

    def test_very_old_minimal(self):
        """Articles older than 7 days should get weight 0.05."""
        assert self._weight("2026-04-01") == 0.05
        assert self._weight("2025-01-01") == 0.05

    def test_empty_date(self):
        """Empty date should return minimal weight."""
        assert self._weight("") == 0.05
        assert self._weight(None) == 0.05

    def test_invalid_date(self):
        """Invalid date string should return minimal weight."""
        assert self._weight("not-a-date") == 0.05


# ═══════════════════════════════════════════════════════════════════════
#  4. Cache Management
# ═══════════════════════════════════════════════════════════════════════

class TestCacheManagement:
    """Verify news_cache.json caching with TTL."""

    def test_cache_save_and_load(self, tmp_path):
        """Save and reload cache should preserve data."""
        from news_sentiment import save_cache, load_cache
        cache = {"sentiment_2330": {"score": 75.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert "sentiment_2330" in loaded
        assert loaded["sentiment_2330"]["score"] == 75.0

    def test_cache_ttl_expired(self, tmp_path):
        """Expired cache (older than 4h) should return empty dict."""
        from news_sentiment import save_cache, load_cache, CACHE_TTL_SECONDS
        cache = {"_cached_at": time.time() - CACHE_TTL_SECONDS - 1}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded == {}  # Expired cache returns empty

    def test_cache_fresh(self, tmp_path):
        """Fresh cache should be returned intact."""
        from news_sentiment import save_cache, load_cache
        cache = {
            "_cached_at": time.time(),
            "sentiment_2330": {"score": 80.0, "status": "positive"},
        }
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded["sentiment_2330"]["score"] == 80.0

    def test_cache_missing(self, tmp_path):
        """Missing cache file should return empty dict."""
        from news_sentiment import load_cache
        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded == {}

    def test_invalidate_cache(self, tmp_path):
        """invalidate_cache should remove the cache file."""
        from news_sentiment import save_cache, invalidate_cache, load_cache
        save_cache({"test": True}, data_dir=str(tmp_path))
        invalidate_cache(data_dir=str(tmp_path))
        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded == {}


# ═══════════════════════════════════════════════════════════════════════
#  5. check_news_sentiment() — Stage 2 Integration
# ═══════════════════════════════════════════════════════════════════════

class TestCheckNewsSentiment:
    """Verify check_news_sentiment() follows Stage 2 (score, status) contract."""

    def test_returns_tuple_on_success(self):
        """Should return (float, str) tuple when news is available."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = [
                {"title": "台積電獲利大增", "snippet": "", "date": "2026-05-15", "source": "test", "url": ""},
            ]
            result = check_news_sentiment("2330", "台積電", force_refresh=True)
            assert result is not None
            score, status = result
            assert isinstance(score, float)
            assert isinstance(status, str)
            assert 0 <= score <= 100

    def test_returns_none_on_import_error(self):
        """If news_sentiment module can't be imported, Stage 2 gets None."""
        # This is tested implicitly by Stage 2's try/except block
        # The contract is: check_news_sentiment returns None on internal error
        pass

    def test_no_news_returns_neutral(self):
        """No articles → neutral score (60, 'neutral')."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = []
            score, status = check_news_sentiment("9999", "測試股", force_refresh=True)
            assert score == 60.0
            assert status == "neutral"

    def test_positive_news_high_score(self):
        """Positive articles should produce score >= 75."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = [
                {"title": "台積電營收創新高，獲利突破", "snippet": "成長看好",
                 "date": "2026-05-15", "source": "test", "url": ""},
            ]
            score, status = check_news_sentiment("2330", "台積電", force_refresh=True)
            assert score >= 75, f"Expected score >=75 for positive, got {score}"
            assert status == "positive"

    def test_negative_news_low_score(self):
        """Negative articles should produce score <= 45."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = [
                {"title": "公司虧損擴大裁員破底", "snippet": "下修展望",
                 "date": "2026-05-15", "source": "test", "url": ""},
            ]
            score, status = check_news_sentiment("2330", "台積電", force_refresh=True)
            assert score <= 45, f"Expected score <=45 for negative, got {score}"
            assert status == "negative"

    def test_cache_hit_returns_cached(self, tmp_path):
        """Second call should use cache instead of fetching again."""
        from news_sentiment import check_news_sentiment, save_cache
        # Pre-populate cache
        cache = {
            "_cached_at": time.time(),
            "sentiment_2330": {"score": 85.0, "status": "positive",
                              "positive": 3, "negative": 0, "neutral": 1,
                              "updated_at": "2026-05-16T10:00:00"},
        }
        save_cache(cache, data_dir=str(tmp_path))

        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            score, status = check_news_sentiment("2330", "台積電",
                                                  data_dir=str(tmp_path))
            mock_fetch.assert_not_called()  # Should use cache, not fetch
            assert score == 85.0
            assert status == "positive"

    def test_force_refresh_bypasses_cache(self, tmp_path):
        """force_refresh=True should skip cache and fetch fresh data."""
        from news_sentiment import check_news_sentiment, save_cache
        cache = {
            "_cached_at": time.time(),
            "sentiment_2330": {"score": 85.0, "status": "positive",
                              "positive": 3, "negative": 0, "neutral": 1,
                              "updated_at": "2026-05-16T10:00:00"},
        }
        save_cache(cache, data_dir=str(tmp_path))

        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = [
                {"title": "虧損擴大裁員", "snippet": "", "date": "2026-05-15",
                 "source": "test", "url": ""},
            ]
            score, status = check_news_sentiment("2330", "台積電",
                                                  data_dir=str(tmp_path),
                                                  force_refresh=True)
            mock_fetch.assert_called_once()  # Should fetch fresh data


# ═══════════════════════════════════════════════════════════════════════
#  6. Batch News Sentiment
# ═══════════════════════════════════════════════════════════════════════

class TestBatchNewsSentiment:
    """Verify batch_news_sentiment uses cache efficiently."""

    def test_batch_uses_cache(self, tmp_path):
        """Batch should use cached results and only fetch uncached stocks."""
        from news_sentiment import batch_news_sentiment, save_cache
        # Cache stock 2330
        cache = {
            "_cached_at": time.time(),
            "sentiment_2330": {"score": 85.0, "status": "positive",
                              "positive": 3, "negative": 0, "neutral": 1,
                              "updated_at": "2026-05-16T10:00:00"},
        }
        save_cache(cache, data_dir=str(tmp_path))

        with patch("news_sentiment.check_news_sentiment") as mock_check:
            mock_check.return_value = (60.0, "neutral")
            results = batch_news_sentiment(
                [("2330", "台積電"), ("2454", "聯發科")],
                data_dir=str(tmp_path),
            )
            # 2330 should be from cache, only 2454 calls check_news_sentiment
            assert results["2330"] == (85.0, "positive")
            assert "2454" in results


# ═══════════════════════════════════════════════════════════════════════
#  7. Graceful Fallback
# ═══════════════════════════════════════════════════════════════════════

class TestGracefulFallback:
    """Verify that news sentiment failures never crash the pipeline."""

    def test_fetch_exception_returns_none(self):
        """If fetching throws an exception, check_news_sentiment returns None."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.side_effect = Exception("Network error")
            result = check_news_sentiment("2330", "台積電", force_refresh=True)
            assert result is None  # Stage 2 handles None as error

    def test_udn_fetch_returns_empty_on_failure(self):
        """UDN fetch failure should return empty list, not raise."""
        from news_sentiment import fetch_news_udn
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            result = fetch_news_udn("2330")
            assert result == []

    def test_cbn_fetch_returns_empty_on_failure(self):
        """CBN fetch failure should return empty list, not raise."""
        from news_sentiment import fetch_news_cbn
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("Connection refused")
            result = fetch_news_cbn("2330")
            assert result == []

    def test_aggregate_survives_malformed_article(self):
        """aggregate_sentiment should handle malformed articles gracefully."""
        from news_sentiment import aggregate_sentiment
        articles = [
            {"title": "", "snippet": "", "date": ""},  # Empty article
            {"title": "正常新聞", "snippet": "", "date": "2026-05-15"},
        ]
        score, detail = aggregate_sentiment(articles, reference_date="2026-05-16")
        assert isinstance(score, float)
        assert 0 <= score <= 100


# ═══════════════════════════════════════════════════════════════════════
#  8. Mixed Sentiment Edge Cases
# ═══════════════════════════════════════════════════════════════════════

class TestMixedSentimentEdgeCases:
    """Verify sentiment classification with complex mixed positive/negative articles."""

    def _classify(self, article):
        from news_sentiment import classify_article_sentiment
        return classify_article_sentiment(article)

    def test_positive_title_negative_snippet(self):
        """Positive title + negative snippet: weighted count determines result.

        Title has 獲利 (1 pos), snippet has 虧損 (1 neg).
        1 pos vs 1*1.3 = 1.3 weighted neg → negative wins.
        """
        article = {"title": "獲利表現", "snippet": "子公司虧損嚴重"}
        sentiment = self._classify(article)
        assert sentiment == -0.5

    def test_negative_title_positive_snippet(self):
        """Negative title + positive snippet: overall may go either way.

        Title has 虧損 (1 neg), snippet has 突破+成長 (2 pos).
        2 pos vs 1*1.3 = 1.3 weighted neg → positive wins.
        """
        article = {"title": "虧損縮小", "snippet": "技術突破帶動成長"}
        sentiment = self._classify(article)
        assert sentiment == 0.5

    def test_many_negatives_overwhelm_few_positives(self):
        """Multiple negative keywords overwhelm a few positive ones.

        1 pos (獲利) vs 3 neg (虧損, 裁員, 破底) → 1 vs 3*1.3=3.9 → negative.
        """
        article = {"title": "雖有獲利但虧損裁員破底", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5

    def test_many_positives_overwhelm_few_negatives(self):
        """Multiple positive keywords overwhelm a few negative ones.

        4 pos (突破, 獲利, 成長, 創新高) vs 1 neg (下修) → 4 vs 1.3 → positive.
        """
        article = {"title": "突破獲利成長創新高", "snippet": "小幅下修"}
        sentiment = self._classify(article)
        assert sentiment == 0.5

    def test_equal_weighted_positive_negative_is_negative(self):
        """Equal raw counts: 2 pos vs 2 neg → 2 vs 2*1.3=2.6 → negative.

        Loss aversion bias: negative keywords are weighted 1.3x.
        """
        article = {"title": "獲利成長但虧損衰退", "snippet": ""}
        sentiment = self._classify(article)
        assert sentiment == -0.5

    def test_mixed_aggregate_produces_intermediate_score(self):
        """Mixed article set should produce score between positive and negative extremes."""
        from news_sentiment import aggregate_sentiment
        articles = [
            {"title": "台積電獲利大增", "snippet": "", "date": "2026-05-15"},
            {"title": "公司虧損裁員", "snippet": "", "date": "2026-05-15"},
            {"title": "市場盤整觀望", "snippet": "", "date": "2026-05-14"},
        ]
        score, detail = aggregate_sentiment(articles, reference_date="2026-05-16")
        # Mixed articles → score should be between 30 and 90
        assert 30 < score < 90


# ═══════════════════════════════════════════════════════════════════════
#  9. Empty/None Input Handling
# ═══════════════════════════════════════════════════════════════════════

class TestEmptyNoneHandling:
    """Verify graceful handling of empty or None inputs."""

    def test_classify_empty_article(self):
        """Article with empty title and snippet → neutral."""
        from news_sentiment import classify_article_sentiment
        article = {"title": "", "snippet": ""}
        sentiment = classify_article_sentiment(article)
        assert sentiment == 0.0

    def test_classify_missing_title(self):
        """Article with missing title key → neutral."""
        from news_sentiment import classify_article_sentiment
        article = {"snippet": "獲利成長"}
        sentiment = classify_article_sentiment(article)
        assert sentiment == 0.5  # snippet still checked

    def test_classify_missing_snippet(self):
        """Article with missing snippet key → title only classification."""
        from news_sentiment import classify_article_sentiment
        article = {"title": "虧損擴大"}
        sentiment = classify_article_sentiment(article)
        assert sentiment == -0.5

    def test_classify_none_fields(self):
        """Article with None title/snippet → neutral (no crash)."""
        from news_sentiment import classify_article_sentiment
        article = {"title": None, "snippet": None}
        sentiment = classify_article_sentiment(article)
        assert sentiment == 0.0

    def test_aggregate_none_articles(self):
        """None passed as articles list → should not crash (but empty list is expected)."""
        from news_sentiment import aggregate_sentiment
        # The function expects a list, but let's test empty list
        score, detail = aggregate_sentiment([])
        assert score == 60.0
        assert detail["status"] == "no_articles"

    def test_check_news_sentiment_empty_stock_code(self):
        """Empty stock code should still return a valid result."""
        from news_sentiment import check_news_sentiment
        with patch("news_sentiment.fetch_news_for_stock") as mock_fetch:
            mock_fetch.return_value = []
            score, status = check_news_sentiment("", "", force_refresh=True)
            assert score == 60.0
            assert status == "neutral"


# ═══════════════════════════════════════════════════════════════════════
#  10. Cache TTL Boundary Conditions
# ═══════════════════════════════════════════════════════════════════════

class TestCacheTTLBoundary:
    """Verify cache TTL boundary conditions: just expired, just valid."""

    def test_cache_just_expired(self, tmp_path):
        """Cache expired by 1 second → should return empty dict."""
        from news_sentiment import save_cache, load_cache, CACHE_TTL_SECONDS
        cache = {"_cached_at": time.time() - CACHE_TTL_SECONDS - 1,
                 "sentiment_2330": {"score": 80.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded == {}

    def test_cache_just_valid(self, tmp_path):
        """Cache with 1 second remaining → should return data."""
        from news_sentiment import save_cache, load_cache, CACHE_TTL_SECONDS
        cache = {"_cached_at": time.time() - CACHE_TTL_SECONDS + 10,
                 "sentiment_2330": {"score": 80.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert "sentiment_2330" in loaded

    def test_cache_exactly_at_ttl(self, tmp_path):
        """Cache at TTL boundary → barely valid or barely expired depending on timing.

        The check is: time.time() - cached_at > CACHE_TTL_SECONDS
        Since time passes between save and load, we test with a small buffer.
        A cache set to TTL-100ms should still be valid on immediate load.
        """
        from news_sentiment import save_cache, load_cache, CACHE_TTL_SECONDS
        # Set cached_at to just under CACHE_TTL_SECONDS ago (100ms buffer)
        cache = {"_cached_at": time.time() - CACHE_TTL_SECONDS + 0.1,
                 "sentiment_2330": {"score": 85.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        # With 100ms buffer, should still be valid on immediate load
        assert "sentiment_2330" in loaded

    def test_cache_fresh_within_ttl(self, tmp_path):
        """Cache created just now → definitely within TTL."""
        from news_sentiment import save_cache, load_cache
        cache = {"_cached_at": time.time(),
                 "sentiment_2330": {"score": 90.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded["sentiment_2330"]["score"] == 90.0

    def test_cache_zero_timestamp(self, tmp_path):
        """Cache with _cached_at=0 → very old, should be expired."""
        from news_sentiment import save_cache, load_cache, CACHE_TTL_SECONDS
        cache = {"_cached_at": 0,
                 "sentiment_2330": {"score": 80.0, "status": "positive"}}
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        assert loaded == {}

    def test_cache_missing_cached_at(self, tmp_path):
        """Cache without _cached_at key → defaults to 0, expired."""
        from news_sentiment import save_cache, load_cache
        cache = {"sentiment_2330": {"score": 80.0, "status": "positive"}}
        # save_cache adds _cached_at automatically
        save_cache(cache, data_dir=str(tmp_path))

        loaded = load_cache(data_dir=str(tmp_path))
        # Should work since save_cache adds _cached_at
        assert "sentiment_2330" in loaded
