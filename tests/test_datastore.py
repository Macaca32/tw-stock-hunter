"""Unit tests for core/datastore.py — SQLite data layer.

Phase 23: Tests migration, query helpers, and idempotency.
All data is inline — no API calls, no real data files needed.
"""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  Fixtures
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture
def data_dir(tmp_path):
    """Create a temporary data directory with sample JSON files."""
    # price_history.json — 3 stocks, 5 days each
    price_history = {
        "2330": [
            {"date": "2026-05-01", "open": 980, "high": 990, "low": 975, "close": 985, "volume": 15000000, "adj_close": 980.5, "adj_volume": 14900000},
            {"date": "2026-05-02", "open": 985, "high": 995, "low": 980, "close": 990, "volume": 14500000, "adj_close": 985.5, "adj_volume": 14400000},
            {"date": "2026-05-05", "open": 990, "high": 1000, "low": 985, "close": 995, "volume": 16000000, "adj_close": 990.5, "adj_volume": 15900000},
            {"date": "2026-05-06", "open": 995, "high": 1005, "low": 990, "close": 1000, "volume": 15500000, "adj_close": 995.5, "adj_volume": 15400000},
            {"date": "2026-05-07", "open": 1000, "high": 1010, "low": 995, "close": 1005, "volume": 14800000, "adj_close": 1000.5, "adj_volume": 14700000},
        ],
        "2317": [
            {"date": "2026-05-01", "open": 150, "high": 153, "low": 149, "close": 152, "volume": 50000000, "adj_close": 151.8, "adj_volume": 49900000},
            {"date": "2026-05-02", "open": 152, "high": 155, "low": 151, "close": 154, "volume": 48000000, "adj_close": 153.8, "adj_volume": 47900000},
            {"date": "2026-05-05", "open": 154, "high": 156, "low": 153, "close": 155, "volume": 52000000, "adj_close": 154.8, "adj_volume": 51900000},
            {"date": "2026-05-06", "open": 155, "high": 157, "low": 154, "close": 156, "volume": 51000000, "adj_close": 155.8, "adj_volume": 50900000},
            {"date": "2026-05-07", "open": 156, "high": 158, "low": 155, "close": 157, "volume": 49000000, "adj_close": 156.8, "adj_volume": 48900000},
        ],
        "2454": [
            {"date": "2026-05-01", "open": 620, "high": 630, "low": 615, "close": 625, "volume": 8000000, "adj_close": 622.0, "adj_volume": 7950000},
            {"date": "2026-05-02", "open": 625, "high": 635, "low": 620, "close": 630, "volume": 7500000, "adj_close": 627.0, "adj_volume": 7450000},
            {"date": "2026-05-05", "open": 630, "high": 640, "low": 625, "close": 635, "volume": 8500000, "adj_close": 632.0, "adj_volume": 8450000},
            {"date": "2026-05-06", "open": 635, "high": 645, "low": 630, "close": 640, "volume": 7800000, "adj_close": 637.0, "adj_volume": 7750000},
            {"date": "2026-05-07", "open": 640, "high": 650, "low": 635, "close": 645, "volume": 7200000, "adj_close": 642.0, "adj_volume": 7150000},
        ],
    }
    (tmp_path / "price_history.json").write_text(
        json.dumps(price_history, ensure_ascii=False), encoding="utf-8"
    )

    # twt49u_ex_dividend.json — corporate actions
    corp_actions = {
        "2330": [
            {"date": "2026-07-15", "cash_div": 4.5, "stock_div": 0.5, "source": "twt49u"},
        ],
        "2317": [
            {"date": "2026-08-01", "cash_div": 5.2, "stock_div": 0.0, "source": "twt49u"},
        ],
    }
    (tmp_path / "twt49u_ex_dividend.json").write_text(
        json.dumps(corp_actions, ensure_ascii=False), encoding="utf-8"
    )

    # regime.json — current market regime
    regime_data = {
        "regime": "normal",
        "raw_regime": "normal",
        "days_in_regime": 5,
        "volatility": 0.0125,
        "timestamp": "2026-05-07T11:30:00",
        "confidence": "high",
    }
    (tmp_path / "regime.json").write_text(
        json.dumps(regime_data, ensure_ascii=False), encoding="utf-8"
    )

    # daily_2026-05-07.json — for TAIEX proxy computation
    daily_data = [
        {"Code": "2330", "ClosingPrice": "1005", "TradeVolume": "14800000", "Change": "5.0"},
        {"Code": "2317", "ClosingPrice": "157", "TradeVolume": "49000000", "Change": "1.0"},
        {"Code": "2454", "ClosingPrice": "645", "TradeVolume": "7200000", "Change": "5.0"},
    ]
    (tmp_path / "daily_2026-05-07.json").write_text(
        json.dumps(daily_data, ensure_ascii=False), encoding="utf-8"
    )

    # paper_trades.json — paper trading history
    paper_trades = [
        {
            "code": "2330",
            "name": "台積電",
            "entry_date": "2026-05-01",
            "entry_price": 985.0,
            "exit_date": "2026-05-07",
            "exit_price": 1005.0,
            "exit_reason": "take_profit",
            "pnl_pct": 1.43,
            "status": "closed",
        },
        {
            "code": "2454",
            "name": "聯發科",
            "entry_date": "2026-05-05",
            "entry_price": 635.0,
            "exit_price": None,
            "exit_reason": None,
            "pnl_pct": None,
            "status": "open",
        },
    ]
    (tmp_path / "paper_trades.json").write_text(
        json.dumps(paper_trades, ensure_ascii=False), encoding="utf-8"
    )

    return tmp_path


@pytest.fixture
def migrated_db(data_dir):
    """Run migration and return the data_dir for queries."""
    from datastore import migrate_json_to_sqlite
    migrate_json_to_sqlite(data_dir=str(data_dir), verbose=False)
    return data_dir


# ═══════════════════════════════════════════════════════════════════════
#  1. Schema Initialization
# ═══════════════════════════════════════════════════════════════════════

class TestSchemaInit:
    """Verify database schema creation and versioning."""

    def test_db_file_created(self, data_dir):
        """init_db should create the hunter.db file."""
        from datastore import init_db
        conn = init_db(str(data_dir))
        conn.close()
        db_path = data_dir / "hunter.db"
        assert db_path.exists()

    def test_tables_created(self, data_dir):
        """All four data tables plus _meta should exist."""
        from datastore import init_db
        conn = init_db(str(data_dir))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {r[0] for r in cursor}
        conn.close()
        expected = {"_meta", "corporate_actions", "portfolio_history",
                    "regime_snapshots", "stocks_daily"}
        assert expected.issubset(tables)

    def test_schema_version_set(self, data_dir):
        """Schema version should be recorded in _meta."""
        from datastore import init_db, SCHEMA_VERSION
        conn = init_db(str(data_dir))
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'schema_version'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert int(row[0]) == SCHEMA_VERSION

    def test_indexes_created(self, data_dir):
        """Critical indexes should exist for query performance."""
        from datastore import init_db
        conn = init_db(str(data_dir))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
        indexes = {r[0] for r in cursor}
        conn.close()
        # At minimum, these indexes should exist
        assert "idx_stocks_daily_date" in indexes
        assert "idx_stocks_daily_stock_date_desc" in indexes
        assert "idx_corp_actions_ex_date" in indexes
        assert "idx_portfolio_date" in indexes


# ═══════════════════════════════════════════════════════════════════════
#  2. Migration: stocks_daily
# ═══════════════════════════════════════════════════════════════════════

class TestMigrationStocksDaily:
    """Verify price_history.json → stocks_daily migration."""

    def test_rows_inserted(self, migrated_db):
        """All 15 rows (3 stocks × 5 days) should be inserted."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        count = conn.execute("SELECT COUNT(*) FROM stocks_daily").fetchone()[0]
        conn.close()
        assert count == 15

    def test_data_integrity(self, migrated_db):
        """Spot-check a known row for correct values."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        row = conn.execute(
            "SELECT close, adj_close, volume FROM stocks_daily WHERE stock_id='2330' AND date='2026-05-01'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert abs(row[0] - 985) < 0.01
        assert abs(row[1] - 980.5) < 0.01
        assert abs(row[2] - 15000000) < 0.01

    def test_idempotent_migration(self, data_dir):
        """Running migration twice should not duplicate rows."""
        from datastore import migrate_json_to_sqlite, get_connection
        stats1 = migrate_json_to_sqlite(data_dir=str(data_dir))
        stats2 = migrate_json_to_sqlite(data_dir=str(data_dir))

        conn = get_connection(str(data_dir), readonly=True)
        count = conn.execute("SELECT COUNT(*) FROM stocks_daily").fetchone()[0]
        conn.close()

        # Second run should have 0 inserts (all skipped)
        assert stats2["stocks_daily_inserted"] == 0
        assert count == 15  # Same count as first run

    def test_distinct_stocks(self, migrated_db):
        """Three distinct stock IDs should be present."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        count = conn.execute(
            "SELECT COUNT(DISTINCT stock_id) FROM stocks_daily"
        ).fetchone()[0]
        conn.close()
        assert count == 3


# ═══════════════════════════════════════════════════════════════════════
#  3. Migration: corporate_actions
# ═══════════════════════════════════════════════════════════════════════

class TestMigrationCorporateActions:
    """Verify corporate actions migration from twt49u and dividends JSON."""

    def test_twt49u_actions_inserted(self, migrated_db):
        """Two TWT49U corporate actions should be present."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM corporate_actions WHERE source='twt49u'"
        ).fetchone()[0]
        conn.close()
        assert count == 2

    def test_cash_dividend_value(self, migrated_db):
        """2330's cash dividend should be 4.5."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        row = conn.execute(
            "SELECT cash_div FROM corporate_actions WHERE stock_id='2330' AND ex_date='2026-07-15'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert abs(row[0] - 4.5) < 0.01

    def test_idempotent_corporate_actions(self, data_dir):
        """Second migration should not duplicate corporate actions."""
        from datastore import migrate_json_to_sqlite, get_connection
        migrate_json_to_sqlite(data_dir=str(data_dir))
        stats2 = migrate_json_to_sqlite(data_dir=str(data_dir))

        conn = get_connection(str(data_dir), readonly=True)
        count = conn.execute("SELECT COUNT(*) FROM corporate_actions").fetchone()[0]
        conn.close()

        assert stats2["corporate_actions_inserted"] == 0
        assert count == 2  # Still only 2 actions

    def test_dividends_file_migration(self, data_dir):
        """dividends_*.json should also populate corporate_actions."""
        # Create a dividends file with estimated ex-date
        div_data = [
            {
                "公司代號": "1101",
                "股東會日期": "1150605",  # ROC 115/06/05 = 2026-06-05
                "股東配發-盈餘分配之現金股利(元/股)": "2.0",
                "股東配發-盈餘轉增資配股(元/股)": "1.0",
                "決議（擬議）進度": "通過",
            }
        ]
        (data_dir / "dividends_2026-05-07.json").write_text(
            json.dumps(div_data, ensure_ascii=False), encoding="utf-8"
        )

        from datastore import migrate_json_to_sqlite, get_connection
        stats = migrate_json_to_sqlite(data_dir=str(data_dir))

        conn = get_connection(str(data_dir), readonly=True)
        row = conn.execute(
            "SELECT cash_div, stock_div, source FROM corporate_actions WHERE stock_id='1101'"
        ).fetchone()
        conn.close()

        assert row is not None
        assert abs(row[0] - 2.0) < 0.01
        assert abs(row[1] - 1.0) < 0.01
        assert row[2] == "dividend_declaration"


# ═══════════════════════════════════════════════════════════════════════
#  4. Migration: regime_snapshots
# ═══════════════════════════════════════════════════════════════════════

class TestMigrationRegimeSnapshots:
    """Verify regime.json → regime_snapshots migration."""

    def test_regime_inserted(self, migrated_db):
        """Current regime should be inserted."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        row = conn.execute(
            "SELECT regime_label FROM regime_snapshots WHERE date='2026-05-07'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "normal"

    def test_taiex_proxy_computed(self, migrated_db):
        """TAIEX proxy should be computed from daily data."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        row = conn.execute(
            "SELECT taiex_close, taiex_change_pct FROM regime_snapshots WHERE date='2026-05-07'"
        ).fetchone()
        conn.close()
        assert row is not None
        # TAIEX proxy is average of top stocks' close
        assert row[0] is not None and row[0] > 0


# ═══════════════════════════════════════════════════════════════════════
#  5. Migration: portfolio_history
# ═══════════════════════════════════════════════════════════════════════

class TestMigrationPortfolioHistory:
    """Verify paper_trades.json → portfolio_history migration."""

    def test_entry_exit_rows_created(self, migrated_db):
        """Closed trade → 2 rows (entry + exit), open trade → 1 row (entry only)."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        count = conn.execute("SELECT COUNT(*) FROM portfolio_history").fetchone()[0]
        conn.close()
        assert count == 3  # 2 (2330 entry+exit) + 1 (2454 entry)

    def test_entry_action(self, migrated_db):
        """Entry rows should have action='entry'."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        entries = conn.execute(
            "SELECT COUNT(*) FROM portfolio_history WHERE action='entry'"
        ).fetchone()[0]
        conn.close()
        assert entries == 2

    def test_exit_with_pnl(self, migrated_db):
        """Exit row should have P&L calculated from entry_price * pnl_pct / 100."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        row = conn.execute(
            "SELECT pnl FROM portfolio_history WHERE stock_id='2330' AND action='exit'"
        ).fetchone()
        conn.close()
        assert row is not None
        # entry_price=985, pnl_pct=1.43 → pnl = 985 * 1.43 / 100 ≈ 14.0855
        assert abs(row[0] - 14.0855) < 0.01

    def test_idempotent_portfolio(self, data_dir):
        """Second migration should not duplicate portfolio entries."""
        from datastore import migrate_json_to_sqlite, get_connection
        migrate_json_to_sqlite(data_dir=str(data_dir))
        stats2 = migrate_json_to_sqlite(data_dir=str(data_dir))

        conn = get_connection(str(data_dir), readonly=True)
        count = conn.execute("SELECT COUNT(*) FROM portfolio_history").fetchone()[0]
        conn.close()

        assert stats2["portfolio_history_inserted"] == 0
        assert count == 3


# ═══════════════════════════════════════════════════════════════════════
#  6. Query Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestGetDailyHistory:
    """Test get_daily_history query helper."""

    def test_basic_query(self, migrated_db):
        """Should return all available days for a stock."""
        from datastore import get_daily_history
        rows = get_daily_history("2330", data_dir=str(migrated_db))
        assert len(rows) == 5
        assert rows[0]["date"] == "2026-05-01"
        assert rows[-1]["date"] == "2026-05-07"

    def test_since_date_filter(self, migrated_db):
        """Should only return rows on or after since_date."""
        from datastore import get_daily_history
        rows = get_daily_history("2330", since_date="2026-05-05", data_dir=str(migrated_db))
        assert len(rows) == 3
        assert rows[0]["date"] == "2026-05-05"

    def test_limit(self, migrated_db):
        """Should respect the limit parameter."""
        from datastore import get_daily_history
        rows = get_daily_history("2330", limit=2, data_dir=str(migrated_db))
        # Without since_date, returns most recent 2 rows
        assert len(rows) == 2
        # Should be the latest 2 dates in ASC order
        assert rows[0]["date"] == "2026-05-06"
        assert rows[1]["date"] == "2026-05-07"

    def test_nonexistent_stock(self, migrated_db):
        """Querying a stock that doesn't exist should return empty list."""
        from datastore import get_daily_history
        rows = get_daily_history("9999", data_dir=str(migrated_db))
        assert rows == []

    def test_result_keys(self, migrated_db):
        """Each row should have the expected column keys."""
        from datastore import get_daily_history
        rows = get_daily_history("2330", data_dir=str(migrated_db))
        expected_keys = {"stock_id", "date", "open", "high", "low", "close",
                         "volume", "adj_close", "adj_volume"}
        assert set(rows[0].keys()) == expected_keys


class TestGetRegimeHistory:
    """Test get_regime_history query helper."""

    def test_basic_query(self, migrated_db):
        """Should return regime snapshots."""
        from datastore import get_regime_history
        rows = get_regime_history(data_dir=str(migrated_db))
        assert len(rows) >= 1
        assert rows[0]["regime_label"] == "normal"

    def test_since_date_filter(self, migrated_db):
        """Should filter by start date."""
        from datastore import get_regime_history
        rows = get_regime_history(since_date="2026-05-08", data_dir=str(migrated_db))
        assert len(rows) == 0  # No data after May 7


class TestGetPortfolioPnl:
    """Test get_portfolio_pnl query helper."""

    def test_basic_query(self, migrated_db):
        """Should return all portfolio entries."""
        from datastore import get_portfolio_pnl
        rows = get_portfolio_pnl(data_dir=str(migrated_db))
        assert len(rows) == 3

    def test_date_range_filter(self, migrated_db):
        """Should filter by date range."""
        from datastore import get_portfolio_pnl
        rows = get_portfolio_pnl(
            date_range=("2026-05-01", "2026-05-06"),
            data_dir=str(migrated_db),
        )
        # Should include entries on 5/1, 5/5, 5/6 but NOT 5/7
        dates = {r["date"] for r in rows}
        assert "2026-05-07" not in dates


class TestGetCorporateActions:
    """Test get_corporate_actions query helper."""

    def test_all_actions(self, migrated_db):
        """Should return all corporate actions when no filter."""
        from datastore import get_corporate_actions
        rows = get_corporate_actions(data_dir=str(migrated_db))
        assert len(rows) == 2

    def test_filter_by_stock(self, migrated_db):
        """Should filter by stock_id."""
        from datastore import get_corporate_actions
        rows = get_corporate_actions(stock_id="2330", data_dir=str(migrated_db))
        assert len(rows) == 1
        assert rows[0]["stock_id"] == "2330"

    def test_filter_by_date(self, migrated_db):
        """Should filter by since_date."""
        from datastore import get_corporate_actions
        rows = get_corporate_actions(since_date="2026-08-01", data_dir=str(migrated_db))
        assert len(rows) == 1
        assert rows[0]["stock_id"] == "2317"


class TestGetStocksOnDate:
    """Test get_stocks_on_date cross-sectional query."""

    def test_returns_all_stocks(self, migrated_db):
        """Should return data for all stocks on a given date."""
        from datastore import get_stocks_on_date
        rows = get_stocks_on_date("2026-05-01", data_dir=str(migrated_db))
        assert len(rows) == 3

    def test_nonexistent_date(self, migrated_db):
        """Should return empty list for date with no data."""
        from datastore import get_stocks_on_date
        rows = get_stocks_on_date("2020-01-01", data_dir=str(migrated_db))
        assert rows == []


class TestGetAvailableDates:
    """Test get_available_dates query."""

    def test_returns_dates(self, migrated_db):
        """Should return list of dates, most recent first."""
        from datastore import get_available_dates
        dates = get_available_dates(data_dir=str(migrated_db))
        assert len(dates) == 5
        assert dates[0] == "2026-05-07"  # Most recent first


class TestGetStockDateRange:
    """Test get_stock_date_range query."""

    def test_returns_range(self, migrated_db):
        """Should return (min_date, max_date) for a stock."""
        from datastore import get_stock_date_range
        result = get_stock_date_range("2330", data_dir=str(migrated_db))
        assert result is not None
        assert result[0] == "2026-05-01"
        assert result[1] == "2026-05-07"

    def test_nonexistent_stock(self, migrated_db):
        """Should return None for stock not in DB."""
        from datastore import get_stock_date_range
        result = get_stock_date_range("9999", data_dir=str(migrated_db))
        assert result is None


class TestGetDailyHistoryBatch:
    """Test get_daily_history_batch multi-stock query."""

    def test_batch_query(self, migrated_db):
        """Should return data for all requested stocks."""
        from datastore import get_daily_history_batch
        result = get_daily_history_batch(
            ["2330", "2317"], data_dir=str(migrated_db)
        )
        assert "2330" in result
        assert "2317" in result
        assert len(result["2330"]) == 5
        assert len(result["2317"]) == 5

    def test_empty_stock_list(self, migrated_db):
        """Should return empty dict for empty stock list."""
        from datastore import get_daily_history_batch
        result = get_daily_history_batch([], data_dir=str(migrated_db))
        assert result == {}


# ═══════════════════════════════════════════════════════════════════════
#  7. Database Info / Diagnostics
# ═══════════════════════════════════════════════════════════════════════

class TestDbInfo:
    """Test db_info diagnostic function."""

    def test_info_before_migration(self, data_dir):
        """Should report exists=False before migration."""
        from datastore import db_info
        info = db_info(str(data_dir))
        assert info["exists"] is False

    def test_info_after_migration(self, migrated_db):
        """Should report row counts and date range."""
        from datastore import db_info
        info = db_info(str(migrated_db))
        assert info["exists"] is True
        assert info["stocks_daily_rows"] == 15
        assert info["corporate_actions_rows"] == 2
        assert info["regime_snapshots_rows"] == 1
        assert info["portfolio_history_rows"] == 3
        assert info["distinct_stocks"] == 3
        assert info["stocks_daily_date_range"] == ("2026-05-01", "2026-05-07")


# ═══════════════════════════════════════════════════════════════════════
#  8. Edge Cases
# ═══════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_data_dir(self, tmp_path):
        """Migration with no JSON files should succeed with 0 rows."""
        from datastore import migrate_json_to_sqlite
        stats = migrate_json_to_sqlite(data_dir=str(tmp_path), verbose=False)
        assert stats["stocks_daily_inserted"] == 0
        assert stats["corporate_actions_inserted"] == 0
        assert stats["regime_snapshots_inserted"] == 0
        assert stats["portfolio_history_inserted"] == 0

    def test_malformed_json(self, data_dir):
        """Malformed JSON files should be skipped gracefully."""
        (data_dir / "price_history.json").write_text("not valid json{", encoding="utf-8")

        from datastore import migrate_json_to_sqlite
        stats = migrate_json_to_sqlite(data_dir=str(data_dir), verbose=False)
        # Should not crash, just skip the bad file
        assert stats["stocks_daily_inserted"] == 0

    def test_readonly_connection(self, migrated_db):
        """Readonly connection should not allow writes."""
        from datastore import get_connection
        conn = get_connection(str(migrated_db), readonly=True)
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO stocks_daily (stock_id, date) VALUES ('TEST', '2020-01-01')")
        conn.close()

    def test_historical_files_supplementary(self, data_dir):
        """historical_*.json should supplement price_history.json data."""
        # Create a historical file for a date NOT in price_history.json
        hist_data = [
            {"Code": "2330", "OpeningPrice": "950", "HighestPrice": "960",
             "LowestPrice": "945", "ClosingPrice": "955", "TradeVolume": "14000000"},
        ]
        (data_dir / "historical_2026-04-28.json").write_text(
            json.dumps(hist_data, ensure_ascii=False), encoding="utf-8"
        )

        from datastore import migrate_json_to_sqlite, get_connection
        migrate_json_to_sqlite(data_dir=str(data_dir), verbose=False)

        conn = get_connection(str(data_dir), readonly=True)
        row = conn.execute(
            "SELECT close FROM stocks_daily WHERE stock_id='2330' AND date='2026-04-28'"
        ).fetchone()
        conn.close()

        assert row is not None
        assert abs(row[0] - 955) < 0.01

    def test_special_share_classes_filtered(self, data_dir):
        """Stock codes ending in B or R should be filtered from dividends."""
        div_data = [
            {
                "公司代號": "1101B",  # Special share class — should be skipped
                "股東會日期": "1150605",
                "股東配發-盈餘分配之現金股利(元/股)": "3.0",
                "決議（擬議）進度": "通過",
            },
        ]
        (data_dir / "dividends_2026-05-07.json").write_text(
            json.dumps(div_data, ensure_ascii=False), encoding="utf-8"
        )

        from datastore import migrate_json_to_sqlite, get_connection
        migrate_json_to_sqlite(data_dir=str(data_dir), verbose=False)

        conn = get_connection(str(data_dir), readonly=True)
        row = conn.execute(
            "SELECT * FROM corporate_actions WHERE stock_id='1101B'"
        ).fetchone()
        conn.close()

        assert row is None  # Should not be inserted
