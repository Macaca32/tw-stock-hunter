#!/usr/bin/env python3
"""
SQLite Data Layer — Read-only view on top of existing JSON data files.

Phase 23: Provides a relational query interface over the flat JSON files
in data/. The SQLite database at data/hunter.db is populated from JSON
via migrate_json_to_sqlite() and acts as a read-optimized cache.

Key design decisions:
- JSON files remain the write target (backward compat).
- SQLite layer is read-only — no write-through.
- Migration is idempotent (safe to run multiple times).
- Tables use INSERT OR IGNORE to handle re-runs gracefully.

Tables:
  stocks_daily      — Daily OHLCV + adj_close/adj_volume per stock
  corporate_actions — Ex-dividend dates, cash/stock dividend amounts
  regime_snapshots  — Daily regime label + TAIEX proxy metrics
  portfolio_history — Paper trade entries/exits with P&L
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

DB_FILENAME = "hunter.db"
SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
#  Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
-- ── Table: stocks_daily ──────────────────────────────────────────────────
-- One row per (stock_id, date). Populated from price_history.json and
-- historical_*.json files.
CREATE TABLE IF NOT EXISTS stocks_daily (
    stock_id   TEXT    NOT NULL,
    date       TEXT    NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    adj_close  REAL,
    adj_volume REAL,
    PRIMARY KEY (stock_id, date)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_stocks_daily_date
    ON stocks_daily (date);

CREATE INDEX IF NOT EXISTS idx_stocks_daily_stock_date_desc
    ON stocks_daily (stock_id, date DESC);


-- ── Table: corporate_actions ─────────────────────────────────────────────
-- Ex-dividend / ex-rights events. Populated from twt49u_ex_dividend.json
-- and dividends_*.json (merged, TWT49U takes priority).
CREATE TABLE IF NOT EXISTS corporate_actions (
    stock_id  TEXT    NOT NULL,
    ex_date   TEXT    NOT NULL,
    cash_div  REAL    DEFAULT 0,
    stock_div REAL    DEFAULT 0,
    source    TEXT    DEFAULT '',
    PRIMARY KEY (stock_id, ex_date)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_corp_actions_ex_date
    ON corporate_actions (ex_date);


-- ── Table: regime_snapshots ──────────────────────────────────────────────
-- One row per trading day. Populated from regime.json and TAIEX proxy
-- (top-stock aggregate close / change_pct).
CREATE TABLE IF NOT EXISTS regime_snapshots (
    date            TEXT PRIMARY KEY,
    regime_label    TEXT    NOT NULL,
    taiex_close     REAL,
    taiex_change_pct REAL
) WITHOUT ROWID;


-- ── Table: portfolio_history ─────────────────────────────────────────────
-- Paper-trade ledger. Populated from paper_trades.json.
-- action is 'entry' or 'exit'.
CREATE TABLE IF NOT EXISTS portfolio_history (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    date      TEXT    NOT NULL,
    action    TEXT    NOT NULL CHECK(action IN ('entry', 'exit')),
    stock_id  TEXT    NOT NULL,
    quantity  REAL    DEFAULT 0,
    price     REAL    DEFAULT 0,
    pnl       REAL    DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_portfolio_date
    ON portfolio_history (date);

CREATE INDEX IF NOT EXISTS idx_portfolio_stock
    ON portfolio_history (stock_id);


-- ── Table: stage1_results ────────────────────────────────────────────────
-- Phase 25: One row per candidate that passed stage1 screening.
-- Populated after each pipeline run so stage2_deep can query via SQL
-- instead of reading JSON files.
CREATE TABLE IF NOT EXISTS stage1_results (
    run_date   TEXT    NOT NULL,
    stock_id   TEXT    NOT NULL,
    stock_name TEXT    DEFAULT '',
    close      REAL,
    composite_score REAL,
    revenue_score   REAL DEFAULT 25,
    profitability_score REAL DEFAULT 25,
    valuation_score REAL DEFAULT 25,
    flow_score      REAL DEFAULT 25,
    momentum_score  REAL DEFAULT 25,
    passed      INTEGER DEFAULT 1,
    PRIMARY KEY (run_date, stock_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_stage1_run_date
    ON stage1_results (run_date);

CREATE INDEX IF NOT EXISTS idx_stage1_stock
    ON stage1_results (stock_id);


-- ── Table: _meta ─────────────────────────────────────────────────────────
-- Internal versioning / migration tracking.
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# ---------------------------------------------------------------------------
#  Connection helpers
# ---------------------------------------------------------------------------

def get_db_path(data_dir: Optional[str] = None) -> Path:
    """Return the Path to hunter.db, defaulting to repo/data/hunter.db."""
    if data_dir is not None:
        return Path(data_dir) / DB_FILENAME
    return Path(__file__).parent.parent / "data" / DB_FILENAME


def get_connection(data_dir: Optional[str] = None, readonly: bool = True) -> sqlite3.Connection:
    """Open a connection to hunter.db.

    Args:
        data_dir:  Directory containing hunter.db. Defaults to repo/data/.
        readonly:  If True (default), open in read-only mode for safety.
                   Read-only connections skip WAL PRAGMA (not allowed on ro).

    Returns:
        sqlite3.Connection with row factory set to sqlite3.Row.
    """
    db_path = get_db_path(data_dir)

    if readonly:
        # Read-only: open with mode=ro URI. Skip WAL PRAGMA (requires write access).
        db_path.parent.mkdir(parents=True, exist_ok=True)
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        # WAL mode only on read-write connections
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass  # WAL not supported (e.g., tmpfs, NFS)

    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.OperationalError:
        pass
    return conn


def init_db(data_dir: Optional[str] = None) -> sqlite3.Connection:
    """Create tables if they don't exist and set schema version.

    Returns a read-write connection.
    """
    conn = get_connection(data_dir, readonly=False)
    conn.executescript(_SCHEMA_SQL)

    # Record schema version
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
#  Safe numeric conversion (mirrors fetch_history.safe_float)
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0) -> float:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
#  Migration: JSON → SQLite
# ---------------------------------------------------------------------------

def migrate_json_to_sqlite(data_dir: Optional[str] = None, verbose: bool = False) -> dict:
    """Read existing JSON files and populate the SQLite database.

    Idempotent: uses INSERT OR IGNORE so running multiple times is safe.
    Only inserts rows that don't already exist (by primary key).

    Returns a dict with migration statistics.
    """
    if data_dir is None:
        data_dir = str(Path(__file__).parent.parent / "data")
    else:
        data_dir = str(data_dir)

    data_path = Path(data_dir)
    conn = init_db(data_dir)
    stats = {
        "stocks_daily_inserted": 0,
        "stocks_daily_skipped": 0,
        "corporate_actions_inserted": 0,
        "corporate_actions_skipped": 0,
        "regime_snapshots_inserted": 0,
        "regime_snapshots_skipped": 0,
        "portfolio_history_inserted": 0,
        "portfolio_history_skipped": 0,
    }

    try:
        # ── 1. stocks_daily from price_history.json ──────────────────────
        _migrate_price_history(conn, data_path, stats, verbose)

        # ── 2. corporate_actions from twt49u_ex_dividend.json + dividends_*.json ──
        _migrate_corporate_actions(conn, data_path, stats, verbose)

        # ── 3. regime_snapshots from regime.json ─────────────────────────
        _migrate_regime_snapshots(conn, data_path, stats, verbose)

        # ── 4. portfolio_history from paper_trades.json ──────────────────
        _migrate_portfolio_history(conn, data_path, stats, verbose)

        # Record migration timestamp
        conn.execute(
            "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
            ("last_migration", datetime.now().isoformat()),
        )
        conn.commit()

    finally:
        conn.close()

    if verbose:
        logger.info("Migration complete: %s", stats)

    return stats


def _migrate_price_history(conn, data_path, stats, verbose):
    """Migrate price_history.json → stocks_daily table.

    price_history.json schema:
        { "2330": [ {"date": "2025-06-01", "close": 950, "open": 948,
                      "high": 955, "low": 945, "volume": 15000000,
                      "adj_close": 945.2, "adj_volume": 14800000}, ... ],
          ... }
    """
    ph_file = data_path / "price_history.json"
    if not ph_file.exists():
        if verbose:
            logger.info("price_history.json not found — skipping stocks_daily migration")
        return

    try:
        with open(ph_file, "r", encoding="utf-8") as f:
            price_history = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to read price_history.json: %s", e)
        return

    if not isinstance(price_history, dict):
        logger.warning("price_history.json is not a dict — skipping")
        return

    rows = []
    for stock_id, entries in price_history.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            date = entry.get("date", "")
            if not date:
                continue
            rows.append((
                stock_id,
                date,
                _safe_float(entry.get("open")),
                _safe_float(entry.get("high")),
                _safe_float(entry.get("low")),
                _safe_float(entry.get("close")),
                _safe_float(entry.get("volume")),
                _safe_float(entry.get("adj_close")),
                _safe_float(entry.get("adj_volume")),
            ))

    if not rows:
        return

    # Batch insert with INSERT OR IGNORE for idempotency
    result = conn.executemany(
        """INSERT OR IGNORE INTO stocks_daily
           (stock_id, date, open, high, low, close, volume, adj_close, adj_volume)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    stats["stocks_daily_inserted"] = result.rowcount
    stats["stocks_daily_skipped"] = len(rows) - result.rowcount

    if verbose:
        logger.info(
            "stocks_daily: %d inserted, %d skipped (already exist)",
            stats["stocks_daily_inserted"],
            stats["stocks_daily_skipped"],
        )

    # Also scan historical_*.json for any dates not in price_history.json
    # (price_history may be rebuilt daily and miss older historical files)
    _migrate_historical_files(conn, data_path, stats, verbose)


def _migrate_historical_files(conn, data_path, stats, verbose):
    """Migrate historical_*.json → stocks_daily (supplementary).

    These files contain raw (unadjusted) OHLCV data per trading day.
    Only inserts rows that don't already exist.
    """
    hist_files = sorted(data_path.glob("historical_*.json"))
    if not hist_files:
        return

    total_inserted = 0
    for filepath in hist_files:
        # Extract date from filename: historical_2025-06-01.json
        date_str = filepath.stem.replace("historical_", "")
        if len(date_str) != 10 or "-" not in date_str:
            continue

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        if not isinstance(data, list):
            continue

        rows = []
        for stock in data:
            code = stock.get("Code", stock.get("證券代號", ""))
            if not code:
                continue
            rows.append((
                code,
                date_str,
                _safe_float(stock.get("OpeningPrice", stock.get("開盤價", ""))),
                _safe_float(stock.get("HighestPrice", stock.get("最高價", ""))),
                _safe_float(stock.get("LowestPrice", stock.get("最低價", ""))),
                _safe_float(stock.get("ClosingPrice", stock.get("收盤價", ""))),
                _safe_float(stock.get("TradeVolume", stock.get("成交股數", ""))),
                # historical files don't have adjusted prices
                None,  # adj_close
                None,  # adj_volume
            ))

        if rows:
            result = conn.executemany(
                """INSERT OR IGNORE INTO stocks_daily
                   (stock_id, date, open, high, low, close, volume, adj_close, adj_volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            total_inserted += result.rowcount

    if total_inserted > 0 and verbose:
        logger.info("stocks_daily (historical files): %d additional rows inserted", total_inserted)
    stats["stocks_daily_inserted"] += total_inserted


def _migrate_corporate_actions(conn, data_path, stats, verbose):
    """Migrate corporate action data → corporate_actions table.

    Sources (in priority order):
    1. twt49u_ex_dividend.json — actual ex-dates with precise data
    2. dividends_*.json — dividend declarations with estimated ex-dates

    TWT49U takes priority: if both sources have the same (stock_id, ex_date),
    the TWT49U version is kept.
    """
    rows = []
    seen_keys = set()

    # Source 1: twt49u_ex_dividend.json (priority)
    twt49u_file = data_path / "twt49u_ex_dividend.json"
    if twt49u_file.exists():
        try:
            with open(twt49u_file, "r", encoding="utf-8") as f:
                twt49u_data = json.load(f)
            if isinstance(twt49u_data, dict):
                for stock_id, actions in twt49u_data.items():
                    if not isinstance(actions, list):
                        continue
                    for action in actions:
                        if not isinstance(action, dict):
                            continue
                        ex_date = action.get("date", "")
                        if not ex_date:
                            continue
                        key = (stock_id, ex_date)
                        seen_keys.add(key)
                        rows.append((
                            stock_id,
                            ex_date,
                            _safe_float(action.get("cash_div", 0)),
                            _safe_float(action.get("stock_div", 0)),
                            "twt49u",
                        ))
        except (json.JSONDecodeError, IOError) as e:
            logger.warning("Failed to read twt49u_ex_dividend.json: %s", e)

    # Source 2: dividends_*.json (supplementary, skip if TWT49U covers it)
    dividend_files = sorted(data_path.glob("dividends_*.json"))
    for div_file in dividend_files:
        try:
            with open(div_file, "r", encoding="utf-8") as f:
                div_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        if not isinstance(div_data, list):
            continue

        for item in div_data:
            code = item.get("公司代號", "")
            if not code:
                continue
            # Skip special share classes
            if code.endswith("B") or code.endswith("R"):
                continue

            # Sum cash dividend from all sources
            cash_div = 0.0
            for key in [
                "股東配發-盈餘分配之現金股利(元/股)",
                "股東配發-法定盈餘公積發放之現金(元/股)",
                "股東配發-資本公積發放之現金(元/股)",
            ]:
                cash_div += _safe_float(item.get(key, 0))

            # Sum stock dividend from all sources
            stock_div = 0.0
            for key in [
                "股東配發-盈餘轉增資配股(元/股)",
                "股東配發-法定盈餘公積轉增資配股(元/股)",
                "股東配發-資本公積轉增資配股(元/股)",
            ]:
                stock_div += _safe_float(item.get(key, 0))

            if cash_div <= 0 and stock_div <= 0:
                continue

            # Estimate ex-date from shareholder meeting date
            # (Same logic as corporate_actions.py)
            sh_date_str = item.get("股東會日期", "")
            ex_date = _estimate_ex_date(sh_date_str)
            if not ex_date:
                continue

            pk = (code, ex_date)
            if pk in seen_keys:
                # TWT49U already covers this — skip duplicate
                continue
            seen_keys.add(pk)
            rows.append((
                code,
                ex_date,
                round(cash_div, 4),
                round(stock_div, 4),
                "dividend_declaration",
            ))

    if not rows:
        return

    result = conn.executemany(
        """INSERT OR IGNORE INTO corporate_actions
           (stock_id, ex_date, cash_div, stock_div, source)
           VALUES (?, ?, ?, ?, ?)""",
        rows,
    )
    stats["corporate_actions_inserted"] = result.rowcount
    stats["corporate_actions_skipped"] = len(rows) - result.rowcount

    if verbose:
        logger.info(
            "corporate_actions: %d inserted, %d skipped",
            stats["corporate_actions_inserted"],
            stats["corporate_actions_skipped"],
        )


def _estimate_ex_date(sh_date_str: str) -> Optional[str]:
    """Estimate ex-dividend date from shareholder meeting date (ROC format).

    Mirrors CorporateActionHandler._estimate_ex_date().
    Taiwan convention: ex-date ≈ shareholder meeting + 7 days.
    """
    if not sh_date_str or len(sh_date_str) < 7:
        return None
    try:
        roc_year = int(sh_date_str[:3])
        month = int(sh_date_str[3:5])
        day = int(sh_date_str[5:7])
        year = roc_year + 1911
        from datetime import timedelta as _td
        sh_date = datetime(year, month, day)
        ex_date = sh_date + _td(days=7)
        return ex_date.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def _migrate_regime_snapshots(conn, data_path, stats, verbose):
    """Migrate regime.json → regime_snapshots table.

    regime.json is a single snapshot (not historical). We store it with
    today's date as the key. On re-runs, it updates the existing row.

    Also scan for historical regime data if available in the future.
    For now, we also compute a TAIEX proxy from daily_*.json files.
    """
    regime_file = data_path / "regime.json"
    if not regime_file.exists():
        if verbose:
            logger.info("regime.json not found — skipping regime_snapshots migration")
        return

    try:
        with open(regime_file, "r", encoding="utf-8") as f:
            regime_data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to read regime.json: %s", e)
        return

    if not isinstance(regime_data, dict):
        return

    regime_label = regime_data.get("regime", "unknown")
    timestamp = regime_data.get("timestamp", datetime.now().isoformat())

    # Extract date from timestamp or use today
    try:
        date_str = timestamp[:10]
    except (ValueError, IndexError):
        date_str = datetime.now().strftime("%Y-%m-%d")

    # Try to compute TAIEX proxy from the most recent daily_*.json
    taiex_close, taiex_change_pct = _compute_taiex_proxy(data_path, date_str)

    # Use INSERT OR REPLACE since regime is a single snapshot that gets updated
    result = conn.execute(
        """INSERT OR REPLACE INTO regime_snapshots
           (date, regime_label, taiex_close, taiex_change_pct)
           VALUES (?, ?, ?, ?)""",
        (date_str, regime_label, taiex_close, taiex_change_pct),
    )
    stats["regime_snapshots_inserted"] = result.rowcount

    if verbose:
        logger.info(
            "regime_snapshots: %s = %s (TAIEX proxy: %.2f, %.2f%%)",
            date_str, regime_label, taiex_close, taiex_change_pct or 0,
        )


def _compute_taiex_proxy(data_path, date_str) -> Tuple[Optional[float], Optional[float]]:
    """Compute a TAIEX proxy (average close of top-volume stocks) from daily data.

    Returns (taiex_close, taiex_change_pct) or (None, None) if data unavailable.
    This is a rough approximation — the actual TAIEX is market-cap weighted.
    """
    daily_file = data_path / f"daily_{date_str}.json"
    if not daily_file.exists():
        return None, None

    try:
        with open(daily_file, "r", encoding="utf-8") as f:
            daily_data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return None, None

    if not isinstance(daily_data, list) or not daily_data:
        return None, None

    # Top 20 stocks by volume → average close as TAIEX proxy
    stock_entries = []
    for item in daily_data:
        if not isinstance(item, dict):
            continue
        close = _safe_float(item.get("ClosingPrice", item.get("收盤價", 0)))
        volume = _safe_float(item.get("TradeVolume", item.get("成交股數", 0)))
        change = _safe_float(item.get("Change", item.get("漲跌價差", 0)))
        if close > 0:
            stock_entries.append({"close": close, "volume": volume, "change": change})

    if not stock_entries:
        return None, None

    # Sort by volume descending, take top 20
    stock_entries.sort(key=lambda x: x["volume"], reverse=True)
    top20 = stock_entries[:20]

    avg_close = sum(s["close"] for s in top20) / len(top20)
    avg_change = sum(s["change"] for s in top20) / len(top20)
    change_pct = (avg_change / avg_close * 100) if avg_close > 0 else 0

    return round(avg_close, 2), round(change_pct, 2)


def _migrate_portfolio_history(conn, data_path, stats, verbose):
    """Migrate paper_trades.json → portfolio_history table.

    paper_trades.json is a list of trade dicts. Each trade has:
    - entry_date, entry_price, code, status
    - exit_date, exit_price, exit_reason, pnl_pct (if closed)

    We create two rows per closed trade (entry + exit),
    and one row per open trade (entry only).
    """
    trades_file = data_path / "paper_trades.json"
    if not trades_file.exists():
        if verbose:
            logger.info("paper_trades.json not found — skipping portfolio_history migration")
        return

    try:
        with open(trades_file, "r", encoding="utf-8") as f:
            trades = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to read paper_trades.json: %s", e)
        return

    if not isinstance(trades, list):
        return

    rows = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue

        stock_id = trade.get("code", "")
        entry_date = trade.get("entry_date", "")
        entry_price = _safe_float(trade.get("entry_price", 0))
        status = trade.get("status", "open")

        if not stock_id or not entry_date:
            continue

        # Entry row
        rows.append((
            entry_date,
            "entry",
            stock_id,
            1.0,  # quantity (paper trades are per-share basis)
            entry_price,
            0.0,  # no P&L at entry
        ))

        # Exit row (only for closed trades)
        if status == "closed":
            exit_date = trade.get("exit_date", "")
            exit_price = _safe_float(trade.get("exit_price", 0))
            pnl_pct = _safe_float(trade.get("pnl_pct", 0))

            # Convert pnl_pct to absolute P&L per share
            pnl_abs = round(entry_price * pnl_pct / 100, 4) if entry_price > 0 else 0

            if exit_date:
                rows.append((
                    exit_date,
                    "exit",
                    stock_id,
                    1.0,
                    exit_price,
                    pnl_abs,
                ))

    if not rows:
        return

    # portfolio_history uses AUTOINCREMENT id, so we can't use INSERT OR IGNORE
    # on the data columns directly. Instead, check for existing entries.
    # For idempotency, we use a simple approach: delete+reinsert would be
    # wasteful. Instead, we check if rows with same (date, action, stock_id, price)
    # already exist before inserting.

    # More efficient approach: use a composite check
    existing = set()
    for row in conn.execute(
        "SELECT date, action, stock_id, price FROM portfolio_history"
    ):
        existing.add((row[0], row[1], row[2], row[3]))

    new_rows = []
    for row in rows:
        # row = (date, action, stock_id, quantity, price, pnl)
        # Dedup key matches DB query: (date, action, stock_id, price)
        key = (row[0], row[1], row[2], row[4])
        if key not in existing:
            new_rows.append(row)
            existing.add(key)

    if new_rows:
        conn.executemany(
            """INSERT INTO portfolio_history
               (date, action, stock_id, quantity, price, pnl)
               VALUES (?, ?, ?, ?, ?, ?)""",
            new_rows,
        )
        stats["portfolio_history_inserted"] = len(new_rows)

    stats["portfolio_history_skipped"] = len(rows) - len(new_rows)

    if verbose:
        logger.info(
            "portfolio_history: %d inserted, %d skipped",
            stats["portfolio_history_inserted"],
            stats["portfolio_history_skipped"],
        )


# ---------------------------------------------------------------------------
#  Query helpers
# ---------------------------------------------------------------------------

def get_daily_history(
    stock_id: str,
    since_date: Optional[str] = None,
    limit: int = 365,
    data_dir: Optional[str] = None,
) -> List[dict]:
    """Get daily OHLCV history for a single stock.

    Args:
        stock_id:   Stock code (e.g. "2330").
        since_date: Optional start date (YYYY-MM-DD). If None, returns
                    the most recent `limit` rows.
        limit:      Maximum number of rows to return (default 365).
        data_dir:   Data directory override.

    Returns:
        List of dicts with keys: stock_id, date, open, high, low, close,
        volume, adj_close, adj_volume. Sorted by date ASC.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        if since_date:
            cursor = conn.execute(
                """SELECT stock_id, date, open, high, low, close,
                          volume, adj_close, adj_volume
                   FROM stocks_daily
                   WHERE stock_id = ? AND date >= ?
                   ORDER BY date ASC
                   LIMIT ?""",
                (stock_id, since_date, limit),
            )
        else:
            # Most recent `limit` rows, then reverse to ASC
            cursor = conn.execute(
                """SELECT stock_id, date, open, high, low, close,
                          volume, adj_close, adj_volume
                   FROM stocks_daily
                   WHERE stock_id = ?
                   ORDER BY date DESC
                   LIMIT ?""",
                (stock_id, limit),
            )
            rows = [dict(r) for r in cursor]
            rows.reverse()
            return rows

        return [dict(r) for r in cursor]
    finally:
        conn.close()


def get_regime_history(
    since_date: Optional[str] = None,
    data_dir: Optional[str] = None,
) -> List[dict]:
    """Get historical regime snapshots.

    Args:
        since_date: Optional start date (YYYY-MM-DD).
        data_dir:   Data directory override.

    Returns:
        List of dicts with keys: date, regime_label, taiex_close,
        taiex_change_pct. Sorted by date ASC.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        if since_date:
            cursor = conn.execute(
                """SELECT date, regime_label, taiex_close, taiex_change_pct
                   FROM regime_snapshots
                   WHERE date >= ?
                   ORDER BY date ASC""",
                (since_date,),
            )
        else:
            cursor = conn.execute(
                """SELECT date, regime_label, taiex_close, taiex_change_pct
                   FROM regime_snapshots
                   ORDER BY date ASC"""
            )
        return [dict(r) for r in cursor]
    finally:
        conn.close()


def get_portfolio_pnl(
    date_range: Optional[Tuple[str, str]] = None,
    data_dir: Optional[str] = None,
) -> List[dict]:
    """Get portfolio P&L entries within a date range.

    Args:
        date_range: Optional (start_date, end_date) tuple (YYYY-MM-DD).
        data_dir:   Data directory override.

    Returns:
        List of dicts with keys: date, action, stock_id, quantity, price, pnl.
        Sorted by date ASC.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        if date_range:
            start, end = date_range
            cursor = conn.execute(
                """SELECT date, action, stock_id, quantity, price, pnl
                   FROM portfolio_history
                   WHERE date >= ? AND date <= ?
                   ORDER BY date ASC""",
                (start, end),
            )
        else:
            cursor = conn.execute(
                """SELECT date, action, stock_id, quantity, price, pnl
                   FROM portfolio_history
                   ORDER BY date ASC"""
            )
        return [dict(r) for r in cursor]
    finally:
        conn.close()


def get_corporate_actions(
    stock_id: Optional[str] = None,
    since_date: Optional[str] = None,
    data_dir: Optional[str] = None,
) -> List[dict]:
    """Get corporate action records.

    Args:
        stock_id:   Optional stock code filter.
        since_date: Optional start date filter (YYYY-MM-DD).
        data_dir:   Data directory override.

    Returns:
        List of dicts with keys: stock_id, ex_date, cash_div, stock_div, source.
        Sorted by ex_date ASC.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        conditions = []
        params = []

        if stock_id:
            conditions.append("stock_id = ?")
            params.append(stock_id)
        if since_date:
            conditions.append("ex_date >= ?")
            params.append(since_date)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        sql = f"SELECT stock_id, ex_date, cash_div, stock_div, source FROM corporate_actions{where} ORDER BY ex_date ASC"

        cursor = conn.execute(sql, params)
        return [dict(r) for r in cursor]
    finally:
        conn.close()


def get_stocks_on_date(
    date: str,
    data_dir: Optional[str] = None,
) -> List[dict]:
    """Get all stocks' daily data for a specific date.

    Useful for cross-sectional queries (e.g., market breadth).

    Args:
        date:     Trading date (YYYY-MM-DD).
        data_dir: Data directory override.

    Returns:
        List of dicts with stocks_daily columns.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        cursor = conn.execute(
            """SELECT stock_id, date, open, high, low, close,
                      volume, adj_close, adj_volume
               FROM stocks_daily
               WHERE date = ?
               ORDER BY volume DESC""",
            (date,),
        )
        return [dict(r) for r in cursor]
    finally:
        conn.close()


def get_available_dates(
    data_dir: Optional[str] = None,
    limit: int = 100,
) -> List[str]:
    """Get the most recent trading dates available in stocks_daily.

    Args:
        data_dir: Data directory override.
        limit:    Maximum dates to return.

    Returns:
        List of date strings (YYYY-MM-DD), most recent first.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        cursor = conn.execute(
            "SELECT DISTINCT date FROM stocks_daily ORDER BY date DESC LIMIT ?",
            (limit,),
        )
        return [r[0] for r in cursor]
    finally:
        conn.close()


def get_stock_date_range(
    stock_id: str,
    data_dir: Optional[str] = None,
) -> Optional[Tuple[str, str]]:
    """Get the date range for a stock in stocks_daily.

    Args:
        stock_id: Stock code.
        data_dir: Data directory override.

    Returns:
        (earliest_date, latest_date) tuple, or None if stock not found.
    """
    conn = get_connection(data_dir, readonly=True)
    try:
        cursor = conn.execute(
            """SELECT MIN(date), MAX(date) FROM stocks_daily WHERE stock_id = ?""",
            (stock_id,),
        )
        row = cursor.fetchone()
        if row and row[0]:
            return (row[0], row[1])
        return None
    finally:
        conn.close()


def get_daily_history_batch(
    stock_ids: List[str],
    since_date: Optional[str] = None,
    limit: int = 365,
    data_dir: Optional[str] = None,
) -> Dict[str, List[dict]]:
    """Get daily history for multiple stocks at once.

    Args:
        stock_ids:  List of stock codes.
        since_date: Optional start date (YYYY-MM-DD).
        limit:      Max rows per stock (default 365).
        data_dir:   Data directory override.

    Returns:
        Dict {stock_id: [row_dicts]}, each sorted by date ASC.
    """
    if not stock_ids:
        return {}

    conn = get_connection(data_dir, readonly=True)
    try:
        # Use temp table for efficient multi-stock lookup
        conn2 = get_connection(data_dir, readonly=False)
        try:
            conn2.execute("CREATE TEMP TABLE IF NOT EXISTS _query_stocks (stock_id TEXT)")
            conn2.execute("DELETE FROM _query_stocks")
            conn2.executemany(
                "INSERT INTO _query_stocks (stock_id) VALUES (?)",
                [(s,) for s in stock_ids],
            )

            if since_date:
                cursor = conn2.execute(
                    """SELECT stock_id, date, open, high, low, close,
                              volume, adj_close, adj_volume
                       FROM stocks_daily
                       WHERE stock_id IN (SELECT stock_id FROM _query_stocks)
                         AND date >= ?
                       ORDER BY stock_id, date ASC""",
                    (since_date,),
                )
            else:
                cursor = conn2.execute(
                    """SELECT stock_id, date, open, high, low, close,
                              volume, adj_close, adj_volume
                       FROM stocks_daily
                       WHERE stock_id IN (SELECT stock_id FROM _query_stocks)
                       ORDER BY stock_id, date ASC""",
                )

            result: Dict[str, List[dict]] = {}
            for row in cursor:
                d = dict(row)
                sid = d.pop("stock_id")
                result.setdefault(sid, []).append(d)

            # Trim to limit per stock
            if limit:
                for sid in result:
                    if len(result[sid]) > limit:
                        result[sid] = result[sid][-limit:]

            return result
        finally:
            conn2.execute("DELETE FROM _query_stocks")
            conn2.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
#  Stage 1 Results — Phase 25: SQL-backed stage1↔stage2 data passing
# ---------------------------------------------------------------------------

def save_stage1_to_sqlite(stage1_output: dict, data_dir: Optional[str] = None) -> int:
    """Save Stage 1 screening results into the stage1_results table.

    Called by the pipeline after stage1_screen completes. Replaces any
    existing rows for the same run_date (idempotent per run).

    Args:
        stage1_output:  The dict returned by run_stage1().
        data_dir:       Data directory override.

    Returns:
        Number of candidate rows inserted.
    """
    run_date = stage1_output.get("date", "")
    if not run_date:
        logger.warning("save_stage1_to_sqlite: no date in stage1_output — skipping")
        return 0

    candidates = stage1_output.get("candidates", [])
    if not candidates:
        return 0

    conn = get_connection(data_dir, readonly=False)
    try:
        # Delete previous rows for this run_date (idempotent replace)
        conn.execute("DELETE FROM stage1_results WHERE run_date = ?", (run_date,))

        rows = []
        for c in candidates:
            sb = c.get("score_breakdown", {})
            rows.append((
                run_date,
                str(c.get("code", "")),
                str(c.get("name", "")),
                _safe_float(c.get("close"), 0),
                _safe_float(c.get("composite_score"), 0),
                _safe_float(sb.get("revenue", 25), 25),
                _safe_float(sb.get("profitability", 25), 25),
                _safe_float(sb.get("valuation", 25), 25),
                _safe_float(sb.get("flow", 25), 25),
                _safe_float(sb.get("momentum", 25), 25),
                1 if c.get("pass", c.get("composite_score", 0) >= 65) else 0,
            ))

        conn.executemany(
            """INSERT INTO stage1_results
               (run_date, stock_id, stock_name, close, composite_score,
                revenue_score, profitability_score, valuation_score,
                flow_score, momentum_score, passed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        logger.info("stage1_results: saved %d candidates for %s", len(rows), run_date)
        return len(rows)
    except Exception as e:
        logger.warning("save_stage1_to_sqlite failed: %s", e)
        return 0
    finally:
        conn.close()


def load_stage1_from_sqlite(
    date_str: Optional[str] = None,
    data_dir: Optional[str] = None,
) -> Optional[dict]:
    """Load Stage 1 results from SQLite instead of JSON file.

    Returns the same dict shape as load_stage1_results() in stage2_deep.py
    so stage2 can use it as a drop-in replacement. Returns None if no data.

    Args:
        date_str:   Run date (YYYY-MM-DD). If None, uses the most recent.
        data_dir:   Data directory override.

    Returns:
        Dict with keys: stage, date, timestamp, candidates, watchlist,
        rejected_count, summary — matching the JSON file format.
    """
    db_path = get_db_path(data_dir)
    if not db_path.exists():
        return None

    conn = get_connection(data_dir, readonly=True)
    try:
        if date_str is None:
            # Find most recent run_date
            row = conn.execute(
                "SELECT MAX(run_date) FROM stage1_results"
            ).fetchone()
            if not row or not row[0]:
                return None
            date_str = row[0]

        cursor = conn.execute(
            """SELECT stock_id, stock_name, close, composite_score,
                      revenue_score, profitability_score, valuation_score,
                      flow_score, momentum_score, passed
               FROM stage1_results
               WHERE run_date = ?
               ORDER BY composite_score DESC""",
            (date_str,),
        )

        candidates = []
        for r in cursor:
            candidates.append({
                "code": r[0],
                "name": r[1],
                "close": r[2],
                "composite_score": r[3],
                "score_breakdown": {
                    "revenue": r[4],
                    "profitability": r[5],
                    "valuation": r[6],
                    "flow": r[7],
                    "momentum": r[8],
                },
                "pass": bool(r[9]),
            })

        if not candidates:
            return None

        return {
            "stage": 1,
            "date": date_str,
            "timestamp": datetime.now().isoformat(),
            "candidates": candidates,
            "watchlist": [],
            "rejected_count": 0,
            "summary": {
                "total_screened": len(candidates),
                "passed": len([c for c in candidates if c.get("pass")]),
                "watchlist": 0,
                "rejected": 0,
            },
        }
    except Exception as e:
        logger.debug("load_stage1_from_sqlite failed: %s", e)
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
#  Database info / diagnostics
# ---------------------------------------------------------------------------

def db_info(data_dir: Optional[str] = None) -> dict:
    """Return summary statistics about the SQLite database.

    Useful for debugging and verifying migration results.
    """
    db_path = get_db_path(data_dir)
    if not db_path.exists():
        return {"exists": False, "path": str(db_path)}

    conn = get_connection(data_dir, readonly=True)
    try:
        info = {
            "exists": True,
            "path": str(db_path),
            "size_bytes": db_path.stat().st_size,
        }

        # Schema version
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'schema_version'"
        ).fetchone()
        info["schema_version"] = int(row[0]) if row else 0

        # Last migration
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'last_migration'"
        ).fetchone()
        info["last_migration"] = row[0] if row else None

        # Row counts
        for table in ["stocks_daily", "corporate_actions", "regime_snapshots", "portfolio_history", "stage1_results"]:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            info[f"{table}_rows"] = row[0]

        # Date range in stocks_daily
        row = conn.execute(
            "SELECT MIN(date), MAX(date) FROM stocks_daily"
        ).fetchone()
        info["stocks_daily_date_range"] = (row[0], row[1]) if row and row[0] else None

        # Distinct stock count
        row = conn.execute(
            "SELECT COUNT(DISTINCT stock_id) FROM stocks_daily"
        ).fetchone()
        info["distinct_stocks"] = row[0]

        return info
    finally:
        conn.close()


# ---------------------------------------------------------------------------
#  CLI entry point
# ---------------------------------------------------------------------------

def main():
    """Run migration and print summary."""
    import argparse

    parser = argparse.ArgumentParser(
        description="SQLite data layer — migrate JSON to SQLite and query"
    )
    parser.add_argument(
        "--migrate", action="store_true",
        help="Run JSON → SQLite migration",
    )
    parser.add_argument(
        "--info", action="store_true",
        help="Print database info",
    )
    parser.add_argument(
        "--query-daily", type=str, metavar="STOCK_ID",
        help="Query daily history for a stock",
    )
    parser.add_argument(
        "--since", type=str, metavar="YYYY-MM-DD",
        help="Start date for queries",
    )
    parser.add_argument(
        "--limit", type=int, default=10,
        help="Number of rows to display (default 10)",
    )
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help="Override data directory path",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output",
    )
    args = parser.parse_args()

    if not any([args.migrate, args.info, args.query_daily]):
        parser.print_help()
        return

    # Setup logging for CLI
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.migrate:
        stats = migrate_json_to_sqlite(data_dir=args.data_dir, verbose=True)
        print("Migration results:")
        for key, val in stats.items():
            print(f"  {key}: {val}")

    if args.info:
        info = db_info(data_dir=args.data_dir)
        print("Database info:")
        for key, val in info.items():
            print(f"  {key}: {val}")

    if args.query_daily:
        rows = get_daily_history(
            args.query_daily,
            since_date=args.since,
            limit=args.limit,
            data_dir=args.data_dir,
        )
        print(f"Daily history for {args.query_daily} ({len(rows)} rows):")
        for r in rows[:args.limit]:
            print(
                f"  {r['date']}: O={r['open']:.1f} H={r['high']:.1f} "
                f"L={r['low']:.1f} C={r['close']:.1f} V={r['volume']:.0f} "
                f"adjC={r.get('adj_close') or 'N/A'}"
            )


if __name__ == "__main__":
    main()
