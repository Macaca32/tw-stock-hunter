#!/usr/bin/env python3
"""
Pydantic Schema Enforcement for TW Stock Hunter data pipeline.

Survivorship Bias Tier 2: Validates all incoming data structures to prevent
silent field-name mismatches and type errors that produce wrong scores.

Key issues this prevents:
- TradeValue vs volume_twd confusion (caused 0% volume readings)
- Empty strings where numbers expected (safe_float catches but no warning)
- Missing required fields silently defaulting to zero
- Corporate action dates in wrong format
"""

from typing import Optional, Dict, List, Any
from datetime import datetime

try:
    from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
except ImportError:
    print("⚠ Pydantic not available - install with: pip install pydantic")
    raise


# --------------------------------------------------------------------------- #
#  Daily Stock Data (TWSE STOCK_DAY_ALL)
# --------------------------------------------------------------------------- #

class DailyStockRecord(BaseModel):
    """Schema for a single row from TWSE STOCK_DAY_ALL endpoint.
    
    Uses both Chinese and English key names since TWSE API returns
    different keys depending on request params.
    """
    code: str = Field(description="證券代號 / Code", min_length=1)
    name: str = Field(default="", description="證券名稱 / Name")
    closing_price: float = Field(ge=0, description="收盤價 / ClosingPrice")
    opening_price: Optional[float] = Field(default=None, ge=0, description="開盤價 / OpeningPrice")
    highest_price: Optional[float] = Field(default=None, ge=0, description="最高價 / HighestPrice")
    lowest_price: Optional[float] = Field(default=None, ge=0, description="最低價 / LowestPrice")
    trade_volume: float = Field(ge=0, description="成交股數 / TradeVolume")
    trade_value: float = Field(ge=0, description="成交額 / TradeValue (TWD)")
    transactions: Optional[float] = Field(default=None, ge=0, description="成交筆數 / Transaction")
    change: Optional[float] = Field(default=None, description="漲跌價差 / Change")
    turnover_rate: Optional[float] = Field(default=None, description="轉手率(%) / TurnoverRate")

    @field_validator("closing_price")
    @classmethod
    def closing_price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f"Closing price must be > 0, got {v}")
        return v


# --------------------------------------------------------------------------- #
#  PE/PB Ratio Data (TWSE BWIBBU_ALL)
# --------------------------------------------------------------------------- #

class PERatioRecord(BaseModel):
    """Schema for P/E ratio data from TWSE BWIBBU endpoint.
    
    Uses aliases to match actual TWSE API response keys:
    Code, Name, PEratio, DividendYield, PBratio.
    """
    model_config = ConfigDict(populate_by_name=True)
    
    code: str = Field(alias="Code", description="證券代號 / Code", min_length=1)
    name: Optional[str] = Field(default=None, alias="Name", description="證券名稱")
    pe_ratio: Optional[float] = Field(default=None, alias="PEratio", description="本益比")
    pb_ratio: Optional[float] = Field(default=None, ge=0, alias="PBratio", description="股價淨值比")
    dividend_yield: Optional[float] = Field(default=None, alias="DividendYield", description="殖利率(%)")
    
    @field_validator("pe_ratio", "pb_ratio", "dividend_yield", mode="before")
    @classmethod
    def empty_to_none(cls, v):
        """Convert empty strings to None for numeric fields."""
        if isinstance(v, str) and v.strip() == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


# --------------------------------------------------------------------------- #
#  Company Info (TWSE t187ap03_L)
# --------------------------------------------------------------------------- #

class CompanyInfo(BaseModel):
    """Schema for company information from TWSE t187ap03_L endpoint.
    
    Uses aliases to match actual Chinese keys from TWSE API.
    """
    model_config = ConfigDict(populate_by_name=True, extra="ignore")
    
    code: str = Field(alias="公司代號", description="公司代號", min_length=1)
    name: Optional[str] = Field(default=None, alias="公司名稱", description="公司名稱")
    paid_in_capital: Optional[float] = Field(
        default=None, ge=0, alias="實收資本額", description="實收資本額 (TWD)"
    )
    list_date: Optional[str] = Field(default=None, alias="上市日期", description="上市日期 YYYYMMDD")
    
    @field_validator("paid_in_capital", mode="before")
    @classmethod
    def parse_paid_in_capital(cls, v):
        """Parse paid-in capital from string to float."""
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return None
        return v


# --------------------------------------------------------------------------- #
#  Revenue Data (TWSE t187ap05_L)
# --------------------------------------------------------------------------- #

class RevenueRecord(BaseModel):
    """Schema for monthly revenue data from TWSE t187ap05_L endpoint.
    
    Actual TWSE keys use prefix format: 營業收入-當月營收, etc.
    Uses aliases to match both Chinese and English key names.
    """
    model_config = ConfigDict(populate_by_name=True, extra="ignore")
    
    code: str = Field(alias="公司代號", description="公司代號", min_length=1)
    name: Optional[str] = Field(default=None, alias="公司名稱", description="公司名稱")
    current_month_revenue: Optional[float] = Field(
        default=None, ge=0,
        alias="營業收入-當月營收",
        description="當月營收 (TWD)"
    )
    last_month_revenue: Optional[float] = Field(
        default=None, ge=0,
        alias="營業收入-上月營收",
        description="上月營收 (TWD)"
    )
    yoy_change_pct: Optional[float] = Field(
        default=None,
        alias="營業收入-去年同月增減(%)",
        description="去年同月增減(%)"
    )
    mom_change_pct: Optional[float] = Field(
        default=None,
        alias="營業收入-上月比較增減(%)",
        description="上月比較增減(%)"
    )

    @field_validator("current_month_revenue", "last_month_revenue", "yoy_change_pct", "mom_change_pct", mode="before")
    @classmethod
    def parse_revenue_fields(cls, v):
        """Parse revenue fields from string to float."""
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return None
        return v


# --------------------------------------------------------------------------- #
#  Corporate Action (from corporate_actions.py)
# --------------------------------------------------------------------------- #

class CorporateAction(BaseModel):
    """Schema for a single corporate action record."""
    date: str = Field(description="Ex-date in YYYY-MM-DD format")
    cash_div: float = Field(default=0.0, ge=0, description="Cash dividend per share (TWD)")
    stock_div: float = Field(default=0.0, ge=0, description="Stock dividend in 元 (face value units)")
    ref_price: Optional[float] = Field(default=None, ge=0, description="Reference price")
    source: str = Field(description="Data source: 'twt49u' or 'dividend_declaration'")

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Date must be YYYY-MM-DD format, got '{v}'")
        return v


# --------------------------------------------------------------------------- #
#  Price History Point (from fetch_history.py)
# --------------------------------------------------------------------------- #

class PricePoint(BaseModel):
    """Schema for a single price history entry."""
    date: str = Field(description="Date in YYYY-MM-DD format")
    close: float = Field(ge=0, description="Raw close price")
    adj_close: Optional[float] = Field(default=None, ge=0, description="Backward-adjusted close")
    volume: Optional[float] = Field(default=None, ge=0, description="Trading volume (shares)")
    open: Optional[float] = Field(default=None, ge=0)
    high: Optional[float] = Field(default=None, ge=0)
    low: Optional[float] = Field(default=None, ge=0)
    cumulative_factor: float = Field(default=1.0, description="Cumulative adjustment factor")

    @field_validator("date")
    @classmethod
    def validate_date(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Date must be YYYY-MM-DD format, got '{v}'")
        return v

    @model_validator(mode="after")
    def adj_close_defaults_to_close(self):
        if self.adj_close is None and self.close > 0:
            object.__setattr__(self, "adj_close", self.close)
        return self


# --------------------------------------------------------------------------- #
#  Stage 1 Candidate (screening output)
# --------------------------------------------------------------------------- #

class ScoreBreakdown(BaseModel):
    """Per-dimension score breakdown."""
    revenue: float = Field(ge=0, le=100)
    profitability: float = Field(ge=0, le=100)
    valuation: float = Field(ge=0, le=100)
    flow: float = Field(ge=0, le=100)
    momentum: float = Field(ge=0, le=100)


class Stage1Candidate(BaseModel):
    """Schema for a Stage 1 screening candidate.
    
    Phase 9 R7: extra='forbid' to catch bugs in our own code (Z.ai recommendation).
    Unknown fields indicate problems upstream, not flexible data sources.
    """
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    
    code: str = Field(min_length=1)
    name: str = Field(default="")
    close: float = Field(ge=0)
    composite_score: float = Field(ge=0, le=100)
    score_breakdown: ScoreBreakdown
    passed: bool = Field(alias="pass", description="Whether candidate passed threshold")


# --------------------------------------------------------------------------- #
#  Stage 2 Check Result
# --------------------------------------------------------------------------- #

class DeepCheckResult(BaseModel):
    """Schema for a single Stage 2 deep check."""
    score: float = Field(ge=0, le=100)
    status: str = Field(min_length=1)


class Stage2Candidate(BaseModel):
    """Schema for a Stage 2 candidate output.
    
    Phase 9 R7: extra='forbid' to catch bugs in our own code (Z.ai recommendation).
    Unknown fields indicate problems upstream, not flexible data sources.
    """
    model_config = ConfigDict(extra="forbid")
    
    code: str = Field(min_length=1)
    name: str = Field(default="")
    stage1_score: float = Field(ge=0, le=100)
    stage2_score: float = Field(ge=0, le=100)
    checks: Dict[str, DeepCheckResult]
    combined_score: float = Field(ge=0, le=100)


# --------------------------------------------------------------------------- #
#  Regime Output
# --------------------------------------------------------------------------- #

class RegimeOutput(BaseModel):
    """Schema for regime detector output."""
    regime: str = Field(pattern="^(normal|caution|stress|crisis|black_swan|unknown)$")
    raw_regime: Optional[str] = None
    days_in_regime: int = Field(ge=1)
    volatility: float = Field(ge=0)
    ex_dividend_season: bool = False
    global_risk: str = Field(pattern="^(low|moderate|high|extreme|neutral)$")
    data_quality: str = Field(default="OK")
    confidence: str = Field(pattern="^(high|medium|low)$")


# --------------------------------------------------------------------------- #
#  Holiday Entry (from TWSE holidaySchedule)
# --------------------------------------------------------------------------- #

class HolidayEntry(BaseModel):
    """Schema for a TWSE holiday schedule entry.
    
    Matches the actual TWSE /holidaySchedule/holidaySchedule response format:
    Name (Chinese), Date (ROC YYYYMMDD), Weekday, Description.
    """
    model_config = ConfigDict(populate_by_name=True)
    
    name: str = Field(alias="Name", description="Holiday name in Traditional Chinese")
    date_roc: str = Field(alias="Date", description="ROC date format YYYYMMDD as string (e.g., '1150522')")
    weekday: Optional[str] = Field(default=None, alias="Weekday")
    description: Optional[str] = Field(default=None, alias="Description")


# --------------------------------------------------------------------------- #
#  Validation Helpers
# --------------------------------------------------------------------------- #

class DataValidationError(Exception):
    """Raised when data validation fails."""
    def __init__(self, field: str, value, message: str):
        self.field = field
        self.value = value
        self.message = message
        super().__init__(f"Validation error in '{field}': {message} (got {value!r})")


def validate_daily_stock(raw: dict) -> Optional[DailyStockRecord]:
    """Validate a raw TWSE daily stock record. Returns None on failure."""
    try:
        # Map Chinese/English keys to our schema
        code = raw.get("證券代號", raw.get("Code", ""))
        closing_price_raw = raw.get("收盤價", raw.get("ClosingPrice", 0))

        return DailyStockRecord(
            code=str(code),
            name=str(raw.get("證券名稱", raw.get("Name", ""))),
            closing_price=float(closing_price_raw) if closing_price_raw else 0.0,
            opening_price=_safe_parse_float(raw.get("開盤價", raw.get("OpeningPrice"))),
            highest_price=_safe_parse_float(raw.get("最高價", raw.get("HighestPrice"))),
            lowest_price=_safe_parse_float(raw.get("最低價", raw.get("LowestPrice"))),
            trade_volume=_safe_parse_float(raw.get("成交股數", raw.get("TradeVolume"))) or 0.0,
            trade_value=_safe_parse_float(raw.get("成交額", raw.get("TradeValue"))) or 0.0,
            transactions=_safe_parse_float(raw.get("成交筆數", raw.get("Transaction"))),
            change=_safe_parse_float(raw.get("Change")),
            turnover_rate=_safe_parse_float(raw.get("轉手率(%)", raw.get("TurnoverRate"))),
        )
    except (ValueError, TypeError) as e:
        return None


def validate_price_point(raw: dict) -> Optional[PricePoint]:
    """Validate a price history point."""
    try:
        return PricePoint(**raw)
    except (ValueError, TypeError):
        return None


def _safe_parse_float(val) -> Optional[float]:
    """Parse float from various input types. Returns None on failure."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def batch_validate(records: list, schema_cls, name: str) -> tuple:
    """Validate a batch of records against a Pydantic model.

    Args:
        records: List of raw dicts to validate.
        schema_cls: Pydantic BaseModel subclass.
        name: Human-readable dataset name for error messages.

    Returns:
        (valid_records, validation_errors_count) tuple.
        valid_records is a list of validated model instances (as dicts).
    """
    if not records:
        return [], 0

    valid = []
    errors = 0

    for i, raw in enumerate(records):
        try:
            # Don't normalize keys — let Pydantic handle Chinese↔English via aliases.
            # normalize_keys() converts "公司代號"→"company_code" but models expect
            # either the alias ("公司代號") or field name ("code"). Pre-normalizing
            # breaks this contract.
            instance = schema_cls(**raw)
            valid.append(instance.model_dump())
        except Exception as e:
            errors += 1
            # Log first few errors for debugging
            if errors <= 3 and i < 50:
                code_val = raw.get("公司代號", raw.get("Code", raw.get("code", f"#{i}")))
                print(f"   ⚠ {name} record {code_val}: {type(e).__name__}")

    if errors > 0:
        pct_err = (errors / len(records) * 100)
        if pct_err > 5:
            print(f"❌ {name}: {errors}/{len(records)} records failed validation ({pct_err:.1f}% — HIGH)")
        else:
            print(f"⚠ {name}: {errors}/{len(records)} records failed validation")

    return valid, errors


# --------------------------------------------------------------------------- #
#  Field Name Alias Map (catches common TWSE key mismatches)
# --------------------------------------------------------------------------- #

FIELD_ALIASES = {
    # Chinese → English canonical names
    "證券代號": "code",
    "證券名稱": "name",
    "收盤價": "closing_price",
    "開盤價": "opening_price",
    "最高價": "highest_price",
    "最低價": "lowest_price",
    "成交股數": "trade_volume",
    "成交額": "trade_value",
    "成交筆數": "transactions",
    "本益比": "pe_ratio",
    "股價淨值比": "pb_ratio",
    "殖利率(%)": "dividend_yield",
    "公司代號": "company_code",
    "實收資本額": "paid_in_capital",
    # Additional PE endpoint keys
    "每股盈餘": "eps",
    "淨值": "book_value",
    # Revenue endpoint keys
    "當月營收": "current_month_revenue",
    "去年同月增減(%)": "yoy_change_pct",
    "上月比較增減(%)": "mom_change_pct",
}


def normalize_keys(raw_dict: dict) -> dict:
    """Normalize Chinese keys to English canonical names.

    Useful for catching field mismatches before Pydantic validation.
    """
    normalized = {}
    for key, value in raw_dict.items():
        canonical = FIELD_ALIASES.get(key, key)
        if canonical not in normalized:  # Don't overwrite existing English keys
            normalized[canonical] = value
    return normalized


if __name__ == "__main__":
    # Quick test
    sample_daily = {
        "證券代號": "2330",
        "證券名稱": "台積電",
        "收盤價": 980.5,
        "開盤價": 975.0,
        "最高價": 985.0,
        "最低價": 972.0,
        "成交股數": 15000000,
        "成交額": 14700000000,
        "成交筆數": 45000,
    }

    record = validate_daily_stock(sample_daily)
    if record:
        print(f"✓ Validated daily stock: {record.code} {record.name} @ {record.closing_price}")
    else:
        print("✗ Validation failed")

    # Test price point
    sample_price = {
        "date": "2026-05-11",
        "close": 980.5,
        "adj_close": 975.2,
        "volume": 15000000,
        "open": 975.0,
        "high": 985.0,
        "low": 972.0,
    }

    pp = validate_price_point(sample_price)
    if pp:
        print(f"✓ Validated price point: {pp.date} close={pp.close} adj_close={pp.adj_close}")
