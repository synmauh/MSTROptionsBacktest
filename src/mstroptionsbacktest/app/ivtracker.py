"""IV-Tracker.

COPYRIGHT BY SYNERGETIK GMBH
The copyright of this source code(s) herein is the property of
Synergetik GmbH, Schiffweiler, Germany. (www.synergetik.de)
The program(s) may be used only with the written permission of
Synergetik GmbH or in accordance with the terms and conditions stipulated
in an agreement/contract under which the program(s) have been supplied.
Examples (not exclusive) of restrictions:
    - all sources are confidential and under NDA
    - giving these sources to other people/companies is not allowed
    - Using these sources in other projects is not allowed
    - copying parts of these sources is not allowed
    - changing these sources is not allowed Markus
"""

__author__    = "Markus Uhle"
__copyright__ = "Synergetik GmbH"


# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------


import logging
import math
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from ib_insync import IB, ContractDetails, Option, Stock, Ticker

# -----------------------------------------------------------------------------
# -- custom module import
# -----------------------------------------------------------------------------
from src.logger import configure_logger

# -----------------------------------------------------------------------------
# -- logging
# -----------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# -- Constants
# -----------------------------------------------------------------------------
# === CONFIGURATION ===
IB_HOST = "127.0.0.1"
# IB_PORT   = 4002  # paper TWS/Gateway default
IB_PORT   = 4001  # live TWS/Gateway
CLIENT_ID = 123  # unique integer per session
SYMBOL    = "MSTR"

DATA_MODE = 1  # 1:live, 2:frozen, 3:delayed, 4:delayed frozen

DB_PATH = "iv_tracker.db"

DEBUG_ALWAYS_GET_DATA = True


# -----------------------------------------------------------------------------
# -- Classes
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class MarketSchedule:
    time_zone_id  : str
    liquid_hours  : str
    trading_hours : str


@dataclass(slots=True)
class MarketState:
    is_open               : bool
    now_local             : datetime
    current_session_start : datetime | None
    current_session_end   : datetime | None
    next_session_start    : datetime | None


@dataclass(slots=True)
class TrackedOptionPosition:
    account       : str
    con_id        : int
    symbol        : str
    exchange      : str
    currency      : str
    local_symbol  : str
    trading_class : str
    expiry        : str
    strike        : float
    right         : str
    multiplier    : str
    position      : float
    avg_cost      : float


@dataclass(slots=True)
class OptionSnapshot:
    timestamp_utc          : datetime
    symbol                 : str
    con_id                 : int
    local_symbol           : str
    expiry                 : str
    strike                 : float
    right                  : str
    option_price           : float | None
    iv                     : float | None
    delta                  : float | None
    underlying_price       : float | None
    underlying_iv30        : float | None
    underlying_iv_rank_13w : float | None
    underlying_iv_rank_52w : float | None


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def connect_ib() -> IB:
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, readonly=True)

    if not ib.isConnected():
        raise ConnectionError("Could not connect to IBKR")

    market_data_mode = 2 if DEBUG_ALWAYS_GET_DATA else DATA_MODE
    ib.reqMarketDataType(market_data_mode)

    logger.info("Connected to IBKR: %s:%s clientId=%s", IB_HOST, IB_PORT, CLIENT_ID)
    return ib


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def check_market_state(schedule: MarketSchedule) -> MarketState:
    tz        = ZoneInfo(schedule.time_zone_id)
    now_local = datetime.now(tz)

    current_session_start : datetime | None = None
    current_session_end   : datetime | None = None
    next_session_start    : datetime | None = None

    for part in schedule.liquid_hours.split(";"):
        parsed_range = _parse_ib_hours_range(part, schedule.time_zone_id)
        if parsed_range is None:
            continue

        start_dt, end_dt = parsed_range

        if start_dt <= now_local <= end_dt:
            current_session_start = start_dt
            current_session_end   = end_dt
            break

        if now_local < start_dt:
            if next_session_start is None or start_dt < next_session_start:
                next_session_start = start_dt

    is_open = current_session_start is not None

    return MarketState(
        is_open               = is_open,
        now_local             = now_local,
        current_session_start = current_session_start,
        current_session_end   = current_session_end,
        next_session_start    = next_session_start,
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def log_market_state(state: MarketState, schedule: MarketSchedule) -> None:
    logger.info("Market timezone       : %s", schedule.time_zone_id)
    logger.info("Now local             : %s", state.now_local.isoformat())
    logger.info("Market open           : %s", state.is_open)
    logger.info(
        "Current session start : %s",
        state.current_session_start.isoformat() if state.current_session_start else None,
    )
    logger.info(
        "Current session end   : %s",
        state.current_session_end.isoformat() if state.current_session_end else None,
    )
    logger.info(
        "Next session start    : %s",
        state.next_session_start.isoformat() if state.next_session_start else None,
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_mstr_market_schedule(ib: IB) -> MarketSchedule:
    contract     = Stock("MSTR", "SMART", "USD")
    details_list = ib.reqContractDetails(contract)

    if not details_list:
        raise RuntimeError("No contract details found for MSTR")

    details: ContractDetails = details_list[0]

    return MarketSchedule(
        time_zone_id  = details.timeZoneId,
        liquid_hours  = details.liquidHours,
        trading_hours = details.tradingHours,
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def _parse_ib_hours_range(
    part    : str,
    tz_name : str,
) -> tuple[datetime, datetime] | None:
    part = part.strip()

    if not part:
        return None

    date_part, hours_part = part.split(":", maxsplit=1)
    hours_part            = hours_part.strip()

    if hours_part == "CLOSED":
        return None

    start_str, end_str = hours_part.split("-", maxsplit=1)

    start_raw = f"{date_part}{start_str}" if len(start_str) == 4 else start_str
    end_raw = f"{date_part}{end_str}" if len(end_str) == 4 else end_str

    return (
        _parse_ib_local_datetime(start_raw, tz_name),
        _parse_ib_local_datetime(end_raw, tz_name),
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def _parse_ib_local_datetime(value: str, tz_name: str) -> datetime:
    normalized = value.replace(":", "")
    naive_dt   = datetime.strptime(normalized, "%Y%m%d%H%M")  # noqa: DTZ007 - Naive datetime constructed using `datetime.datetime.strptime()` without %z
    return naive_dt.replace(tzinfo=ZoneInfo(tz_name))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def _is_valid_number(value: float | None) -> bool:
    return value is not None and not math.isnan(value)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def _has_useful_underlying_data(ticker: Ticker) -> bool:
    return any(
        _is_valid_number(value)
        for value in (
            ticker.bid,
            ticker.ask,
            ticker.last,
            ticker.close,
        )
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_open_mstr_call_positions(ib: IB) -> list[TrackedOptionPosition]:
    positions = ib.positions()
    result: list[TrackedOptionPosition] = []

    for pos in positions:
        contract = pos.contract

        if contract.secType != "OPT":
            continue

        if contract.symbol != SYMBOL:
            continue

        if contract.right != "C":
            continue

        if pos.position == 0:
            continue

        result.append(
            TrackedOptionPosition(
                account       = pos.account,
                con_id        = contract.conId,
                symbol        = contract.symbol,
                exchange      = contract.exchange,
                currency      = contract.currency,
                local_symbol  = contract.localSymbol,
                trading_class = contract.tradingClass,
                expiry        = contract.lastTradeDateOrContractMonth,
                strike        = contract.strike,
                right         = contract.right,
                multiplier    = contract.multiplier,
                position      = pos.position,
                avg_cost      = pos.avgCost,
            ),
        )

    logger.info("Found %d open MSTR call positions", len(result))
    return result


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def test_underlying_data(ib: IB) -> None:
    contract = Stock("MSTR", "NASDAQ", "USD")
    ib.qualifyContracts(contract)

    ticker = ib.reqMktData(contract, "", snapshot=False, regulatorySnapshot=False)
    try:
        deadline = time.monotonic() + 10.0

        while time.monotonic() < deadline:
            if _has_useful_underlying_data(ticker):
                break
            ib.sleep(0.5)

        if not _has_useful_underlying_data(ticker):
            logger.warning("No useful market data received for MSTR")
            return

        logger.info(
            "MSTR stock data: bid=%s ask=%s last=%s close=%s",
            ticker.bid,
            ticker.ask,
            ticker.last,
            ticker.close,
        )
    finally:
        ib.cancelMktData(contract)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_mid_price(ticker: Ticker) -> float | None:
    if _is_valid_number(ticker.bid) and _is_valid_number(ticker.ask):
        return (ticker.bid + ticker.ask) / 2.0

    if _is_valid_number(ticker.last):
        return ticker.last

    return None


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_option_snapshot(
    ib               : IB,
    position         : TrackedOptionPosition,
    timestamp_utc    : datetime,
    underlying_price : float | None,
    underlying_iv30  : float | None,
) -> OptionSnapshot | None:
    #
    # Search correct contract
    contract                         = Option(
        symbol                       = position.symbol,
        lastTradeDateOrContractMonth = position.expiry,
        strike                       = position.strike,
        right                        = position.right,
        exchange                     = position.exchange,
        multiplier                   = position.multiplier,
        currency                     = position.currency,
    )
    contract.conId = position.con_id

    qualified_contracts = ib.qualifyContracts(contract)

    if not qualified_contracts:
        logger.warning(
            "Could not qualify contract for %s (conId=%s)",
            position.local_symbol,
            position.con_id,
        )
        return None

    contract = qualified_contracts[0]

    # Request market data (async)
    ticker = ib.reqMktData(contract, "", snapshot=False, regulatorySnapshot=False)

    # Wait until all fields are filled or timeout
    try:
        # Maximum wait time
        deadline = time.monotonic() + 10.0

        iv           : float | None = None
        delta        : float | None = None
        option_price : float | None = None

        has_valid_iv    = False
        has_valid_price = False
        has_valid_delta = False

        # Wait for data
        while time.monotonic() < deadline:
            # Get IV
            if ticker.modelGreeks is not None:
                iv    = ticker.modelGreeks.impliedVol
                delta = ticker.modelGreeks.delta

            # Get price
            option_price = get_mid_price(ticker)

            has_valid_iv    = _is_valid_number(iv)
            has_valid_delta = _is_valid_number(delta)
            has_valid_price = option_price is not None

            if has_valid_iv and has_valid_price and has_valid_delta:
                break

            ib.sleep(0.5)

        logger.info(
            "Option snapshot %s price=%s delta=%s iv=%s",
            position.local_symbol,
            option_price,
            delta,
            iv,
        )

        return OptionSnapshot(
            timestamp_utc          = timestamp_utc,
            symbol                 = position.symbol,
            con_id                 = position.con_id,
            local_symbol           = position.local_symbol,
            expiry                 = position.expiry,
            strike                 = position.strike,
            right                  = position.right,
            option_price           = option_price if has_valid_price else None,
            iv                     = iv if has_valid_iv else None,
            delta                  = delta if has_valid_delta else None,
            underlying_price       = underlying_price,
            underlying_iv30        = underlying_iv30,
            underlying_iv_rank_13w = None,
            underlying_iv_rank_52w = None,
        )

    finally:
        # Always cancel subscription
        ib.cancelMktData(contract)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS option_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                con_id INTEGER NOT NULL,
                local_symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                right TEXT NOT NULL,
                option_price REAL,
                iv REAL,
                delta REAL,
                underlying_price REAL,
                underlying_iv30 REAL,
                underlying_iv_rank_13w REAL,
                underlying_iv_rank_52w REAL
            )
            """,
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_option_snapshots_conid_ts
            ON option_snapshots (con_id, timestamp_utc)
            """,
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                con_id INTEGER NOT NULL,
                signal_type TEXT NOT NULL,
                turning_point_time_utc TEXT,
                sent_at_utc TEXT NOT NULL
            )
            """,
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sent_alerts_lookup
            ON sent_alerts (con_id, signal_type, turning_point_time_utc)
            """,
        )

    logger.info("Database initialized: %s", db_path)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_underlying_market_data(
    ib: IB,
) -> tuple[float | None, float | None]:
    contract = Stock(SYMBOL, "SMART", "USD")
    #
    # Get contract
    qualified_contracts = ib.qualifyContracts(contract)

    if not qualified_contracts:
        logger.warning("Could not qualify underlying contract for %s", SYMBOL)
        return None, None

    contract = qualified_contracts[0]

    # 106 = underlying implied volatility
    ticker = ib.reqMktData(contract, "106", snapshot=False, regulatorySnapshot=False)

    try:
        deadline = time.monotonic() + 10.0

        underlying_price : float | None = None
        underlying_iv30  : float | None = None

        while time.monotonic() < deadline:
            underlying_price = get_mid_price(ticker)

            if _is_valid_number(ticker.impliedVolatility):
                underlying_iv30 = ticker.impliedVolatility

            if underlying_price is not None and underlying_iv30 is not None:
                break

            ib.sleep(0.5)

        logger.info(
            "Underlying market data %s price=%s iv=%s",
            SYMBOL,
            underlying_price,
            underlying_iv30,
        )

        return underlying_price, underlying_iv30

    finally:
        ib.cancelMktData(contract)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def save_option_snapshots(db_path: str, snapshots: list[OptionSnapshot]) -> None:
    if not snapshots:
        logger.info("No option snapshots to save")
        return

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO option_snapshots (
                timestamp_utc,
                symbol,
                con_id,
                local_symbol,
                expiry,
                strike,
                right,
                option_price,
                iv,
                delta,
                underlying_price,
                underlying_iv30,
                underlying_iv_rank_13w,
                underlying_iv_rank_52w
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    snapshot.timestamp_utc.isoformat(),
                    snapshot.symbol,
                    snapshot.con_id,
                    snapshot.local_symbol,
                    snapshot.expiry,
                    snapshot.strike,
                    snapshot.right,
                    snapshot.option_price,
                    snapshot.iv,
                    snapshot.delta,
                    snapshot.underlying_price,
                    snapshot.underlying_iv30,
                    snapshot.underlying_iv_rank_13w,
                    snapshot.underlying_iv_rank_52w,
                )
                for snapshot in snapshots
            ],
        )

    logger.info("Saved %d option snapshots", len(snapshots))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_utc_now() -> datetime:
    return datetime.now(UTC)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def collect_and_save_market_data_cycle(ib: IB, db_path: str) -> None:
    timestamp_utc = get_utc_now()

    underlying_price, underlying_iv30 = get_underlying_market_data(ib)

    positions = get_open_mstr_call_positions(ib)

    if not positions:
        logger.info("No open MSTR call positions found")
        return

    option_snapshots: list[OptionSnapshot] = []

    for position in positions:
        snapshot = get_option_snapshot(
            ib,
            position,
            timestamp_utc,
            underlying_price,
            underlying_iv30,
        )
        if snapshot is not None:
            option_snapshots.append(snapshot)

    save_option_snapshots(db_path, option_snapshots)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def test_db_write(db_path: str) -> None:
    logger.info("Starting database write test")

    # Inspect database with https://sqliteviewer.app
    init_db(db_path)

    test_ts = get_utc_now()

    option_snapshots = [
        OptionSnapshot(
            timestamp_utc          = test_ts,
            symbol                 = "MSTR",
            con_id                 = 123456789,
            local_symbol           = "MSTR  260424C00195000",
            expiry                 = "20260424",
            strike                 = 195.0,
            right                  = "C",
            option_price           = 110.50,
            iv                     = 0.62,
            delta                  = 0.123,
            underlying_price       = 301.25,
            underlying_iv30        = 0.55,
            underlying_iv_rank_13w = None,
            underlying_iv_rank_52w = None,
        ),
        OptionSnapshot(
            timestamp_utc          = test_ts,
            symbol                 = "MSTR",
            con_id                 = 123456789,
            local_symbol           = "MSTR  260424C00195000",
            expiry                 = "20260424",
            strike                 = 195.0,
            right                  = "C",
            option_price           = 110.50,
            iv                     = 0.62,
            delta                  = 0.123,
            underlying_price       = 301.25,
            underlying_iv30        = 0.55,
            underlying_iv_rank_13w = None,
            underlying_iv_rank_52w = None,
        ),
    ]

    save_option_snapshots(db_path, option_snapshots)

    logger.info("Database write test finished successfully")


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_next_quarter_hour(dt: datetime) -> datetime:
    schedule_time = 1
    if dt.second == 0 and dt.microsecond == 0 and dt.minute % schedule_time == 0:
        return dt

    dt_floor  = dt.replace(second=0, microsecond=0)
    remainder = dt_floor.minute % schedule_time

    if remainder == 0:
        return dt_floor + timedelta(minutes=schedule_time)

    return dt_floor + timedelta(minutes=(schedule_time - remainder))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def sleep_until(ib: IB, target_dt: datetime) -> None:
    while True:
        now     = datetime.now(target_dt.tzinfo)
        seconds = (target_dt - now).total_seconds()

        if seconds <= 0:
            return

        ib.sleep(min(seconds, 30.0))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def run_market_loop(ib: IB) -> None:
    schedule = get_mstr_market_schedule(ib)

    while True:
        state      = check_market_state(schedule)
        should_run = state.is_open or DEBUG_ALWAYS_GET_DATA

        if should_run:
            next_run = get_next_quarter_hour(state.now_local)

            mode_text = "market-open mode" if state.is_open else "debug-frozen mode"
            logger.info(
                "Scheduler active in %s. Next run at %s",
                mode_text,
                next_run.isoformat(),
            )

            sleep_until(ib, next_run)

            state = check_market_state(schedule)

            should_run = state.is_open or DEBUG_ALWAYS_GET_DATA
            if not should_run:
                logger.info("Market closed before scheduled run, skipping cycle")
                continue

            logger.info("Starting scheduled market-data cycle at %s", state.now_local.isoformat())

            collect_and_save_market_data_cycle(ib, DB_PATH)

        else:
            if state.next_session_start is None:
                logger.warning("No next session start found in current schedule, reloading schedule")
                ib.sleep(60.0)
                schedule = get_mstr_market_schedule(ib)
                continue

            logger.info(
                "Market is closed. Next session starts at %s",
                state.next_session_start.isoformat(),
            )

            sleep_until(ib, state.next_session_start)
            schedule = get_mstr_market_schedule(ib)


# -----------------------------------------------------------------------------
# -- main
# -----------------------------------------------------------------------------
def main() -> None:
    #
    # Initialize database
    init_db(DB_PATH)

    # Connect to IBKR api
    ib = connect_ib()

    try:
        run_market_loop(ib)
    finally:
        ib.disconnect()
        logger.info("Disconnected from IBKR")


# -----------------------------------------------------------------------------
# -- main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    configure_logger()

    if False:
        test_db_write(DB_PATH)

    logger.debug("Start main process")
    logger.warning("Hello")
    main()
