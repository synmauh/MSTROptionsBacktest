"""IV-Analyzer.

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
import smtplib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.message import EmailMessage

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# -----------------------------------------------------------------------------
# -- custom module import
# -----------------------------------------------------------------------------
from src.logger import configure_logger

from .ivtracker import DB_PATH, SYMBOL, get_utc_now

# -----------------------------------------------------------------------------
# -- logging
# -----------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# -- Constants
# -----------------------------------------------------------------------------

ANALYZE_INTERVAL_MINUTES = 5

LOOKBACK_DAYS = 7
SHOW_CHARTS   = True

EMAIL_ENABLED = True

SMTP_HOST     = "mail.lan.synergetik.de"
SMTP_PORT     = 25
SMTP_USERNAME = "your_mail@example.com"
SMTP_PASSWORD = "passwd"

EMAIL_FROM           = "options@synergetik.de"
EMAIL_TO             = ["markus.uhle@synergetik.de"]
EMAIL_SUBJECT_PREFIX = "[IV-Tracker]"


# -----------------------------------------------------------------------------
# -- Classes
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class IvDataPoint:
    timestamp_utc : datetime
    iv            : float


@dataclass(slots=True)
class IvExtremumSignal:
    con_id                 : int
    local_symbol           : str | None
    signal_type            : str | None  # "max", "min", None
    latest_iv              : float | None
    latest_smoothed_iv     : float | None
    window_min_iv          : float | None
    window_max_iv          : float | None
    turning_point_iv       : float | None
    turning_point_time_utc : datetime | None
    sample_count           : int
    reason                 : str


@dataclass(slots=True)
class IvChartSeries:
    local_symbol : str
    times        : list[datetime]
    ivs          : list[float]


# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

_chart_figure: Figure | None = None


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_active_contract_ids_from_db(
    db_path               : str,
    active_within_minutes : int = 60,
) -> list[int]:
    threshold_utc = get_utc_now() - timedelta(minutes=active_within_minutes)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT con_id
            FROM option_snapshots
            WHERE timestamp_utc >= ?
            ORDER BY con_id
            """,
            (threshold_utc.isoformat(),),
        ).fetchall()

    return [int(row[0]) for row in rows]


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_latest_local_symbol_for_contract(
    db_path : str,
    con_id  : int,
) -> str | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT local_symbol
            FROM option_snapshots
            WHERE con_id = ?
            ORDER BY timestamp_utc DESC
            LIMIT 1
            """,
            (con_id,),
        ).fetchone()

    if row is None:
        return None

    return str(row[0])


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def load_iv_series_for_contract(
    db_path       : str,
    con_id        : int,
    lookback_days : int = 7,
) -> list[IvDataPoint]:
    threshold_utc = get_utc_now() - timedelta(days=lookback_days)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT timestamp_utc, iv
            FROM option_snapshots
            WHERE con_id = ?
              AND timestamp_utc >= ?
              AND iv IS NOT NULL
            ORDER BY timestamp_utc ASC
            """,
            (con_id, threshold_utc.isoformat()),
        ).fetchall()

    result: list[IvDataPoint] = []

    for row in rows:
        timestamp_utc = datetime.fromisoformat(row[0])
        iv_value      = float(row[1])

        result.append(
            IvDataPoint(
                timestamp_utc = timestamp_utc,
                iv            = iv_value,
            ),
        )

    return result


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def load_chart_rows_for_contracts(
    db_path       : str,
    con_ids       : list[int],
    lookback_days : int = LOOKBACK_DAYS,
) -> list[tuple[str, int, str, float | None, float | None]]:
    if not con_ids:
        return []

    threshold_utc    = get_utc_now() - timedelta(days=lookback_days)
    placeholder_list = ", ".join("?" for _ in con_ids)

    query = f"""
        SELECT
            timestamp_utc,
            con_id,
            local_symbol,
            iv,
            underlying_price
        FROM option_snapshots
        WHERE con_id IN ({placeholder_list})
        AND timestamp_utc >= ?
        ORDER BY timestamp_utc ASC, con_id ASC
    """  # noqa: S608

    params: list[object] = [*con_ids, threshold_utc.isoformat()]

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    return [
        (
            str(row[0]),
            int(row[1]),
            str(row[2]),
            float(row[3]) if row[3] is not None else None,
            float(row[4]) if row[4] is not None else None,
        )
        for row in rows
    ]


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def smooth_iv_series(
    series      : list[IvDataPoint],
    window_size : int = 3,
) -> list[IvDataPoint]:
    if not series:
        return []

    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    if window_size == 1 or len(series) < window_size:
        return series.copy()

    half_window = window_size // 2
    result: list[IvDataPoint] = []

    for idx, point in enumerate(series):
        start_idx = max(0, idx - half_window)
        end_idx   = min(len(series), idx + half_window + 1)

        window = series[start_idx:end_idx]
        avg_iv = sum(item.iv for item in window) / len(window)

        result.append(
            IvDataPoint(
                timestamp_utc = point.timestamp_utc,
                iv            = avg_iv,
            ),
        )

    return result


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def compute_first_derivative(values: list[float]) -> list[float]:
    if len(values) < 2:
        return []

    return [values[idx] - values[idx - 1] for idx in range(1, len(values))]


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def detect_iv_extremum_for_series(
    con_id              : int,
    local_symbol        : str | None,
    series              : list[IvDataPoint],
    smoothing_window    : int = 3,
    min_required_points : int = 8,
    min_range_abs       : float = 0.02,
    min_move_fraction   : float = 0.05,
) -> IvExtremumSignal:
    if len(series) < min_required_points:
        return IvExtremumSignal(
            con_id                 = con_id,
            local_symbol           = local_symbol,
            signal_type            = None,
            latest_iv              = series[-1].iv if series else None,
            latest_smoothed_iv     = None,
            window_min_iv          = min((p.iv for p in series), default=None),
            window_max_iv          = max((p.iv for p in series), default=None),
            turning_point_iv       = None,
            turning_point_time_utc = None,
            sample_count           = len(series),
            reason                 = "not enough data points",
        )

    smoothed = smooth_iv_series(series, window_size=smoothing_window)

    raw_values      = [point.iv for point in series]
    smoothed_values = [point.iv for point in smoothed]

    derivatives = compute_first_derivative(smoothed_values)

    if len(derivatives) < 2:
        return IvExtremumSignal(
            con_id                 = con_id,
            local_symbol           = local_symbol,
            signal_type            = None,
            latest_iv              = raw_values[-1],
            latest_smoothed_iv     = smoothed_values[-1],
            window_min_iv          = min(raw_values),
            window_max_iv          = max(raw_values),
            turning_point_iv       = None,
            turning_point_time_utc = None,
            sample_count           = len(series),
            reason                 = "not enough derivative points",
        )

    # The point before the last point is the candidate for the local extremum
    candidate_idx = len(smoothed_values) - 2

    prev_slope = derivatives[-2]
    next_slope = derivatives[-1]

    window_min_iv = min(raw_values)
    window_max_iv = max(raw_values)
    iv_range      = window_max_iv - window_min_iv

    if iv_range <= 0:
        return IvExtremumSignal(
            con_id                 = con_id,
            local_symbol           = local_symbol,
            signal_type            = None,
            latest_iv              = raw_values[-1],
            latest_smoothed_iv     = smoothed_values[-1],
            window_min_iv          = window_min_iv,
            window_max_iv          = window_max_iv,
            turning_point_iv       = smoothed_values[candidate_idx],
            turning_point_time_utc = smoothed[candidate_idx].timestamp_utc,
            sample_count           = len(series),
            reason                 = "iv range is zero",
        )

    required_move = max(min_range_abs, iv_range * min_move_fraction)

    candidate_iv   = smoothed_values[candidate_idx]
    latest_iv      = raw_values[-1]
    latest_smooth  = smoothed_values[-1]
    candidate_time = smoothed[candidate_idx].timestamp_utc

    # Local maximum
    if prev_slope > 0 and next_slope < 0:
        if (candidate_iv - latest_smooth) >= required_move:
            upper_zone_threshold = window_max_iv - 0.10 * iv_range

            if candidate_iv >= upper_zone_threshold:
                return IvExtremumSignal(
                    con_id                 = con_id,
                    local_symbol           = local_symbol,
                    signal_type            = "max",
                    latest_iv              = latest_iv,
                    latest_smoothed_iv     = latest_smooth,
                    window_min_iv          = window_min_iv,
                    window_max_iv          = window_max_iv,
                    turning_point_iv       = candidate_iv,
                    turning_point_time_utc = candidate_time,
                    sample_count           = len(series),
                    reason                 = "local maximum detected near upper window range",
                )

    # Local minimum
    if prev_slope < 0 and next_slope > 0:
        if (latest_smooth - candidate_iv) >= required_move:
            lower_zone_threshold = window_min_iv + 0.10 * iv_range

            if candidate_iv <= lower_zone_threshold:
                return IvExtremumSignal(
                    con_id                 = con_id,
                    local_symbol           = local_symbol,
                    signal_type            = "min",
                    latest_iv              = latest_iv,
                    latest_smoothed_iv     = latest_smooth,
                    window_min_iv          = window_min_iv,
                    window_max_iv          = window_max_iv,
                    turning_point_iv       = candidate_iv,
                    turning_point_time_utc = candidate_time,
                    sample_count           = len(series),
                    reason                 = "local minimum detected near lower window range",
                )

    return IvExtremumSignal(
        con_id                 = con_id,
        local_symbol           = local_symbol,
        signal_type            = None,
        latest_iv              = latest_iv,
        latest_smoothed_iv     = latest_smooth,
        window_min_iv          = window_min_iv,
        window_max_iv          = window_max_iv,
        turning_point_iv       = candidate_iv,
        turning_point_time_utc = candidate_time,
        sample_count           = len(series),
        reason                 = "no relevant extremum detected",
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def analyze_active_contracts_from_db(
    db_path               : str,
    active_within_minutes : int = 60,
    lookback_days         : int = LOOKBACK_DAYS,
) -> list[IvExtremumSignal]:
    active_contract_ids       = get_active_contract_ids_from_db(
        db_path               = db_path,
        active_within_minutes = active_within_minutes,
    )

    logger.info("Found %d active contracts in database", len(active_contract_ids))

    signals: list[IvExtremumSignal] = []

    for con_id in active_contract_ids:
        local_symbol      = get_latest_local_symbol_for_contract(db_path, con_id)
        iv_series         = load_iv_series_for_contract(
            db_path       = db_path,
            con_id        = con_id,
            lookback_days = lookback_days,
        )

        signal           = detect_iv_extremum_for_series(
            con_id       = con_id,
            local_symbol = local_symbol,
            series       = iv_series,
        )

        signals.append(signal)

    return signals


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def log_iv_analysis_results(signals: list[IvExtremumSignal]) -> None:
    if not signals:
        logger.info("No IV analysis results available")
        return

    for signal in signals:
        logger.info(
            (
                "IV analysis: conId=%s localSymbol=%s signal=%s "
                "latest_iv=%s latest_smoothed_iv=%s "
                "window_min=%s window_max=%s "
                "turning_point_iv=%s turning_point_time=%s "
                "samples=%s reason=%s"
            ),
            signal.con_id,
            signal.local_symbol,
            signal.signal_type,
            signal.latest_iv,
            signal.latest_smoothed_iv,
            signal.window_min_iv,
            signal.window_max_iv,
            signal.turning_point_iv,
            signal.turning_point_time_utc.isoformat() if signal.turning_point_time_utc else None,
            signal.sample_count,
            signal.reason,
        )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def show_iv_chart_for_active_contracts(
    db_path               : str,
    active_within_minutes : int = 60,
    lookback_days         : int = LOOKBACK_DAYS,
) -> None:
    global _chart_figure

    active_contract_ids       = get_active_contract_ids_from_db(
        db_path               = db_path,
        active_within_minutes = active_within_minutes,
    )

    if not active_contract_ids:
        logger.info("No active contracts available for chart")
        return

    rows              = load_chart_rows_for_contracts(
        db_path       = db_path,
        con_ids       = active_contract_ids,
        lookback_days = lookback_days,
    )

    if not rows:
        logger.info("No chart data available in the selected lookback window")
        return

    underlying_points     : dict[datetime, float] = {}
    iv_series_by_contract : dict[int, IvChartSeries] = {}

    for timestamp_str, con_id, local_symbol, iv_value, underlying_price in rows:
        timestamp_utc = datetime.fromisoformat(timestamp_str)

        # Store underlying price only once per timestamp
        if underlying_price is not None and timestamp_utc not in underlying_points:
            underlying_points[timestamp_utc] = underlying_price

        # Create per-contract IV series container
        if con_id not in iv_series_by_contract:
            iv_series_by_contract[con_id]  = IvChartSeries(
                local_symbol               = local_symbol,
                times                      = [],
                ivs                        = [],
            )

        # Append IV point only if valid
        if iv_value is not None:
            iv_series_by_contract[con_id].times.append(timestamp_utc)
            iv_series_by_contract[con_id].ivs.append(iv_value)

    if _chart_figure is None or not plt.fignum_exists(_chart_figure.number):
        _chart_figure, ax_underlying = plt.subplots()
    else:
        _chart_figure.clear()
        ax_underlying = _chart_figure.add_subplot(111)

    ax_iv = ax_underlying.twinx()

    # Plot underlying only once
    underlying_values: list[float] = []
    if underlying_points:
        underlying_times     = sorted(underlying_points.keys())
        underlying_values    = [underlying_points[ts] for ts in underlying_times]
        underlying_times_num = mdates.date2num(underlying_times)

        ax_underlying.plot(
            underlying_times_num,
            underlying_values,
            label     = SYMBOL,
            linewidth = 3.0,
            color     = "black",
        )
        ax_underlying.set_ylabel("Underlying Price")

        underlying_min = min(underlying_values)
        underlying_max = max(underlying_values)
        if (underlying_max - underlying_min) < 10.0:
            center = (underlying_max + underlying_min) / 2.0
            ax_underlying.set_ylim(center - 5.0, center + 5.0)

    # Plot IV lines for all active contracts
    plotted_iv_count = 0
    all_iv_values: list[float] = []

    for series in iv_series_by_contract.values():
        if not series.times:
            continue

        times_num = mdates.date2num(series.times)
        ax_iv.plot(times_num, series.ivs, label=series.local_symbol)
        all_iv_values.extend(series.ivs)
        plotted_iv_count += 1

    ax_iv.set_ylabel("Implied Volatility")

    if all_iv_values:
        iv_min = min(all_iv_values)
        iv_max = max(all_iv_values)
        if (iv_max - iv_min) < 0.2:
            center = (iv_max + iv_min) / 2.0
            ax_iv.set_ylim(center - 0.1, center + 0.1)

    ax_underlying.set_xlabel(f"Time (last {lookback_days} days)")
    ax_underlying.set_title(f"{SYMBOL} underlying and option IVs")

    # Format x-axis as datetime
    ax_underlying.xaxis_date()
    ax_underlying.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

    # Build one combined legend
    handles_underlying, labels_underlying = ax_underlying.get_legend_handles_labels()
    handles_iv, labels_iv                 = ax_iv.get_legend_handles_labels()

    if handles_underlying or handles_iv:
        ax_underlying.legend(
            handles_underlying + handles_iv,
            labels_underlying + labels_iv,
            loc="best",
        )

    _chart_figure.autofmt_xdate()
    _chart_figure.tight_layout()
    _chart_figure.canvas.draw()
    _chart_figure.canvas.flush_events()
    plt.pause(0.001)

    logger.info(
        "Displayed IV chart with %d active contracts and %d underlying points",
        plotted_iv_count,
        len(underlying_points),
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def test_iv_extremum_analysis(db_path: str) -> None:
    logger.info("Starting IV extremum analysis test")

    signals                   = analyze_active_contracts_from_db(
        db_path               = db_path,
        active_within_minutes = 60,
        lookback_days         = LOOKBACK_DAYS,
    )

    log_iv_analysis_results(signals)

    max_count = sum(1 for signal in signals if signal.signal_type == "max")
    min_count = sum(1 for signal in signals if signal.signal_type == "min")

    logger.info(
        "IV extremum analysis test finished: total=%d max=%d min=%d",
        len(signals),
        max_count,
        min_count,
    )

    send_email_alerts_for_signals(db_path, signals)

    if SHOW_CHARTS:
        show_iv_chart_for_active_contracts(
            db_path               = db_path,
            active_within_minutes = 60,
            lookback_days         = LOOKBACK_DAYS,
        )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def send_email(
    subject : str,
    body    : str,
) -> None:
    if not EMAIL_ENABLED:
        logger.info("Email disabled. Would send: subject=%s body=%s", subject, body)
        return

    msg            = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = ", ".join(EMAIL_TO)
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        # smtp.starttls()
        # smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(msg)

    logger.info("Email sent: subject=%s", subject)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def build_signal_email_subject(signal: IvExtremumSignal) -> str:
    local_symbol = signal.local_symbol or f"conId={signal.con_id}"
    signal_text  = signal.signal_type.upper() if signal.signal_type else "UNKNOWN"

    return f"[IV-Tracker] {signal_text} detected for {local_symbol}"


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def build_signal_email_body(signal: IvExtremumSignal) -> str:
    return (
        f"IV extremum detected.\n\n"
        f"Contract ID           : {signal.con_id}\n"
        f"Local Symbol          : {signal.local_symbol}\n"
        f"Signal Type           : {signal.signal_type}\n"
        f"Latest IV             : {signal.latest_iv}\n"
        f"Latest Smoothed IV    : {signal.latest_smoothed_iv}\n"
        f"Window Min IV         : {signal.window_min_iv}\n"
        f"Window Max IV         : {signal.window_max_iv}\n"
        f"Turning Point IV      : {signal.turning_point_iv}\n"
        f"Turning Point Time    : "
        f"{signal.turning_point_time_utc.isoformat() if signal.turning_point_time_utc else None}\n"
        f"Sample Count          : {signal.sample_count}\n"
        f"Reason                : {signal.reason}\n"
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def send_email_alerts_for_signals(
    db_path : str,
    signals : list[IvExtremumSignal],
) -> None:
    alert_signals = [signal for signal in signals if signal.signal_type in ("max", "min")]

    if not alert_signals:
        logger.info("No email alerts to send")
        return

    for signal in alert_signals:
        if was_alert_already_sent(db_path, signal):
            logger.info(
                "Skipping duplicate alert for conId=%s localSymbol=%s signal=%s turningPoint=%s",
                signal.con_id,
                signal.local_symbol,
                signal.signal_type,
                signal.turning_point_time_utc.isoformat() if signal.turning_point_time_utc else None,
            )
            continue

        subject = build_signal_email_subject(signal)
        body    = build_signal_email_body(signal)

        logger.info(
            "Sending email alert for conId=%s localSymbol=%s signal=%s",
            signal.con_id,
            signal.local_symbol,
            signal.signal_type,
        )

        send_email(subject=subject, body=body)
        mark_alert_as_sent(db_path, signal)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def test_email_alert() -> None:
    send_email(
        subject = f"{EMAIL_SUBJECT_PREFIX} Test alert",
        body    = ("This is a test email from IV-Tracker.\n\nIf you receive this message, the SMTP configuration works."),
    )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def was_alert_already_sent(
    db_path : str,
    signal  : IvExtremumSignal,
) -> bool:
    turning_point_time = (
        signal.turning_point_time_utc.isoformat() if signal.turning_point_time_utc is not None else None
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM sent_alerts
            WHERE con_id = ?
              AND signal_type = ?
              AND (
                    (turning_point_time_utc IS NULL AND ? IS NULL)
                 OR turning_point_time_utc = ?
              )
            LIMIT 1
            """,
            (
                signal.con_id,
                signal.signal_type,
                turning_point_time,
                turning_point_time,
            ),
        ).fetchone()

    return row is not None


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def mark_alert_as_sent(
    db_path : str,
    signal  : IvExtremumSignal,
) -> None:
    turning_point_time = (
        signal.turning_point_time_utc.isoformat() if signal.turning_point_time_utc is not None else None
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sent_alerts (
                con_id,
                signal_type,
                turning_point_time_utc,
                sent_at_utc
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                signal.con_id,
                signal.signal_type,
                turning_point_time,
                get_utc_now().isoformat(),
            ),
        )


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def get_next_analysis_time(dt: datetime) -> datetime:
    if dt.second == 0 and dt.microsecond == 0 and dt.minute % ANALYZE_INTERVAL_MINUTES == 0:
        return dt

    dt_floor  = dt.replace(second=0, microsecond=0)
    remainder = dt_floor.minute % ANALYZE_INTERVAL_MINUTES

    if remainder == 0:
        return dt_floor + timedelta(minutes=ANALYZE_INTERVAL_MINUTES)

    return dt_floor + timedelta(minutes=(ANALYZE_INTERVAL_MINUTES - remainder))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def sleep_until(target_dt: datetime) -> None:
    while True:
        now     = datetime.now(target_dt.tzinfo)
        seconds = (target_dt - now).total_seconds()

        if seconds <= 0:
            return

        plt.pause(min(seconds, 0.5))


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def run_analyzer_loop(db_path: str) -> None:
    logger.info("Starting analyzer loop with interval=%d minutes", ANALYZE_INTERVAL_MINUTES)

    logger.info("Starting immediate analyzer cycle at %s", get_utc_now().isoformat())
    test_iv_extremum_analysis(db_path)

    while True:
        now      = get_utc_now()
        next_run = get_next_analysis_time(now)

        logger.info("Next analyzer run at %s", next_run.isoformat())
        sleep_until(next_run)

        logger.info("Starting analyzer cycle at %s", get_utc_now().isoformat())
        test_iv_extremum_analysis(db_path)


# -----------------------------------------------------------------------------
# -- main
# -----------------------------------------------------------------------------
def main() -> None:
    # test_email_alert()
    # test_iv_extremum_analysis(DB_PATH)
    plt.ion()
    plt.show(block=False)

    run_analyzer_loop(DB_PATH)


# -----------------------------------------------------------------------------
# -- main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    configure_logger()

    logger.debug("Start analyzer")
    main()
