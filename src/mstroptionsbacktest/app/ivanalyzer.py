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

# -----------------------------------------------------------------------------
# -- custom module import
# -----------------------------------------------------------------------------
from src.logger import configure_logger

from .ivtracker import DB_PATH, get_utc_now

# -----------------------------------------------------------------------------
# -- logging
# -----------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# -- Constants
# -----------------------------------------------------------------------------

EMAIL_ENABLED = False

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
    lookback_days         : int = 7,
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
def test_iv_extremum_analysis(db_path: str) -> None:
    logger.info("Starting IV extremum analysis test")

    signals                   = analyze_active_contracts_from_db(
        db_path               = db_path,
        active_within_minutes = 60,
        lookback_days         = 7,
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
# -- main
# -----------------------------------------------------------------------------
def main() -> None:
    # test_email_alert()
    test_iv_extremum_analysis(DB_PATH)


# -----------------------------------------------------------------------------
# -- main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    configure_logger()

    logger.debug("Start analyzer")
    main()
