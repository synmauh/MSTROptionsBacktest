"""Application entrypoint.

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
    - changing these sources is not allowed
"""

__author__ = "Markus Uhle"
__copyright__ = "Synergetik GmbH"


# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# -- custom module import
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# -- logging
# -----------------------------------------------------------------------------

# logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# -- constants
# -----------------------------------------------------------------------------

import os
from datetime import datetime, timedelta

import pandas as pd
from ib_insync import IB, LimitOrder, Option, util

# === CONFIGURATION ===
IB_HOST = "127.0.0.1"
IB_PORT = 4002  # paper TWS default
CLIENT_ID = 123
SYMBOL = "MSTR"
EXCEL_FILE = "mstr_options_eod.xlsx"

# Your earnings dates here (YYYY, M, D)
EARNINGS_DATES = [
    datetime(2025, 1, 16).astimezone().date(),
    datetime(2025, 4, 17).astimezone().date(),
    # ... add the rest
]


# === IB CONNECTION SETUP ===
def connect_ib():
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, readonly=True)  # , clientId=CLIENT_ID)
    # 2 = frozen (end-of-day) data
    ib.reqMarketDataType(2)
    return ib


# === FETCH ALL OPTION CONTRACTS FOR SYMBOL ===
def fetch_option_contracts(ib):
    details = ib.reqContractDetails(Option(SYMBOL, "", 0.0, "P", "SMART"))
    contracts = []
    for d in details:
        c = d.contract
        # include both calls and puts; adjust as needed
        contracts.append(
            Option(
                SYMBOL,
                c.lastTradeDateOrContractMonth,
                c.strike,
                c.right,
                "SMART",
                tradingClass=c.tradingClass,
            ),
        )
    return contracts


# === 1) EOD DOWNLOAD & EXCEL APPEND ===
def update_eod_data(ib):
    # Load existing or init empty DataFrame
    if os.path.exists(EXCEL_FILE):
        master_df = pd.read_excel(EXCEL_FILE, engine="openpyxl")
    else:
        master_df = pd.DataFrame(
            columns=[
                "contract",
                "expiry",
                "strike",
                "right",
                "date",
                "bid",
                "ask",
                "last",
                "volume",
            ],
        )

    # Determine backfill start
    if not master_df.empty:
        last_date = master_df["date"].max()
        start_dt = pd.to_datetime(last_date) + pd.Timedelta(days=1)
    else:
        start_dt = datetime.today() - pd.DateOffset(years=2)
    end_dt = datetime.today()

    contracts = fetch_option_contracts(ib)
    new_rows = []

    for c in contracts:
        bars = ib.reqHistoricalData(
            c,
            endDateTime=end_dt.strftime("%Y%m%d 23:59:59"),
            durationStr="2 Y",
            barSizeSetting="1 day",
            whatToShow="BID_ASK",
            useRTH=True,
            formatDate=1,
        )
        df = util.df(bars)
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] >= start_dt.date()]
        if df.empty:
            continue

        df["contract"] = c.localSymbol
        df["expiry"] = c.lastTradeDateOrContractMonth
        df["strike"] = c.strike
        df["right"] = c.right
        df["bid"] = df["high"]
        df["ask"] = df["low"]
        df["last"] = df["close"]
        df["volume"] = df["volume"]

        new_rows.append(
            df[
                [
                    "contract",
                    "expiry",
                    "strike",
                    "right",
                    "date",
                    "bid",
                    "ask",
                    "last",
                    "volume",
                ]
            ],
        )

    if new_rows:
        df_new = pd.concat(new_rows, ignore_index=True)
        master_df = pd.concat([master_df, df_new], ignore_index=True)
        master_df.drop_duplicates(subset=["contract", "date"], inplace=True)
        master_df.sort_values(["contract", "date"], inplace=True)
        master_df.to_excel(EXCEL_FILE, index=False, engine="openpyxl")
        print(f"Appended {len(df_new)} rows to {EXCEL_FILE}")
    else:
        print("No new EOD data to append.")


# === 2) MONTHLY ASSIGNMENT CHECK & 30DTE 10Δ ROLL ===
def check_assigned(ib, date):
    """Stub: return True if you were assigned (short put) on `date`.

    Implement by pulling your fills/trade history via ib.reqExecutions or your own logic.
    """
    return False  # TODO: implement


def find_delta_strike(ib, expiry, target_delta=0.10, right="P"):
    # fetch option parameters
    chain = ib.reqSecDefOptParams(SYMBOL, "", SYMBOL, "")
    params = next(p for p in chain if p.exchange == "SMART" and p.tradingClass)
    strikes = sorted(params.strikes)

    best = None
    for K in strikes:
        c = Option(SYMBOL, expiry, K, right, "SMART")
        tick = ib.reqMktData(c, "", False, False)
        ib.sleep(0.1)
        if tick.modelGreeks:
            delta = abs(tick.modelGreeks.delta)
            if delta >= target_delta:
                best = K
                ib.cancelMktData(tick)
                break
        ib.cancelMktData(tick)
    return best


def monthly_roll(ib):
    today = datetime.today().date()
    # find most recent past expiry
    prev = max((d for d in EARNINGS_DATES if d <= today), default=None)
    if not prev:
        return
    if check_assigned(ib, prev):
        # choose next monthly expiry ~30 days later
        next_exp = (prev + timedelta(days=30)).strftime("%Y%m%d")
        strike = find_delta_strike(ib, next_exp)
        if strike:
            c = Option(SYMBOL, next_exp, strike, "P", "SMART")
            tick = ib.reqMktData(c, "", False, False)
            ib.sleep(0.1)
            mid = (tick.bid + tick.ask) / 2
            order = LimitOrder("SELL", 1, mid)
            ib.placeOrder(c, order)
            ib.cancelMktData(tick)
            print(f"[Monthly Roll] Sold 1 {next_exp} {strike}P @ {mid}")


# === 3) WEEKLY 30DTE → 7DTE 10Δ ROLL ===
def weekly_roll(ib):
    today = datetime.today().date()
    # find and close existing 30DTE 10Δ short put
    for pos in ib.positions():
        if pos.contract.symbol == SYMBOL and pos.position < 0 and pos.contract.right == "P":
            tick = ib.reqMktData(pos.contract, "", False, False)
            ib.sleep(0.1)
            mid = (tick.bid + tick.ask) / 2
            buy = LimitOrder("BUY", abs(pos.position), mid)
            ib.placeOrder(pos.contract, buy)
            ib.cancelMktData(tick)
            print(f"[Weekly Roll] Bought back {pos.contract.localSymbol} @ {mid}")

    # sell new 7DTE 10Δ
    expiry_7d = (today + timedelta(days=7)).strftime("%Y%m%d")
    strike = find_delta_strike(ib, expiry_7d)
    if strike:
        c = Option(SYMBOL, expiry_7d, strike, "P", "SMART")
        tick = ib.reqMktData(c, "", False, False)
        ib.sleep(0.1)
        mid = (tick.bid + tick.ask) / 2
        sell = LimitOrder("SELL", 1, mid)
        ib.placeOrder(c, sell)
        ib.cancelMktData(tick)
        print(f"[Weekly Roll] Sold 1 {expiry_7d} {strike}P @ {mid}")


# === MAIN ===
def main():
    ib = connect_ib()
    try:
        update_eod_data(ib)
        monthly_roll(ib)
        weekly_roll(ib)
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()
