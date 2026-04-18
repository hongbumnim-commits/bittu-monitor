# -*- coding: utf-8 -*-
# bittu monitor v5
# Trend outlook + fetch error fixes.

import os
import json
import time
import datetime as dt
import re
from pathlib import Path

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
LOOKBACK_DAYS = 400

SECTOR_BASKETS = {
    "반도체":   ["005930", "000660", "042700", "403870"],
    "방산조선":  ["012450", "079550", "329180", "042660"],
    "바이오":   ["068270", "207940", "196170", "328130"],
    "2차전지":  ["373220", "006400", "051910", "096770"],
    "금융":     ["105560", "055550", "086790", "316140"],
}


GLOBAL_LIQUIDITY_SERIES = {
    "WALCL": "fed_assets",
    "ECBASSETSW": "ecb_assets",
    "JPNASSETS": "boj_assets",
}

INDEX_GO_KR_FOREIGN_HOLDING_TABLE_URL = (
    "https://www.index.go.kr/unity/potal/eNara/sub/showStblGams3.do"
    "?freq=Y&idx_cd=1086&period=N&stts_cd=108601"
)

def fetch_fred_series(series_id, name=None, days=LOOKBACK_DAYS * 3):
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        df = fdr.DataReader(f"FRED:{series_id}", start)
        if df is None or df.empty:
            return pd.Series(dtype=float, name=name or series_id)
        col = df.columns[0]
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return pd.Series(dtype=float, name=name or series_id)
        s.index = pd.to_datetime(s.index).date
        return s.rename(name or series_id)
    except Exception as e:
        print(f"  [warn] fred {series_id}: {e}")
        return pd.Series(dtype=float, name=name or series_id)


def fetch_usdjpy_series(days=LOOKBACK_DAYS * 3):
    usdjpy = fetch_fred_series("DEXJPUS", "usdjpy", days=days)
    if not usdjpy.empty:
        return usdjpy
    return pd.Series(dtype=float, name="usdjpy")


def fetch_usdkrw_series(days=LOOKBACK_DAYS):
    """원달러 환율 - FRED DEXKOUS (KRW per USD, 일별)."""
    s = fetch_fred_series("DEXKOUS", "usdkrw", days=days * 2)
    if not s.empty:
        latest = float(s.iloc[-1])
        print(f"  usdkrw (fred): {len(s)} rows, latest {latest:.1f}")
    return s


def fetch_global_liquidity_proxy():
    """A안: 주요 중앙은행 자산 합계(정규화 proxy) + USDJPY(Stooq 우선)."""
    series = {}
    for sid, nm in GLOBAL_LIQUIDITY_SERIES.items():
        s = fetch_fred_series(sid, nm)
        if not s.empty:
            series[nm] = s
    proxy = pd.Series(dtype=float, name="global_liquidity")
    if series:
        df = pd.concat(series.values(), axis=1).sort_index().ffill().dropna(how="all")
        if not df.empty:
            normalized = []
            for c in df.columns:
                col = pd.to_numeric(df[c], errors="coerce").dropna()
                if col.empty or col.iloc[0] == 0:
                    continue
                normalized.append(col / col.iloc[0] * 100)
            if normalized:
                proxy = pd.concat(normalized, axis=1).mean(axis=1).rename("global_liquidity")
    usdjpy = fetch_usdjpy_series(days=LOOKBACK_DAYS * 3)
    return {"global_liquidity": proxy, "usdjpy": usdjpy}


def safe(fn_name, fn, default=None, retries=3, sleep=2):
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"[warn] {fn_name} attempt {i+1} failed: {e}")
            time.sleep(sleep)
    print(f"[error] {fn_name} using default")
    return default


def fetch_fdr(symbol, name=None, days=LOOKBACK_DAYS):
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    end = TODAY.strftime("%Y-%m-%d")
    df = fdr.DataReader(symbol, start, end)
    if df.empty:
        return pd.Series(dtype=float, name=name or symbol)
    s = df["Close"]
    s.index = pd.to_datetime(s.index).date
    return s.rename(name or symbol)


def fetch_ust10y_fred():
    start = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    df = fdr.DataReader("FRED:DGS10", start)
    if df.empty:
        return pd.Series(dtype=float, name="ust10y")
    col = df.columns[0]
    s = df[col]
    s.index = pd.to_datetime(s.index).date
    return s.rename("ust10y").dropna()


def fetch_cor1m():
    """
    CBOE 1-Month Implied Correlation Index (^COR1M).
    S&P 500 구성종목 간 내재상관 - '모두 같이 움직일 확률'을 옵션으로 측정.
    높음(60+) = 시스템 공포, 낮음(<20) = 쏠림 극한 (역설적 위험).

    소스 우선순위:
      1) CBOE 공식 CDN CSV (cdn.cboe.com, VIX_History와 같은 패턴) - 가장 안정적
      2) FDR의 ^COR1M
      3) Yahoo chart API (여러 user-agent, 세션 쿠키 시도)
      4) Investing.com historical
    """
    import urllib.parse
    from io import StringIO

    # 1) CBOE 공식 CDN - 가장 안정적. GitHub IP도 막지 않음.
    cboe_urls = [
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_History.csv",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_Historical_Data.csv",
    ]
    for url in cboe_urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if r.status_code != 200 or len(r.text) < 50:
                continue
            # CSV 구조: 보통 "DATE,OPEN,HIGH,LOW,CLOSE" 또는 "Date,Close"
            content = r.text
            # 헤더에 Cboe 경고문구 등이 섞여있을 수 있음 → 실제 헤더 줄 찾기
            lines = content.splitlines()
            header_idx = None
            for i, line in enumerate(lines[:10]):
                if re.search(r"date", line, re.IGNORECASE):
                    header_idx = i
                    break
            if header_idx is None:
                continue
            clean_csv = "\n".join(lines[header_idx:])
            df_cboe = pd.read_csv(StringIO(clean_csv))
            # 날짜 컬럼 찾기
            date_col = None
            for c in df_cboe.columns:
                if re.search(r"date", str(c), re.IGNORECASE):
                    date_col = c
                    break
            if date_col is None:
                continue
            # 종가 컬럼 찾기
            close_col = None
            for c in df_cboe.columns:
                cl = str(c).lower()
                if "close" in cl or cl == "value":
                    close_col = c
                    break
            if close_col is None:
                # 컬럼이 Date와 하나만 있으면 그게 종가
                other_cols = [c for c in df_cboe.columns if c != date_col]
                if len(other_cols) >= 1:
                    close_col = other_cols[-1]  # 마지막 컬럼이 보통 종가
            if close_col is None:
                continue
            df_cboe[date_col] = pd.to_datetime(df_cboe[date_col], errors="coerce")
            df_cboe = df_cboe.dropna(subset=[date_col])
            df_cboe["_close"] = pd.to_numeric(df_cboe[close_col], errors="coerce")
            df_cboe = df_cboe.dropna(subset=["_close"])
            if df_cboe.empty:
                continue
            cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
            df_cboe = df_cboe[df_cboe[date_col].dt.date >= cutoff]
            if df_cboe.empty:
                continue
            s = pd.Series(
                df_cboe["_close"].values,
                index=df_cboe[date_col].dt.date.values,
                name="cor1m",
            )
            last = float(s.iloc[-1])
            if 1 < last < 100:
                print(f"  cor1m via cboe-cdn ({url.split('/')[-1]}): {len(s)} rows, latest {last:.2f}")
                return s
        except Exception as e:
            print(f"  [info] cboe cdn {url.split('/')[-1]}: {e}")

    # 2) FDR로 Yahoo ^COR1M 시도
    try:
        df = fdr.DataReader("^COR1M")
        if df is not None and not df.empty:
            s = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
            s = s.dropna()
            if not s.empty:
                s.index = pd.to_datetime(s.index).date
                cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
                s = s[s.index >= cutoff]
                last = float(s.iloc[-1]) if not s.empty else None
                if last and 1 < last < 100:
                    print(f"  cor1m via fdr: {len(s)} rows, latest {last:.2f}")
                    return s.rename("cor1m")
    except Exception as e:
        print(f"  [info] fdr cor1m failed: {e}")

    # 3) Yahoo chart API - 세션 쿠키 + 다양한 user-agent 시도
    end_ts = int(dt.datetime.now(KST).timestamp())
    start_ts = end_ts - LOOKBACK_DAYS * 86400
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]
    symbol_enc = urllib.parse.quote("^COR1M")
    for ua in user_agents:
        session = requests.Session()
        session.headers.update({
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://finance.yahoo.com/quote/%5ECOR1M/",
        })
        try:
            session.get("https://finance.yahoo.com/", timeout=10)
        except Exception:
            pass
        for url_base in [
            f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol_enc}",
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol_enc}",
        ]:
            try:
                r = session.get(url_base, params={"period1": start_ts, "period2": end_ts, "interval": "1d"}, timeout=15)
                if r.status_code != 200:
                    continue
                data = r.json()
                result = data["chart"]["result"][0]
                timestamps = result.get("timestamp", [])
                closes = result["indicators"]["quote"][0].get("close", [])
                if not timestamps or not closes:
                    continue
                dates = [dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).date() for t in timestamps]
                s = pd.Series(closes, index=dates, name="cor1m").dropna()
                if s.empty:
                    continue
                last = float(s.iloc[-1])
                if 1 < last < 100:
                    print(f"  cor1m via yahoo: {len(s)} rows, latest {last:.2f}")
                    return s
            except Exception:
                continue

    # 4) Investing.com 폴백
    try:
        s = fetch_investing_history("https://www.investing.com/indices/cboe-1month-implied-correlation-historical-data", "cor1m")
        if not s.empty:
            return s
    except Exception as e:
        print(f"  [info] investing cor1m: {e}")

    print(f"  [warn] cor1m: all sources failed")
    return pd.Series(dtype=float, name="cor1m")


def fetch_investing_history(url, name, days=LOOKBACK_DAYS):
    """Investing.com historical page fallback scraper."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.investing.com/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        tables = pd.read_html(r.text)
        cutoff = TODAY - dt.timedelta(days=days)
        for t in tables:
            cols = [str(c) for c in t.columns]
            flat_cols = " ".join(cols).lower()
            if "date" not in flat_cols or "price" not in flat_cols:
                continue
            date_col = next((c for c in t.columns if str(c).lower().startswith("date")), t.columns[0])
            price_col = next((c for c in t.columns if "price" in str(c).lower()), None)
            if price_col is None:
                continue
            tmp = t[[date_col, price_col]].copy()
            tmp.columns = ["date", "price"]
            tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
            tmp["price"] = pd.to_numeric(tmp["price"].astype(str).str.replace(",", "", regex=False), errors="coerce")
            tmp = tmp.dropna().sort_values("date")
            if tmp.empty:
                continue
            s = pd.Series(tmp["price"].values, index=tmp["date"].dt.date, name=name)
            s = s[s.index >= cutoff]
            if not s.empty:
                print(f"  {name} via investing: {len(s)} rows, latest {float(s.iloc[-1]):.2f}")
                return s
    except Exception as e:
        print(f"  [warn] investing {name}: {e}")
    return pd.Series(dtype=float, name=name)
def fetch_sector_basket(tickers):
    """섹터 종목 바스켓을 Base 100 누적 추세로 반환."""
    closes = []
    for t in tickers:
        s = safe(f"ticker_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=LOOKBACK_DAYS),
                 default=pd.Series(dtype=float))
        if not s.empty:
            closes.append(s)
    if not closes:
        return pd.Series(dtype=float)
    df = pd.concat(closes, axis=1).ffill().bfill()
    # 각 종목을 첫 유효값 기준 Base 100으로 정규화 후 평균
    first = df.iloc[0]
    # 0 또는 NaN이 있는 컬럼 제외
    valid_cols = first[(first > 0) & first.notna()].index
    if len(valid_cols) == 0:
        return pd.Series(dtype=float)
    normalized = df[valid_cols].div(first[valid_cols]) * 100
    return normalized.mean(axis=1)


# ================================================================
# 미국 신용잔고 - FINRA Margin Statistics (월별)
# ================================================================
# FINRA Rule 4521에 따라 증권사들이 매월 말일 기준으로 보고
# 공식 URL: finra.org의 margin-statistics.xlsx (매달 갱신)

FINRA_MARGIN_URLS = [
    "https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx",
    "https://www.finra.org/sites/default/files/margin-statistics.xlsx",
]


def fetch_us_margin_debt():
    """
    FINRA 증권사 신용잔고 (Margin Debt) - 월별 데이터.
    단위: 백만달러 (원본) → 십억달러로 변환
    Returns:
        dict {date(월말): debt_bil_usd}
    """
    from io import BytesIO

    def _parse_finra_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, (pd.Timestamp, dt.datetime, dt.date)):
            return val.date() if hasattr(val, 'date') else val
        s = str(val).strip()
        for fmt in ["%b-%y", "%B-%y", "%b %Y", "%B %Y", "%Y-%m", "%Y/%m", "%m/%Y", "%Y%m", "%b-%Y"]:
            try:
                return dt.datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        m = re.search(r'(20\d{2})[-/]?(\d{1,2})', s)
        if m:
            try:
                return dt.date(int(m.group(1)), int(m.group(2)), 1)
            except Exception:
                return None
        return None

    def _clean_num(v):
        try:
            s = str(v).replace(',', '').replace('$', '').strip()
            if s in ('', 'nan', 'None'):
                return None
            return float(s)
        except Exception:
            return None

    def _parse_sheet(df_raw):
        if df_raw is None or df_raw.empty:
            return {}
        # 헤더 행 탐지
        for header_row in range(min(40, len(df_raw))):
            row_txt = ' '.join([str(v) for v in df_raw.iloc[header_row].values if pd.notna(v)]).lower()
            if not row_txt:
                continue
            if ('year' in row_txt and 'month' in row_txt) or ('debit' in row_txt and 'margin' in row_txt):
                cols = [str(c).strip() if pd.notna(c) else f'col_{i}' for i, c in enumerate(df_raw.iloc[header_row].tolist())]
                df = df_raw.iloc[header_row + 1:].copy()
                df.columns = cols
                df = df.reset_index(drop=True)
                date_col = None
                debit_col = None
                for c in df.columns:
                    cl = str(c).lower()
                    if date_col is None and ('year' in cl or 'month' in cl or 'date' in cl):
                        date_col = c
                    if debit_col is None and 'debit' in cl and 'margin' in cl:
                        debit_col = c
                if date_col is None and len(df.columns) >= 1:
                    date_col = df.columns[0]
                if debit_col is None:
                    for c in df.columns[1:]:
                        if any(k in str(c).lower() for k in ['debit', 'margin']):
                            debit_col = c
                            break
                if date_col and debit_col:
                    out = {}
                    for _, row in df.iterrows():
                        d = _parse_finra_date(row.get(date_col))
                        v = _clean_num(row.get(debit_col))
                        if d and v and v > 10000:
                            out[d] = round(v / 1000, 1)
                    if len(out) >= 6:
                        return out
        # 열 추론 fallback
        out = {}
        first_col = df_raw.columns[0]
        for c in df_raw.columns[1:]:
            vals = [_clean_num(v) for v in df_raw[c].tolist()]
            valid = [v for v in vals if v is not None]
            if len(valid) < 6 or max(valid) < 10000:
                continue
            temp = {}
            for i in range(len(df_raw)):
                d = _parse_finra_date(df_raw.iloc[i][first_col])
                v = _clean_num(df_raw.iloc[i][c])
                if d and v and v > 10000:
                    temp[d] = round(v / 1000, 1)
            if len(temp) > len(out):
                out = temp
        return out

    for url in FINRA_MARGIN_URLS:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code != 200:
                continue
            xl = pd.read_excel(BytesIO(r.content), sheet_name=None, engine='openpyxl', header=None)
            for sheet_name, df_raw in xl.items():
                result = _parse_sheet(df_raw)
                if result:
                    cutoff = TODAY - dt.timedelta(days=36 * 31)
                    result = {d: v for d, v in result.items() if d >= cutoff}
                    if result:
                        latest = max(result)
                        print(f"  us margin debt (finra:{sheet_name}): {len(result)} months, latest {latest} ${result[latest]:.1f}B")
                        return dict(sorted(result.items()))
        except Exception as e:
            print(f"  [warn] finra {url.split('/')[-1]}: {e}")

    # YCharts fallback
    try:
        r = requests.get('https://ycharts.com/indicators/finra_margin_debt', headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if r.status_code == 200:
            pairs = re.findall(r'"formatted_date":"([A-Za-z]{3} \d{1,2}, \d{4})"[^\}]*?"raw_data":([0-9.]+)', r.text)
            result = {}
            for ds, vs in pairs:
                try:
                    d = dt.datetime.strptime(ds, '%b %d, %Y').date().replace(day=1)
                    result[d] = round(float(vs) / 1000, 1)
                except Exception:
                    continue
            if result:
                cutoff = TODAY - dt.timedelta(days=36 * 31)
                result = {d: v for d, v in result.items() if d >= cutoff}
                if result:
                    latest = max(result)
                    print(f"  us margin debt (ycharts): {len(result)} months, latest {latest} ${result[latest]:.1f}B")
                    return dict(sorted(result.items()))
    except Exception as e:
        print(f"  [warn] ycharts margin debt: {e}")

    print(f"  [warn] us margin debt: all sources failed")
    return {}


# ================================================================
# M7 + AVGO + TSM 바스켓 (개별 종목 누적 추세)
# ================================================================

M7_PLUS_STOCKS = {
    "SP500": "S&P500",
    "IXIC": "나스닥",
    "AAPL":  "애플",
    "MSFT":  "마이크로소프트",
    "GOOGL": "구글",
    "AMZN":  "아마존",
    "META":  "메타",
    "NVDA":  "엔비디아",
    "TSLA":  "테슬라",
    "AVGO":  "브로드컴",
    "TSM":   "TSMC",
}


def fetch_m7_plus_basket():
    """M7 + AVGO + TSM 개별 종목 + S&P500/나스닥. dict {ticker: Series} 반환."""
    result = {}
    for ticker in M7_PLUS_STOCKS.keys():
        source_symbol = "US500" if ticker == "SP500" else ticker
        if ticker == "IXIC":
            source_symbol = "IXIC"
        s = safe(f"m7_{ticker}", lambda ts=source_symbol, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
                 default=pd.Series(dtype=float))
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  m7+ basket: {len(result)} tickers")
    return result


# ================================================================
# 한국 외국인 주식 보유금액 & 비중 - index.go.kr (월별)
# ================================================================
def fetch_foreign_holding_kr():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    result = {}

    def _parse_periods(header_text):
        periods = []
        for year, month in re.findall(r"(20\d{2})(?:\s*(\d{1,2})월)?", header_text):
            y = int(year)
            m = int(month) if month else 12
            if 1 <= m <= 12:
                d = dt.date(y, m, 1)
                if d not in periods:
                    periods.append(d)
        return periods

    def _parse_values(line):
        values = []
        for raw in re.findall(r"-?\d[\d,]*\.?\d*", line):
            try:
                values.append(float(raw.replace(",", "")))
            except ValueError:
                continue
        return values

    try:
        r = requests.get(INDEX_GO_KR_FOREIGN_HOLDING_TABLE_URL, headers=headers, timeout=20)
        if r.status_code != 200:
            print(f"  [info] foreign holding kr: status {r.status_code}")
            return result

        text = BeautifulSoup(r.text, "html.parser").get_text("\n", strip=True)
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
        header_text = " ".join(lines[:20])
        periods = _parse_periods(header_text)
        amount_line = next((line for line in lines if "보유금액" in line and "외국인" in line), "")
        pct_line = next((line for line in lines if "시가총액대비" in line and "외국인" in line), "")
        amounts = _parse_values(amount_line)
        pcts = _parse_values(pct_line)

        if not periods or not amounts:
            print(f"  [info] foreign holding kr: table parse empty (periods={len(periods)}, amounts={len(amounts)})")
            return result

        for d, amount in zip(periods, amounts):
            result[d] = {"amount_trillion": amount, "pct": None}
        for d, pct in zip(periods, pcts):
            result.setdefault(d, {"amount_trillion": None, "pct": None})
            result[d]["pct"] = pct

        cutoff = TODAY - dt.timedelta(days=36 * 31)
        result = {d: v for d, v in result.items() if d >= cutoff}
        if result:
            latest = max(result.keys())
            latest_amount = result[latest].get("amount_trillion")
            latest_pct = result[latest].get("pct")
            amt_str = f"{latest_amount:.1f}조원" if latest_amount is not None else "-"
            pct_str = f"{latest_pct:.1f}%" if latest_pct is not None else "-"
            print(f"  foreign holding kr (index.go.kr): {len(result)} months, latest {latest}: {amt_str} / {pct_str}")
        else:
            print("  [info] foreign holding kr: no recent rows")
    except Exception as e:
        print(f"  [info] foreign holding kr: {e}")
    return result


MAIN_COLS = [
    "date",
    "kospi", "kosdaq", "samsung", "hynix",
    "credit_balance_eok", "forced_sale_eok",
    "samsung_ret_pct", "hynix_ret_pct",
    "sp500", "nasdaq", "vix", "nvda", "ust10y", "cor1m",
    "sec_반도체", "sec_방산조선", "sec_바이오", "sec_2차전지", "sec_금융",
]


def load_history():
    fp = DATA_DIR / "history.csv"
    if not fp.exists():
        return pd.DataFrame(columns=MAIN_COLS)
    try:
        df = pd.read_csv(fp)
        if df.empty or "date" not in df.columns:
            return pd.DataFrame(columns=MAIN_COLS)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])
        for c in MAIN_COLS:
            if c not in df.columns:
                df[c] = None
        return df[MAIN_COLS]
    except Exception as e:
        print(f"[warn] load_history: {e}")
        return pd.DataFrame(columns=MAIN_COLS)


def save_history(df):
    fp = DATA_DIR / "history.csv"
    if df.empty:
        fp.write_text(",".join(MAIN_COLS) + "\n")
        return df
    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS * 2)
    df = df[df["date"] >= cutoff].sort_values("date").drop_duplicates("date", keep="last")
    df.to_csv(fp, index=False)
    return df


def update_data():
    print(f"[{TODAY}] Fetching all data...")

    kospi = safe("kospi",  lambda: fetch_fdr("KS11", "kospi"),  default=pd.Series(dtype=float, name="kospi"))
    kosdaq = safe("kosdaq", lambda: fetch_fdr("KQ11", "kosdaq"), default=pd.Series(dtype=float, name="kosdaq"))
    samsung = safe("samsung", lambda: fetch_fdr("005930", "samsung"), default=pd.Series(dtype=float, name="samsung"))
    hynix = safe("hynix", lambda: fetch_fdr("000660", "hynix"), default=pd.Series(dtype=float, name="hynix"))

    sp500 = safe("sp500", lambda: fetch_fdr("US500", "sp500"), default=pd.Series(dtype=float, name="sp500"))
    nasdaq = safe("nasdaq", lambda: fetch_fdr("IXIC", "nasdaq"), default=pd.Series(dtype=float, name="nasdaq"))
    vix = safe("vix", lambda: fetch_fdr("VIX", "vix"), default=pd.Series(dtype=float, name="vix"))
    nvda = safe("nvda", lambda: fetch_fdr("NVDA", "nvda"), default=pd.Series(dtype=float, name="nvda"))
    ust10y = safe("ust10y_fred", fetch_ust10y_fred, default=pd.Series(dtype=float, name="ust10y"))
    cor1m = safe("cor1m", fetch_cor1m, default=pd.Series(dtype=float, name="cor1m"))

    sectors = {}
    for name, tickers in SECTOR_BASKETS.items():
        s = safe(f"sector_{name}", lambda tt=tickers: fetch_sector_basket(tt), default=pd.Series(dtype=float))
        sectors[f"sec_{name}"] = s.rename(f"sec_{name}")

    # --- 한국 시장 펀더멘털 데이터 ---
    # 1. 신용공여잔고 - data.go.kr 우선, 실패 시 네이버 폴백 (단일값만)
    def _credit_balance_with_fallback():
        try:
            return fetch_credit_balance()  # {date: eok}
        except Exception as e:
            return {}
            print(f"  [info] data.go.kr credit failed, fallback to naver: {e}")
            return {}

    # 2. 증시자금추이 (반대매매, 예탁금) - data.go.kr 우선, 실패 시 네이버
    def _market_capital_with_fallback():
        try:
            return fetch_securities_market_capital()  # {'forced_sale': {...}, 'investor_deposit': {...}}
        except Exception as e:
            return {}

    credit_map = safe("credit", _credit_balance_with_fallback, default={})
    market_cap = safe("market_cap", _market_capital_with_fallback, default={"forced_sale": {}, "investor_deposit": {}})

    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix,
        "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y,
        "cor1m": cor1m,
        **sectors
    }
    df = pd.DataFrame(series_dict)
    df = df.reset_index().rename(columns={"index": "date"})
    df["samsung_ret_pct"] = df["samsung"].pct_change() * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change() * 100
    df["credit_balance_eok"] = None
    df["forced_sale_eok"] = None

    # 신용공여잔고 채우기
    for cdate, cval in credit_map.items():
        if cdate in df["date"].values:
            df.loc[df["date"] == cdate, "credit_balance_eok"] = cval

    # 반대매매 채우기
    for fdate, fval in market_cap.get("forced_sale", {}).items():
        if fdate in df["date"].values:
            df.loc[df["date"] == fdate, "forced_sale_eok"] = fval

    for c in MAIN_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[MAIN_COLS]

    history = load_history()
    combined = pd.concat([history, df], ignore_index=True)
    combined = save_history(combined)
    print(f"  saved: {len(combined)} rows, latest: {combined['date'].max() if not combined.empty else 'none'}")

    # --- 추가 데이터 (월별/별도) ---
    us_margin_debt = safe("us_margin_debt", fetch_us_margin_debt, default={})
    m7_basket = safe("m7_plus", fetch_m7_plus_basket, default={})
    kr_foreign_holding = safe("foreign_holding_kr", fetch_foreign_holding_kr, default={})
    gl = safe("global_liquidity", fetch_global_liquidity_proxy, default={"global_liquidity": pd.Series(dtype=float), "usdjpy": pd.Series(dtype=float)})
    usdkrw = safe("usdkrw", fetch_usdkrw_series, default=pd.Series(dtype=float, name="usdkrw"))

    extras = {
        "us_margin_debt": us_margin_debt,
        "m7_basket": m7_basket,
        "kr_foreign_holding": kr_foreign_holding,
        "global_liquidity": gl.get("global_liquidity", pd.Series(dtype=float)),
        "usdjpy": gl.get("usdjpy", pd.Series(dtype=float)),
        "usdkrw": usdkrw,
    }
    return combined, extras


def level_from_gap(v, thresholds):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0
    if v >= thresholds[2]: return 3
    if v >= thresholds[1]: return 2
    if v >= thresholds[0]: return 1
    return 0


def pct_deviation_from_ma(series, window=200):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < window:
        return None
    ma = s.rolling(window).mean().iloc[-1]
    cur = s.iloc[-1]
    if ma == 0 or pd.isna(ma):
        return None
    return float((cur / ma - 1) * 100)


def compute_signals(df, extras=None):
    if extras is None:
        extras = {}
    if df.empty:
        return {"kr": {}, "us": {}, "score_kr": 0, "max_kr": 18, "score_us": 0, "max_us": 21,
                "label_kr": "대기", "label_us": "대기"}

    df = df.sort_values("date").reset_index(drop=True)
    last = df.iloc[-1]
    kr, us = {}, {}

    forced_series = pd.to_numeric(df["forced_sale_eok"], errors="coerce").dropna()
    avg20 = float(forced_series.tail(20).mean()) if len(forced_series) else 0.0
    today_forced = float(last.get("forced_sale_eok") or 0)
    peak = max(today_forced, avg20)
    kr["KR1"] = {
        "name": "반대매매 일평균",
        "level": level_from_gap(peak, [200, 400, 600]),
        "description": f"오늘 {today_forced:.0f}억 / 20일 평균 {avg20:.0f}억"
    }

    kospi_ser = pd.to_numeric(df["kospi"], errors="coerce").dropna()
    credit_ser = pd.to_numeric(df["credit_balance_eok"], errors="coerce").dropna()
    kr["KR2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}
    if len(kospi_ser) >= 30 and len(credit_ser) >= 30:
        kospi_30d = (kospi_ser.iloc[-1] / kospi_ser.iloc[-30] - 1) * 100
        credit_30d = (credit_ser.iloc[-1] / credit_ser.iloc[-30] - 1) * 100
        gap = credit_30d - kospi_30d
        kr["KR2"] = {
            "name": "신용잔고 증가율이 코스피 추월",
            "level": level_from_gap(gap, [0.5, 1.0, 2.0]),
            "description": f"30일 코스피 {kospi_30d:+.1f}% vs 신용잔고 {credit_30d:+.1f}% ({gap:+.1f}%p)"
        }

    last5 = df.tail(5)
    sret = pd.to_numeric(last5["samsung_ret_pct"], errors="coerce")
    hret = pd.to_numeric(last5["hynix_ret_pct"], errors="coerce")
    both = int(((sret <= -3) & (hret <= -3)).sum())
    any_drop = bool((sret <= -3).any() or (hret <= -3).any())
    lvl = 3 if both >= 2 else 2 if both == 1 else 1 if any_drop else 0
    kr["KR3"] = {
        "name": "반도체 양대장 동시 -3%",
        "level": lvl,
        "description": f"최근 5일 동시 -3%: {both}회"
    }

    kr["KR4"] = {"name": "외국인 7일 누적 순매도", "level": 0, "description": "데이터 부족"}
    if len(foreign_ser) >= 7:
        sum7 = float(foreign_ser.tail(7).sum())
        outflow = -sum7 / 10000
        kr["KR4"] = {
            "name": "외국인 7일 누적 순매도",
            "level": level_from_gap(outflow, [1.0, 3.0, 5.0]),
            "description": f"최근 7영업일 합계 {sum7/10000:+.2f}조"
        }

    dev_kospi = pct_deviation_from_ma(df["kospi"], 200)
    kr["KR5"] = {"name": "코스피-200일선 괴리", "level": 0, "description": "데이터 부족"}
    if dev_kospi is not None:
        kr["KR5"] = {
            "name": "코스피-200일선 괴리율",
            "level": level_from_gap(abs(dev_kospi), [10, 20, 30]),
            "description": f"200일선 대비 {dev_kospi:+.1f}%"
        }

    sp500_ser = pd.to_numeric(df["sp500"], errors="coerce").dropna()
    us["US1"] = {"name": "S&P500 일간 변동성", "level": 0, "description": "데이터 부족"}
    if len(sp500_ser) >= 2:
        ret = (sp500_ser.iloc[-1] / sp500_ser.iloc[-2] - 1) * 100
        us["US1"] = {
            "name": "S&P500 전일 대비",
            "level": level_from_gap(abs(ret), [1, 2, 3]),
            "description": f"전일 대비 {ret:+.2f}%"
        }

    dev_nas = pct_deviation_from_ma(df["nasdaq"], 200)
    us["US2"] = {"name": "나스닥-200일선 괴리", "level": 0, "description": "데이터 부족"}
    if dev_nas is not None:
        us["US2"] = {
            "name": "나스닥-200일선 괴리율",
            "level": level_from_gap(abs(dev_nas), [10, 20, 30]),
            "description": f"200일선 대비 {dev_nas:+.1f}%"
        }

    vix_ser = pd.to_numeric(df["vix"], errors="coerce").dropna()
    us["US3"] = {"name": "VIX 공포지수", "level": 0, "description": "데이터 부족"}
    if len(vix_ser):
        v = float(vix_ser.iloc[-1])
        us["US3"] = {
            "name": "VIX 공포지수",
            "level": level_from_gap(v, [20, 30, 40]),
            "description": f"현재 {v:.1f}"
        }

    nvda_ser = pd.to_numeric(df["nvda"], errors="coerce").dropna()
    us["US4"] = {"name": "엔비디아 일간", "level": 0, "description": "데이터 부족"}
    if len(nvda_ser) >= 2:
        ret = (nvda_ser.iloc[-1] / nvda_ser.iloc[-2] - 1) * 100
        us["US4"] = {
            "name": "엔비디아 전일 대비",
            "level": level_from_gap(abs(ret), [3, 5, 8]),
            "description": f"전일 대비 {ret:+.2f}%"
        }

    ust_ser = pd.to_numeric(df["ust10y"], errors="coerce").dropna()
    us["US5"] = {"name": "10년물 금리 1주 변화", "level": 0, "description": "데이터 부족"}
    if len(ust_ser) >= 5:
        delta = float(ust_ser.iloc[-1] - ust_ser.iloc[-5])
        cur = float(ust_ser.iloc[-1])
        us["US5"] = {
            "name": "10년물 국채금리 1주 변화",
            "level": level_from_gap(abs(delta) * 100, [15, 25, 40]),
            "description": f"현재 {cur:.2f}% / 1주 변화 {delta*100:+.0f}bp"
        }

    # US6: CBOE 1-Month Implied Correlation (COR1M)
    # 양방향 경고 지표 - 높을 때(시스템 공포)와 낮을 때(쏠림 극한) 모두 위험 신호
    cor_ser = pd.to_numeric(df["cor1m"], errors="coerce").dropna()
    us["US6"] = {"name": "내재상관 COR1M", "level": 0, "description": "데이터 수집 중"}
    if len(cor_ser):
        v = float(cor_ser.iloc[-1])
        # 양방향 위험 판정
        if v >= 60:
            lvl = 3; why = "시스템 공포 (동반 하락 예상)"
        elif v >= 45:
            lvl = 2; why = "상관 급등 - 섹터 차별화 소멸"
        elif v >= 30:
            lvl = 1; why = "상승 추세 - 주의 필요"
        elif v <= 10:
            lvl = 3; why = "쏠림 극한 - 변동성 폭발 위험"
        elif v <= 15:
            lvl = 2; why = "극도의 쏠림 - 경계"
        elif v <= 20:
            lvl = 1; why = "낮음 - 소수 종목 주도"
        else:
            lvl = 0; why = "정상 범위 (20~30)"
        us["US6"] = {
            "name": "CBOE 1M 내재상관 (COR1M)",
            "level": lvl,
            "description": f"현재 {v:.1f} · {why}"
        }

    # US7: 미국 신용잔고 (FINRA Margin Debt) 월별 추세
    # 3개월 증가율이 S&P500 상승률을 크게 앞지르면 과열 신호
    us["US7"] = {"name": "미국 신용잔고 추세", "level": 0, "description": "데이터 수집 중"}
    margin_debt = extras.get("us_margin_debt", {})
    if margin_debt and len(margin_debt) >= 4:
        sorted_dates = sorted(margin_debt.keys())
        latest_v = margin_debt[sorted_dates[-1]]
        # 3개월 전 값 (월별이므로 인덱스 -4 = 약 3개월 전)
        if len(sorted_dates) >= 4:
            prev3m_v = margin_debt[sorted_dates[-4]]
            debt_growth_3m = (latest_v / prev3m_v - 1) * 100 if prev3m_v > 0 else 0
            # S&P500 3개월 증가율
            sp_ser = pd.to_numeric(df["sp500"], errors="coerce").dropna()
            if len(sp_ser) >= 63:
                sp_growth_3m = (sp_ser.iloc[-1] / sp_ser.iloc[-63] - 1) * 100
                gap = debt_growth_3m - sp_growth_3m
                us["US7"] = {
                    "name": "미국 신용잔고 vs S&P500 (3개월)",
                    "level": level_from_gap(gap, [2.0, 5.0, 10.0]),
                    "description": f"3개월 S&P500 {sp_growth_3m:+.1f}% vs 신용잔고 {debt_growth_3m:+.1f}% (gap {gap:+.1f}%p) · ${latest_v:.0f}B"
                }
            else:
                us["US7"] = {
                    "name": "미국 신용잔고 (3개월 변화)",
                    "level": level_from_gap(debt_growth_3m, [3, 7, 12]),
                    "description": f"3개월 증가율 {debt_growth_3m:+.1f}% · ${latest_v:.0f}B"
                }

    kr_score = sum(s["level"] for s in kr.values())
    us_score = sum(s["level"] for s in us.values())

    def label(score, maxi):
        pct = score / maxi if maxi else 0
        if pct >= 0.7: return "위험"
        if pct >= 0.5: return "경고"
        if pct >= 0.3: return "경계"
        if pct >= 0.15: return "주의"
        return "평상"

    return {
        "kr": kr, "us": us,
        "score_kr": kr_score, "max_kr": len(kr) * 3,
        "score_us": us_score, "max_us": len(us) * 3,
        "label_kr": label(kr_score, len(kr) * 3),
        "label_us": label(us_score, len(us) * 3),
    }


def compute_regime(index_series):
    s = pd.to_numeric(index_series, errors="coerce").dropna()
    result = {
        "short": {"label": "대기", "score": 0, "reasons": []},
        "mid":   {"label": "대기", "score": 0, "reasons": []},
        "long":  {"label": "대기", "score": 0, "reasons": []},
    }
    if len(s) < 200:
        return result

    cur = s.iloc[-1]
    ma50 = s.rolling(50).mean().iloc[-1]
    ma200 = s.rolling(200).mean().iloc[-1]
    ma50_20prev = s.rolling(50).mean().iloc[-21] if len(s) > 220 else ma50
    ma200_30prev = s.rolling(200).mean().iloc[-31] if len(s) > 230 else ma200
    high52w = s.tail(252).max() if len(s) >= 252 else s.max()
    low52w = s.tail(252).min() if len(s) >= 252 else s.min()

    short_score = 0
    ret30d = (cur / s.iloc[-30] - 1) * 100 if len(s) >= 30 else 0
    if ret30d > 5:
        short_score += 2; result["short"]["reasons"].append(f"최근 30일 +{ret30d:.1f}% (강한 상승)")
    elif ret30d > 2:
        short_score += 1; result["short"]["reasons"].append(f"최근 30일 +{ret30d:.1f}%")
    elif ret30d < -5:
        short_score -= 2; result["short"]["reasons"].append(f"최근 30일 {ret30d:.1f}% (강한 하락)")
    elif ret30d < -2:
        short_score -= 1; result["short"]["reasons"].append(f"최근 30일 {ret30d:.1f}%")
    else:
        result["short"]["reasons"].append(f"최근 30일 {ret30d:+.1f}% (보합)")

    ma50_slope = (ma50 / ma50_20prev - 1) * 100 if ma50_20prev else 0
    if ma50_slope > 2:
        short_score += 1; result["short"]["reasons"].append(f"50일선 우상향 +{ma50_slope:.1f}%")
    elif ma50_slope < -2:
        short_score -= 1; result["short"]["reasons"].append(f"50일선 우하향 {ma50_slope:.1f}%")

    mid_score = 0
    if ma50 > ma200 * 1.02:
        mid_score += 2; result["mid"]["reasons"].append("50일선이 200일선 위 (골든크로스 유지)")
    elif ma50 > ma200:
        mid_score += 1; result["mid"]["reasons"].append("50일선이 200일선 약간 위")
    elif ma50 < ma200 * 0.98:
        mid_score -= 2; result["mid"]["reasons"].append("50일선이 200일선 아래 (데드크로스)")
    else:
        mid_score -= 1; result["mid"]["reasons"].append("50일선이 200일선 약간 아래")

    if len(s) >= 63:
        ret3m = (cur / s.iloc[-63] - 1) * 100
        if ret3m > 10:
            mid_score += 1; result["mid"]["reasons"].append(f"3개월 +{ret3m:.1f}%")
        elif ret3m < -10:
            mid_score -= 1; result["mid"]["reasons"].append(f"3개월 {ret3m:.1f}%")

    long_score = 0
    ma200_slope = (ma200 / ma200_30prev - 1) * 100 if ma200_30prev else 0
    if ma200_slope > 3:
        long_score += 2; result["long"]["reasons"].append(f"200일선 상승 +{ma200_slope:.1f}%")
    elif ma200_slope > 0:
        long_score += 1; result["long"]["reasons"].append(f"200일선 완만 상승 +{ma200_slope:.1f}%")
    elif ma200_slope < -3:
        long_score -= 2; result["long"]["reasons"].append(f"200일선 하락 {ma200_slope:.1f}%")
    else:
        long_score -= 1; result["long"]["reasons"].append(f"200일선 정체 {ma200_slope:+.1f}%")

    pos_52w = (cur - low52w) / (high52w - low52w) if high52w > low52w else 0.5
    if pos_52w > 0.85:
        long_score += 1; result["long"]["reasons"].append(f"52주 고점 근처 ({pos_52w*100:.0f}%)")
    elif pos_52w < 0.15:
        long_score -= 1; result["long"]["reasons"].append(f"52주 저점 근처 ({pos_52w*100:.0f}%)")
    else:
        result["long"]["reasons"].append(f"52주 범위 {pos_52w*100:.0f}% 지점")

    def label(score):
        if score >= 2: return "강세"
        if score >= 1: return "약한 강세"
        if score <= -2: return "약세"
        if score <= -1: return "약한 약세"
        return "중립"

    result["short"]["score"] = short_score
    result["short"]["label"] = label(short_score)
    result["mid"]["score"] = mid_score
    result["mid"]["label"] = label(mid_score)
    result["long"]["score"] = long_score
    result["long"]["label"] = label(long_score)
    return result


COLOR = {
    "평상": "#1D9E75", "주의": "#BA7517", "경계": "#D85A30",
    "경고": "#A32D2D", "위험": "#501313", "대기": "#888"
}

REGIME_COLOR = {
    "강세": "#1D9E75", "약한 강세": "#97C459",
    "중립": "#888780",
    "약한 약세": "#EF9F27", "약세": "#A32D2D",
    "대기": "#aaa"
}


def lvl_style(lvl):
    colors = ["#1D9E75", "#BA7517", "#D85A30", "#A32D2D"]
    labels = ["안전", "주의", "경계", "경고"]
    return colors[min(lvl, 3)], labels[min(lvl, 3)]


def render_regime_block(regime, title):
    horizons = [
        ("short", "단기 (1~3개월)"),
        ("mid",   "중기 (3~6개월)"),
        ("long",  "장기 (6~12개월)"),
    ]
    cards = []
    for key, label in horizons:
        r = regime.get(key, {"label": "대기", "score": 0, "reasons": []})
        color = REGIME_COLOR.get(r["label"], "#888")
        reasons_html = "<br>".join(f"· {x}" for x in r["reasons"]) if r["reasons"] else "· 데이터 수집 중"
        cards.append(f"""
        <div class="regime-card" style="border-top-color: {color};">
          <div class="regime-name">{label}</div>
          <div class="regime-value" style="color: {color};">{r['label']}</div>
          <div class="regime-score">점수 {r['score']:+d}</div>
          <div class="regime-reasons">{reasons_html}</div>
        </div>""")
    return f"""
      <div class="regime-block">
        <div class="regime-title">{title}</div>
        <div class="regime-grid">{"".join(cards)}</div>
      </div>
    """


def render_dashboard(df, signals, regime_kr, regime_us, extras=None):
    if extras is None:
        extras = {}
    df_plot = df.sort_values("date").tail(250).copy() if not df.empty else pd.DataFrame()

    def sanitize_series(series, min_val=None, max_val=None, max_daily_jump_pct=None):
        s = pd.to_numeric(series, errors="coerce").copy()
        if min_val is not None:
            s = s.where(s >= min_val)
        if max_val is not None:
            s = s.where(s <= max_val)
        if max_daily_jump_pct is not None:
            prev = s.shift(1)
            jump = ((s / prev) - 1).abs() * 100
            s = s.where((jump <= max_daily_jump_pct) | prev.isna())
        return s

    def series_connected(c, *, min_val=None, max_val=None, max_daily_jump_pct=None):
        if df_plot.empty or c not in df_plot.columns:
            return []
        s = sanitize_series(df_plot[c], min_val=min_val, max_val=max_val, max_daily_jump_pct=max_daily_jump_pct).ffill()
        return [None if pd.isna(v) else round(float(v), 2) for v in s]

    def series_nullable(c):
        if df_plot.empty or c not in df_plot.columns:
            return []
        s = pd.to_numeric(df_plot[c], errors="coerce")
        return [None if pd.isna(v) else round(float(v), 2) for v in s]

    def ma_series(c, window=200):
        if df.empty or c not in df.columns or df_plot.empty:
            return []
        full = pd.to_numeric(df[c], errors="coerce")
        ma_full = full.rolling(window).mean().tail(len(df_plot))
        return [None if pd.isna(v) else round(float(v), 2) for v in ma_full]

    def base100(c, *, max_daily_jump_pct=25):
        if df_plot.empty or c not in df_plot.columns:
            return []
        s = sanitize_series(df_plot[c], min_val=1, max_daily_jump_pct=max_daily_jump_pct).ffill()
        valid = s.dropna()
        if valid.empty:
            return []
        base = valid.iloc[0]
        if pd.isna(base) or base == 0:
            return []
        out = (s / base) * 100
        return [None if pd.isna(v) else round(float(v), 2) for v in out]

    def y_range(values_list, pad=0.04):
        vals = []
        for vs in values_list:
            vals.extend([v for v in vs if v is not None])
        if not vals:
            return None
        lo, hi = min(vals), max(vals)
        span = hi - lo
        if span == 0:
            span = max(abs(lo) * 0.05, 1)
        return [round(lo - span * pad, 2), round(hi + span * pad, 2)]

    dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else []

    kospi = series_connected("kospi", min_val=1000, max_daily_jump_pct=20)
    kosdaq = series_connected("kosdaq", min_val=300, max_daily_jump_pct=20)
    sp500 = series_connected("sp500", min_val=1000, max_daily_jump_pct=20)
    nasdaq = series_connected("nasdaq", min_val=1000, max_daily_jump_pct=20)
    cor1m = series_connected("cor1m", min_val=1, max_val=100, max_daily_jump_pct=60)

    js_data = {
        "dates": dates,
        "kospi": kospi,
        "kosdaq": kosdaq,
        "kospi_ma200": ma_series("kospi", 200),
        "kosdaq_ma200": ma_series("kosdaq", 200),
        "kospi_base100": base100("kospi"),
        "kosdaq_base100": base100("kosdaq"),
        "samsung_base100": base100("samsung"),
        "hynix_base100": base100("hynix"),
        "credit": series_connected("credit_balance_eok", min_val=100000, max_daily_jump_pct=15),
        "forced": series_nullable("forced_sale_eok"),
        "sp500": sp500,
        "sp500_ma200": ma_series("sp500", 200),
        "nasdaq": nasdaq,
        "nasdaq_ma200": ma_series("nasdaq", 200),
        "vix": series_connected("vix", min_val=5, max_val=100, max_daily_jump_pct=60),
        "nvda": series_connected("nvda", min_val=10, max_daily_jump_pct=40),
        "ust10y": series_connected("ust10y", min_val=0, max_val=10, max_daily_jump_pct=20),
        "cor1m": cor1m,
        "sec_반도체": base100("sec_반도체"),
        "sec_자동차": base100("sec_자동차"),
        "sec_조선방산": base100("sec_조선방산"),
        "sec_금융": base100("sec_금융"),
        "sec_2차전지": base100("sec_2차전지"),
        "sec_인터넷": base100("sec_인터넷"),
        "sec_바이오": base100("sec_바이오"),
        "sp500_range": y_range([sp500, ma_series("sp500", 200)]),
        "nasdaq_range": y_range([nasdaq, ma_series("nasdaq", 200)]),
        "kospi_range": y_range([kospi, ma_series("kospi", 200)]),
        "kosdaq_range": y_range([kosdaq, ma_series("kosdaq", 200)]),
    }

    # --- 추가 데이터 (extras) ---
    # 1. 미국 신용잔고 (월별)
    us_margin_debt = extras.get("us_margin_debt", {}) or {}
    if us_margin_debt:
        sorted_md = sorted(us_margin_debt.items())
        js_data["margin_dates"] = [d.strftime("%Y-%m") for d, _ in sorted_md]
        js_data["margin_vals"] = [round(v, 1) for _, v in sorted_md]
    else:
        js_data["margin_dates"] = []
        js_data["margin_vals"] = []

    # 2. M7 + AVGO + TSM Base 100 + S&P500
    m7_basket = extras.get("m7_basket", {}) or {}
    m7_dates = []
    m7_series = {}  # {ticker: [values]}
    if m7_basket:
        # 공통 날짜 인덱스 (S&P500 기준, 없으면 첫 종목)
        anchor = None
        if "SP500" in m7_basket and not m7_basket["SP500"].empty:
            anchor = m7_basket["SP500"]
        else:
            for t, s in m7_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
            anchor = anchor[anchor.index >= cutoff]
            m7_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in m7_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                # Base 100
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    m7_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["m7_dates"] = m7_dates
    js_data["m7_series"] = m7_series

    # 2-1. 글로벌 유동성 proxy + 달러-엔
    gl_proxy = extras.get("global_liquidity", pd.Series(dtype=float))
    usdjpy = extras.get("usdjpy", pd.Series(dtype=float))
    gl_dates = []
    gl_proxy_vals = []
    usdjpy_vals = []
    if (isinstance(gl_proxy, pd.Series) and not gl_proxy.empty) or (isinstance(usdjpy, pd.Series) and not usdjpy.empty):
        anchor = None
        if isinstance(gl_proxy, pd.Series) and not gl_proxy.empty:
            anchor = gl_proxy
        elif isinstance(usdjpy, pd.Series) and not usdjpy.empty:
            anchor = usdjpy
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS * 3)
            anchor = anchor[anchor.index >= cutoff]
            if not anchor.empty:
                gl_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
                if isinstance(gl_proxy, pd.Series) and not gl_proxy.empty:
                    gp = gl_proxy[gl_proxy.index >= cutoff].reindex(anchor.index).ffill().bfill()
                    gl_proxy_vals = [None if pd.isna(v) else round(float(v), 2) for v in gp]
                if isinstance(usdjpy, pd.Series) and not usdjpy.empty:
                    uj = usdjpy[usdjpy.index >= cutoff].reindex(anchor.index).ffill().bfill()
                    usdjpy_vals = [None if pd.isna(v) else round(float(v), 2) for v in uj]
    js_data["gl_dates"] = gl_dates
    js_data["global_liquidity"] = gl_proxy_vals
    js_data["usdjpy"] = usdjpy_vals

    last_date = df_plot["date"].iloc[-1].strftime("%Y년 %m월 %d일") if not df_plot.empty else "대기"
    kr_color = COLOR.get(signals["label_kr"], "#888")
    us_color = COLOR.get(signals["label_us"], "#888")

    def render_signals(sigs):
        cards = []
        for key, sig in sigs.items():
            c, l = lvl_style(sig["level"])
            cards.append(f"""
  <div class="sig" style="border-color: {c}">
    <div class="sig-name">{key}. {sig['name']}</div>
    <div class="sig-status" style="color: {c}">{l}</div>
    <div class="sig-desc">{sig['description']}</div>
  </div>""")
        return "\n".join(cards)

    kr_cards = render_signals(signals["kr"])
    us_cards = render_signals(signals["us"])
    regime_kr_html = render_regime_block(regime_kr, "코스피 추세 전망")
    regime_us_html = render_regime_block(regime_us, "S&P 500 추세 전망")

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>빚투 모니터 - 추세 전망 & 위험 신호</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 24px; color: #222; background: #fafafa; }}
  h1 {{ font-size: 26px; font-weight: 600; margin: 0 0 6px; }}
  .meta {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
  .tabs {{ display: flex; gap: 4px; margin-bottom: 20px; border-bottom: 2px solid #eee; }}
  .tab {{ padding: 12px 22px; cursor: pointer; font-weight: 500; font-size: 15px; color: #666; border-bottom: 2px solid transparent; margin-bottom: -2px; }}
  .tab.active {{ color: #222; border-bottom-color: #222; }}
  .pane {{ display: none; }}
  .pane.active {{ display: block; }}
  .overall {{ background: white; padding: 18px 22px; border-radius: 12px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; }}
  .overall-label {{ font-size: 13px; color: #888; }}
  .overall-value {{ font-size: 28px; font-weight: 600; }}
  .regime-block {{ background: white; padding: 18px 20px; border-radius: 12px; margin-bottom: 20px; }}
  .regime-title {{ font-size: 15px; font-weight: 600; margin-bottom: 14px; color: #333; }}
  .regime-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
  @media (max-width: 768px) {{ .regime-grid {{ grid-template-columns: 1fr; }} }}
  .regime-card {{ background: #fafafa; padding: 14px 16px; border-radius: 10px; border-top: 4px solid; }}
  .regime-name {{ font-size: 12px; color: #888; margin-bottom: 4px; }}
  .regime-value {{ font-size: 20px; font-weight: 600; margin-bottom: 2px; }}
  .regime-score {{ font-size: 11px; color: #aaa; margin-bottom: 8px; }}
  .regime-reasons {{ font-size: 12px; color: #555; line-height: 1.6; }}
  .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 20px; }}
  .sig {{ background: white; padding: 14px 16px; border-radius: 10px; border-top: 4px solid; }}
  .sig-name {{ font-size: 12px; color: #888; margin-bottom: 4px; }}
  .sig-status {{ font-size: 17px; font-weight: 600; margin-bottom: 6px; }}
  .sig-desc {{ font-size: 12px; color: #555; line-height: 1.4; }}
  .chart {{ background: white; padding: 14px; border-radius: 12px; margin-bottom: 14px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }}
  @media (max-width: 768px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
  .section-title {{ font-size: 16px; font-weight: 600; margin: 24px 0 10px; color: #444; }}
  .footer {{ font-size: 11px; color: #aaa; margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; }}
</style>
</head>
<body>
<h1>빚투 모니터</h1>
<div class="meta">시장 추세 전망 & 위험 신호 · 매일 평일 17:30 자동 갱신 (1일 1회) · 업데이트 {last_date}</div>

<div class="tabs">
  <div class="tab active" data-tab="kr">🇰🇷 한국장 ({signals['label_kr']})</div>
  <div class="tab" data-tab="us">🇺🇸 미국장 ({signals['label_us']})</div>
</div>

<div id="pane-kr" class="pane active">
  {regime_kr_html}
  <div class="section-title">단기 위험 신호 (일간)</div>
  <div class="overall" style="border-left: 6px solid {kr_color};">
    <div><div class="overall-label">한국 위험도 점수</div><div class="overall-value" style="color: {kr_color};">{signals['label_kr']}</div></div>
    <div style="font-size: 14px; color: #888;">{signals['score_kr']} / {signals['max_kr']}점</div>
  </div>
  <div class="signal-grid">{kr_cards}</div>
  <div class="section-title">차트</div>
  <div class="chart"><div id="c_kr_rel" style="height:320px;"></div></div>
  </div>
  <div class="chart-grid">
    <div class="chart"><div id="c_kr_kospi_abs" style="height:280px;"></div></div>
    <div class="chart"><div id="c_kr_kosdaq_abs" style="height:280px;"></div></div>
  </div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_usdkrw" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_credit_vs_krw" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_semi" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_sector" style="height:320px;"></div></div>
</div>

<div id="pane-us" class="pane">
  {regime_us_html}
  <div class="section-title">단기 위험 신호 (일간)</div>
  <div class="overall" style="border-left: 6px solid {us_color};">
    <div><div class="overall-label">미국 위험도 점수</div><div class="overall-value" style="color: {us_color};">{signals['label_us']}</div></div>
    <div style="font-size: 14px; color: #888;">{signals['score_us']} / {signals['max_us']}점</div>
  </div>
  <div class="signal-grid">{us_cards}</div>
  <div class="section-title">차트</div>
  <div class="chart-grid">
    <div class="chart"><div id="c_us_sp" style="height:300px;"></div></div>
    <div class="chart"><div id="c_us_nasdaq" style="height:300px;"></div></div>
  </div>
  <div class="chart-grid">
    <div class="chart"><div id="c_us_vix" style="height:280px;"></div></div>
    <div class="chart"><div id="c_us_nvda" style="height:280px;"></div></div>
  </div>
  <div class="chart"><div id="c_us_rate" style="height:280px;"></div></div>
  <div class="chart"><div id="c_us_cor1m" style="height:300px;"></div></div>
  <div class="chart"><div id="c_us_margin" style="height:320px;"></div></div>
  <div class="chart"><div id="c_us_m7" style="height:900px;"></div></div>
  <div class="chart"><div id="c_us_gl" style="height:380px;"></div></div>
</div>

<div class="footer">데이터: FinanceDataReader, FRED(미국 10Y), 네이버 금융/공공데이터/보조 스크래핑 · 투자 권유 아님</div>

<script>
const D = {json.dumps(js_data, ensure_ascii=False)};
const base = {{
  margin: {{t: 30, r: 45, b: 35, l: 50}}, font: {{family: 'system-ui'}},
  paper_bgcolor: 'white', plot_bgcolor: 'white', legend: {{orientation: 'h', y: -0.2}}, hovermode: 'x unified'
}};

function bindTabs() {{
  document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', e => {{
    document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
    e.currentTarget.classList.add('active');
    const pane = document.getElementById('pane-' + e.currentTarget.dataset.tab);
    if (pane) pane.classList.add('active');
    setTimeout(() => window.dispatchEvent(new Event('resize')), 120);
  }}));
}}

function hasValues(arr) {{
  return Array.isArray(arr) && arr.some(v => v !== null && v !== undefined && !Number.isNaN(v));
}}
function showEmpty(id, message='데이터 수집 실패 또는 데이터 없음') {{
  const el = document.getElementById(id);
  if (el) el.innerHTML = `<div style="height:100%;display:flex;align-items:center;justify-content:center;color:#888;font-size:13px;">${{message}}</div>`;
}}
function safePlot(id, traces, title, extra) {{
  try {{
    const valid = (traces || []).filter(t => hasValues(t.y));
    if (!D.dates.length || valid.length === 0) {{ showEmpty(id); return; }}
    Plotly.newPlot(id, valid, {{...base, title: {{text: title, font: {{size: 13}}}}, ...(extra || {{}})}});
  }} catch (err) {{
    console.error('plot error', id, err);
    showEmpty(id, '차트 렌더링 실패');
  }}
}}

bindTabs();

safePlot('c_kr_rel', [
  {{x: D.dates, y: D.kospi_base100, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#185FA5', width: 2.5}}}},
  {{x: D.dates, y: D.kosdaq_base100, type: 'scatter', mode: 'lines', name: '코스닥', connectgaps: true, line: {{color: '#534AB7', width: 2.5}}}}
], '코스피 vs 코스닥 상대 추세 (Base 100)', {{yaxis: {{title: 'Base 100'}}}});


safePlot('c_kr_kospi_abs', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#185FA5', width: 2.3}}}},
  {{x: D.dates, y: D.kospi_ma200, type: 'scatter', mode: 'lines', name: '200일선', connectgaps: true, line: {{color: '#A32D2D', width: 1.4, dash: 'dash'}}}}
], '코스피 절대 추세', D.kospi_range ? {{yaxis: {{range: D.kospi_range}}}} : undefined);

safePlot('c_kr_kosdaq_abs', [
  {{x: D.dates, y: D.kosdaq, type: 'scatter', mode: 'lines', name: '코스닥', connectgaps: true, line: {{color: '#534AB7', width: 2.3}}}},
  {{x: D.dates, y: D.kosdaq_ma200, type: 'scatter', mode: 'lines', name: '200일선', connectgaps: true, line: {{color: '#A32D2D', width: 1.4, dash: 'dash'}}}}
], '코스닥 절대 추세', D.kosdaq_range ? {{yaxis: {{range: D.kosdaq_range}}}} : undefined);

safePlot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', connectgaps: false, line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
], '코스피 vs 신용잔고', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '잔고(억)', overlaying: 'y', side: 'right'}}}});

safePlot('c_kr_forced', [{{x: D.dates, y: D.forced, type: 'bar', name: '반대매매(억)', marker: {{color: (D.forced || []).map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}}}], '일일 반대매매');

safePlot('c_kr_usdkrw', [
  {{x: D.dates, y: D.usdkrw, type: 'scatter', mode: 'lines', name: '원/달러', connectgaps: true,
    line: {{color: '#C0392B', width: 2}}}}
], '원달러 환율 (KRW/USD)', {{
  yaxis: {{title: '원/달러', tickformat: ',.0f'}},
  shapes: [
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 1300, y1: 1300, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}},
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 1400, y1: 1400, line: {{color: '#A32D2D', width: 1, dash: 'dash'}}}}
  ]
}});

safePlot('c_kr_semi', [
  {{x: D.dates, y: D.kospi_base100, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#888', width: 1.2, dash: 'dot'}}}},
  {{x: D.dates, y: D.samsung_base100, type: 'scatter', mode: 'lines', name: '삼성전자', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.hynix_base100, type: 'scatter', mode: 'lines', name: 'SK하이닉스', connectgaps: true, line: {{color: '#534AB7', width: 2.2}}}}
], '반도체 누적 추세 (Base 100)', {{yaxis: {{title: 'Base 100'}}}});

safePlot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', mode: 'lines', name: '반도체', connectgaps: false, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.sec_자동차, type: 'scatter', mode: 'lines', name: '자동차', connectgaps: false, line: {{color: '#7D56F4', width: 2.0}}}},
  {{x: D.dates, y: D.sec_조선방산, type: 'scatter', mode: 'lines', name: '조선방산', connectgaps: false, line: {{color: '#534AB7', width: 2.0}}}},
  {{x: D.dates, y: D.sec_금융, type: 'scatter', mode: 'lines', name: '금융', connectgaps: false, line: {{color: '#888', width: 1.8}}}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', mode: 'lines', name: '2차전지', connectgaps: false, line: {{color: '#BA7517', width: 1.8}}}},
  {{x: D.dates, y: D.sec_인터넷, type: 'scatter', mode: 'lines', name: '인터넷', connectgaps: false, line: {{color: '#1D9E75', width: 1.8}}}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', mode: 'lines', name: '바이오', connectgaps: false, line: {{color: '#2AA198', width: 1.8}}}}
], '업종 바구니 누적 추세 (Base 100)', {{yaxis: {{title: 'Base 100'}}}});

safePlot('c_us_sp', [
  {{x: D.dates, y: D.sp500, type: 'scatter', mode: 'lines', name: 'S&P 500', connectgaps: true, line: {{color: '#185FA5', width: 2.3}}}},
  {{x: D.dates, y: D.sp500_ma200, type: 'scatter', mode: 'lines', name: '200일선', connectgaps: true, line: {{color: '#A32D2D', width: 1.4, dash: 'dash'}}}}
], 'S&P 500 + 200일선', D.sp500_range ? {{yaxis: {{range: D.sp500_range}}}} : undefined);

safePlot('c_us_nasdaq', [
  {{x: D.dates, y: D.nasdaq, type: 'scatter', mode: 'lines', name: '나스닥', connectgaps: true, line: {{color: '#534AB7', width: 2.3}}}},
  {{x: D.dates, y: D.nasdaq_ma200, type: 'scatter', mode: 'lines', name: '200일선', connectgaps: true, line: {{color: '#A32D2D', width: 1.4, dash: 'dash'}}}}
], '나스닥 + 200일선', D.nasdaq_range ? {{yaxis: {{range: D.nasdaq_range}}}} : undefined);

safePlot('c_us_vix', [{{x: D.dates, y: D.vix, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VIX', connectgaps: false, line: {{color: '#A32D2D', width: 2.2}}, fillcolor: 'rgba(163,45,45,0.10)'}}], 'VIX 공포지수');

safePlot('c_us_nvda', [{{x: D.dates, y: D.nvda, type: 'scatter', mode: 'lines', name: 'NVDA', connectgaps: false, line: {{color: '#1D9E75', width: 2.2}}}}], '엔비디아');

safePlot('c_us_rate', [{{x: D.dates, y: D.ust10y, type: 'scatter', mode: 'lines', name: '10Y', connectgaps: false, line: {{color: '#BA7517', width: 2.2}}}}], '미국 10년물 국채금리 (%)');

safePlot('c_us_cor1m', [{{x: D.dates, y: D.cor1m, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'COR1M', connectgaps: false, line: {{color: '#534AB7', width: 2.2}}, fillcolor: 'rgba(83,74,183,0.08)'}}], 'CBOE 1개월 내재상관 (COR1M)', {{yaxis: {{title: 'COR1M', range: [0, 100]}}, shapes: [
  {{type: 'rect', xref: 'paper', x0: 0, x1: 1, y0: 60, y1: 100, fillcolor: 'rgba(163,45,45,0.08)', line: {{width: 0}}}},
  {{type: 'rect', xref: 'paper', x0: 0, x1: 1, y0: 0, y1: 15, fillcolor: 'rgba(186,117,23,0.10)', line: {{width: 0}}}},
  {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 60, y1: 60, line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}},
  {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 20, y1: 20, line: {{color: '#1D9E75', width: 1, dash: 'dot'}}}},
  {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 15, y1: 15, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}}
]}});

// === 미국 신용잔고 (월별, FINRA) ===
(function() {{
  const id = 'c_us_margin';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.margin_dates || D.margin_dates.length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    Plotly.newPlot(id, [{{
      x: D.margin_dates, y: D.margin_vals, type: 'bar', name: '신용잔고 (십억$)',
      marker: {{color: '#A32D2D', opacity: 0.85}}
    }}], Object.assign({{}}, base, {{
      title: {{text: '미국 신용잔고 (FINRA Margin Debt, 월별)', font: {{size: 14}}}},
      yaxis: {{title: '십억 USD'}},
      xaxis: {{title: ''}}
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('margin plot:', e); showEmpty(id); }}
}})();

// === M7 + AVGO + TSM + IXIC vs S&P500 (Base 100) ===
(function() {{
  const id = 'c_us_m7';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.m7_dates || D.m7_dates.length === 0 || !D.m7_series || Object.keys(D.m7_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    // 서로 잘 구분되는 11개 고대비 팔레트 (겹침 최소화)
    const colorMap = {{
      'SP500': '#6B7280',  // 회색 (벤치마크)
      'IXIC':  '#1E3A8A',  // 진청색 (벤치마크)
      'AAPL':  '#111827',  // 블랙
      'MSFT':  '#0EA5E9',  // 하늘
      'GOOGL': '#10B981',  // 에메랄드
      'AMZN':  '#F59E0B',  // 앰버
      'META':  '#7C3AED',  // 바이올렛
      'NVDA':  '#84CC16',  // 라임
      'TSLA':  '#DC2626',  // 빨강
      'AVGO':  '#EA580C',  // 오렌지
      'TSM':   '#DB2777'   // 핑크
    }};
    const labelMap = {{
      'SP500': 'S&P500',
      'IXIC':  '나스닥 (IXIC)',
      'AAPL':  '애플 AAPL',
      'MSFT':  'MS MSFT',
      'GOOGL': '구글 GOOGL',
      'AMZN':  '아마존 AMZN',
      'META':  '메타 META',
      'NVDA':  '엔비디아 NVDA',
      'TSLA':  '테슬라 TSLA',
      'AVGO':  '브로드컴 AVGO',
      'TSM':   'TSMC TSM'
    }};
    const order = ['SP500', 'IXIC', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'AVGO', 'TSM'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.m7_series[t];
      if (!vals || !hasValues(vals)) return;
      const isSP = t === 'SP500';
      const isNas = t === 'IXIC';
      traces.push({{
        x: D.m7_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,  // 내부 참조용
        line: {{
          color: colorMap[t] || '#666',
          width: (isSP || isNas) ? 2.6 : 2.4,
          dash: isSP ? 'dot' : (isNas ? 'dash' : 'solid'),
          shape: 'spline',
          smoothing: 1.0
        }},
        hoverlabel: {{font: {{size: 13}}}}
      }});
    }});
    if (traces.length === 0) {{ showEmpty(id); return; }}

    // 랭킹 계산 - 최신 값 기준 내림차순
    const ranking = traces
      .map(t => {{
        const lastVal = [...t.y].reverse().find(v => v !== null && v !== undefined);
        return {{key: t._key, name: t.name, last: lastVal}};
      }})
      .filter(x => x.last !== undefined && x.last !== null)
      .sort((a, b) => b.last - a.last);

    // 랭킹 annotations - 박스 없이 텍스트만, 각 줄 색 = 라인 색
    const lineHeight = 0.038;
    const boxTop = 0.985;
    const boxLeft = 0.012;
    const rankAnnotations = [];
    ranking.forEach((x, i) => {{
      const color = colorMap[x.key] || '#333';
      rankAnnotations.push({{
        xref: 'paper', yref: 'paper',
        x: boxLeft, y: boxTop - (i * lineHeight),
        xanchor: 'left', yanchor: 'top',
        align: 'left', showarrow: false,
        text: '<b>' + (i + 1) + '위</b> ' + x.name + '<b> ' + x.last.toFixed(1) + '</b>',
        font: {{size: 15, color: color, family: 'system-ui'}}
      }});
    }});

    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: 'M7 + 브로드컴 + TSMC + 나스닥 vs S&P500 누적 추세 (Base 100)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('m7 plot:', e); showEmpty(id); }}
}})();

// === 글로벌 유동성 Proxy vs 달러-엔 ===
(function() {{
  const id = 'c_us_gl';
  const el = document.getElementById(id);
  if (!el) return;
  const hasGL = D.gl_dates && D.gl_dates.length > 0 && hasValues(D.global_liquidity);
  const hasUJ = D.gl_dates && D.gl_dates.length > 0 && hasValues(D.usdjpy);
  if (!hasGL && !hasUJ) {{ showEmpty(id); return; }}
  try {{
    const traces = [];
    if (hasGL) traces.push({{x:D.gl_dates,y:D.global_liquidity,type:'scatter',mode:'lines',name:'글로벌 유동성 proxy',connectgaps:true,line:{{color:'#185FA5',width:2.8}}}});
    if (hasUJ) traces.push({{x:D.gl_dates,y:D.usdjpy,type:'scatter',mode:'lines',name:'달러-엔',yaxis:'y2',connectgaps:true,line:{{color:'#9ca3af',width:2.1}}}});
    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title:{{text:'글로벌 유동성 Proxy vs 달러-엔', font:{{size:14}}}},
      yaxis:{{title:'Proxy (Base 100)'}},
      yaxis2:{{title:'엔', overlaying:'y', side:'right'}}
    }}), {{displayModeBar:false, responsive:true}});
  }} catch (e) {{ console.error('gl plot:', e); showEmpty(id); }}
}})();

// === 신용잔고 vs 원달러 환율 (이중축) ===
(function() {{
  const id = 'c_kr_credit_vs_krw';
  const el = document.getElementById(id);
  if (!el) return;
  const hasCredit = hasValues(D.credit);
  const hasKrw = hasValues(D.usdkrw);
  if (!hasCredit && !hasKrw) {{ showEmpty(id); return; }}
  try {{
    const traces = [];
    if (hasCredit) {{
      traces.push({{
        x: D.dates, y: D.credit, type: 'scatter', mode: 'lines',
        name: '신용잔고(억)', connectgaps: true,
        line: {{color: '#185FA5', width: 2}}, fill: 'tozeroy',
        fillcolor: 'rgba(24,95,165,0.10)'
      }});
    }}
    if (hasKrw) {{
      traces.push({{
        x: D.dates, y: D.usdkrw, type: 'scatter', mode: 'lines',
        name: '원/달러', yaxis: 'y2', connectgaps: true,
        line: {{color: '#C0392B', width: 1.8, dash: 'dot'}}
      }});
    }}
    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: '신용잔고 vs 원달러 (이중축)', font: {{size: 14}}}},
      yaxis: {{title: '신용잔고(억)', tickformat: ',.0f'}},
      yaxis2: {{title: '원/달러', overlaying: 'y', side: 'right', tickformat: ',.0f'}},
      legend: {{orientation: 'h', y: -0.18}}
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('credit_krw plot:', e); showEmpty(id); }}
}})();
</script>
</body>
</html>
"""
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")
    print("  dashboard written")


def main():
    try:
        df, extras = update_data()
    except Exception as e:
        print(f"[error] update_data fatal: {e}")
        import traceback; traceback.print_exc()
        df = load_history()
        extras = {"us_margin_debt": {}, "m7_basket": {}, "kr_foreign_holding": {}, "global_liquidity": pd.Series(dtype=float), "usdjpy": pd.Series(dtype=float)}

    signals = compute_signals(df, extras)
    regime_kr = compute_regime(df["kospi"]) if not df.empty else {}
    regime_us = compute_regime(df["sp500"]) if not df.empty else {}

    print("=== Signals ===")
    print(json.dumps(signals, indent=2, ensure_ascii=False, default=str))
    print("=== Regime KR ===")
    print(json.dumps(regime_kr, indent=2, ensure_ascii=False, default=str))
    print("=== Regime US ===")
    print(json.dumps(regime_us, indent=2, ensure_ascii=False, default=str))

    try:
        render_dashboard(df, signals, regime_kr, regime_us, extras)
    except Exception as e:
        print(f"[error] render_dashboard: {e}")
        import traceback; traceback.print_exc()

    print("Done.")


if __name__ == "__main__":
    main()
