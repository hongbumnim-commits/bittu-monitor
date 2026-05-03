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
    "전기":     ["298040", "010120", "267260", "062040"],
}



# 공공데이터포털 서비스키 (data.go.kr) - GitHub Secret: DATA_GO_KR_API_KEY
DATA_GO_KR_KEY = os.environ.get("DATA_GO_KR_API_KEY", "")
KOFIA_BASE = "https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService"

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


def fetch_fed_debt(days=LOOKBACK_DAYS * 10):
    """
    FRED: Federal Debt Held by Federal Reserve Banks (FDHBFRBN).
    분기별 데이터, 단위: 십억 달러 (Billions of Dollars).
    """
    s = fetch_fred_series("FDHBFRBN", "fed_debt", days=days)
    if not s.empty:
        print(f"  fed_debt (FDHBFRBN): {len(s)} rows, latest {float(s.iloc[-1]):.1f}B")
    else:
        print("  [warn] fed_debt: empty")
    return s



def fetch_krwusd(days=LOOKBACK_DAYS):
    """
    원/달러 환율 (Korean Won per 1 USD). FRED DEXKOUS.
    값이 클수록 원화 약세. 코스피와 역상관 경향.
    """
    s = fetch_fred_series("DEXKOUS", "krwusd", days=days)
    if not s.empty:
        print(f"  krwusd: {len(s)} rows, latest {float(s.iloc[-1]):.1f}")
    else:
        print("  [warn] krwusd: empty")
    return s


def fetch_cnn_fear_greed(days=LOOKBACK_DAYS):
    """
    CNN Fear & Greed Index 히스토리컬 데이터.
    https://production.dataviz.cnn.io/index/fearandgreed/graphdata
    Returns: pd.Series {date: score (0-100)}
    """
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    cutoff = TODAY - dt.timedelta(days=days)
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://edition.cnn.com/markets/fear-and-greed",
                "Accept": "application/json, text/plain, */*",
            },
            timeout=20,
        )
        if r.status_code != 200:
            print(f"  [warn] cnn fg: status {r.status_code}")
            return pd.Series(dtype=float, name="cnn_fg")
        data = r.json()
        hist = data.get("fear_and_greed_historical", {}).get("data", [])
        if not hist:
            print("  [warn] cnn fg: no historical data in response")
            return pd.Series(dtype=float, name="cnn_fg")
        result = {}
        for item in hist:
            try:
                ts_ms = float(item["x"])
                score = float(item["y"])
                d = dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).date()
                if d >= cutoff:
                    result[d] = round(score, 1)
            except Exception:
                continue
        if result:
            s = pd.Series(result, name="cnn_fg").sort_index()
            print(f"  cnn_fg: {len(s)} rows, latest {float(s.iloc[-1]):.1f}")
            return s
    except Exception as e:
        print(f"  [warn] cnn_fg: {e}")
    return pd.Series(dtype=float, name="cnn_fg")


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

KR_POWER_STOCKS = {
    "KOSPI": ("KS11", "코스피"),
    "KOSDAQ": ("KQ11", "코스닥"),
    "LS_ELECTRIC": ("010120", "LS ELECTRIC"),
    "HD_HE": ("267260", "HD현대일렉트릭"),
    "HYOSUNG_HI": ("298040", "효성중공업"),
    "SANIL": ("062040", "산일전기"),
}


def fetch_kr_power_basket():
    """코스피/코스닥 + 국내 전력기기 대표주 1년 누적 추세용 바스켓."""
    result = {}
    for ticker, (source_symbol, _) in KR_POWER_STOCKS.items():
        s = safe(
            f"kr_power_{ticker}",
            lambda ts=source_symbol, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
            default=pd.Series(dtype=float),
        )
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  kr power basket: {len(result)} tickers")
    return result


KR_SHIP_STOCKS = {
    "KOSPI":       ("KS11",   "코스피"),
    "KOSDAQ":      ("KQ11",   "코스닥"),
    "HD_KSH":      ("009540", "HD한국조선해양"),
    "HANWHA_OC":   ("042660", "한화오션"),
    "SAMSUNG_HI":  ("010140", "삼성중공업"),
    "HD_HHI":      ("329180", "HD현대중공업"),
}


def fetch_kr_ship_basket():
    """코스피/코스닥 + 국내 핵심 조선주 1년 누적 추세용 바스켓."""
    result = {}
    for ticker, (source_symbol, _) in KR_SHIP_STOCKS.items():
        s = safe(
            f"kr_ship_{ticker}",
            lambda ts=source_symbol, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
            default=pd.Series(dtype=float),
        )
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  kr ship basket: {len(result)} tickers")
    return result


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


US_INDICES_STOCKS = {
    "SP500": "S&P500",
    "IXIC":  "나스닥",
    "RUT":   "러셀2000",
    "SOX":   "필라델피아 반도체",
}


def fetch_us_indices_basket():
    """S&P500 + 나스닥 + 러셀2000 + 필라델피아반도체 Base 100 바스켓."""
    SYMBOL_MAP = {"SP500": "US500", "IXIC": "IXIC", "RUT": "IWM", "SOX": "SOXX"}
    result = {}
    for ticker, _ in US_INDICES_STOCKS.items():
        source = SYMBOL_MAP.get(ticker, ticker)
        s = safe(f"us_idx_{ticker}", lambda ts=source, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
                 default=pd.Series(dtype=float))
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  us indices basket: {len(result)} tickers")
    return result


STORAGE_STOCKS = {
    "SP500": "S&P500",
    "IXIC":  "나스닥",
    "STX":   "씨게이트 STX",
    "WDC":   "WDC",
}


def fetch_storage_basket():
    """씨게이트·WDC + S&P500 + 나스닥 Base 100 바스켓."""
    SYMBOL_MAP = {"SP500": "US500", "IXIC": "IXIC", "STX": "STX", "WDC": "WDC"}
    result = {}
    for ticker, _ in STORAGE_STOCKS.items():
        source = SYMBOL_MAP.get(ticker, ticker)
        s = safe(f"storage_{ticker}", lambda ts=source, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
                 default=pd.Series(dtype=float))
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  storage basket: {len(result)} tickers")
    return result


# ================================================================
# 한국 외국인 주식 보유금액 & 비중 - index.go.kr (월별)
# ================================================================

# ================================================================
# data.go.kr 금융투자협회 API - 신용공여잔고추이
# ================================================================
def fetch_credit_balance():
    """
    일자별 신용공여잔고 (신용거래융자 전체).
    getGrantingOfCreditBalanceInfo → crdTrFingWhl (백만원 → 억원)
    Returns: {date: eok_value}
    """
    if not DATA_GO_KR_KEY:
        print("  [warn] fetch_credit_balance: DATA_GO_KR_API_KEY 환경변수 없음")
        return {}

    result = {}
    start_dt = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_dt   = TODAY.strftime("%Y%m%d")
    page, per_page = 1, 100

    while True:
        params = {
            "serviceKey": DATA_GO_KR_KEY,
            "pageNo": page,
            "numOfRows": per_page,
            "resultType": "json",
            "beginBasDt": start_dt,
            "endBasDt":   end_dt,
        }
        try:
            r = requests.get(f"{KOFIA_BASE}/getGrantingOfCreditBalanceInfo",
                             params=params, timeout=20)
            r.raise_for_status()
            body  = r.json().get("response", {}).get("body", {})
            items = body.get("items", {})
            if not items:
                break
            rows = items.get("item", [])
            if isinstance(rows, dict):
                rows = [rows]
            for row in rows:
                ds  = str(row.get("basDt", "")).strip()
                raw = str(row.get("crdTrFingWhl", "")).replace(",", "").strip()
                if len(ds) == 8 and raw not in ("", "nan", "-"):
                    try:
                        d   = dt.date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
                        result[d] = round(float(raw) / 100, 0)   # 백만원 → 억원
                    except Exception:
                        continue
            total = int(body.get("totalCount", 0))
            if page * per_page >= total:
                break
            page += 1
        except Exception as e:
            print(f"  [warn] fetch_credit_balance page {page}: {e}")
            break

    if result:
        latest = max(result)
        print(f"  credit balance (data.go.kr KOFIA): {len(result)} rows, latest {latest}: {result[latest]:.0f}억")
    else:
        print("  [warn] fetch_credit_balance: 0 rows")
    return result


def fetch_foreign_holding_kr():
    return {}

MAIN_COLS = [
    "date",
    "kospi", "kosdaq", "samsung", "hynix", "mu", "sksquare",
    "credit_balance_eok",
    "samsung_ret_pct", "hynix_ret_pct",
    "sp500", "nasdaq", "vix", "nvda", "ust10y", "cor1m",
    "sec_반도체", "sec_방산조선", "sec_바이오", "sec_2차전지", "sec_금융", "sec_전기",
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
    mu = safe("mu", lambda: fetch_fdr("MU", "mu"), default=pd.Series(dtype=float, name="mu"))
    sksquare = safe("sksquare", lambda: fetch_fdr("402340", "sksquare"), default=pd.Series(dtype=float, name="sksquare"))

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
    credit_map  = safe("credit",        fetch_credit_balance,   default={})

    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix, "mu": mu, "sksquare": sksquare,
        "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y,
        "cor1m": cor1m,
        **sectors
    }
    df = pd.DataFrame(series_dict)
    df = df.reset_index().rename(columns={"index": "date"})
    df["samsung_ret_pct"] = df["samsung"].pct_change(fill_method=None) * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change(fill_method=None) * 100
    df["credit_balance_eok"] = None

    # 신용공여잔고
    for d, v in credit_map.items():
        if d in df["date"].values:
            df.loc[df["date"] == d, "credit_balance_eok"] = v

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
    kr_power_basket = safe("kr_power_basket", fetch_kr_power_basket, default={})
    kr_ship_basket = safe("kr_ship_basket", fetch_kr_ship_basket, default={})
    m7_basket = safe("m7_plus", fetch_m7_plus_basket, default={})
    us_indices_basket = safe("us_indices", fetch_us_indices_basket, default={})
    storage_basket = safe("storage", fetch_storage_basket, default={})
    fed_debt  = safe("fed_debt", fetch_fed_debt,       default=pd.Series(dtype=float, name="fed_debt"))
    krwusd    = safe("krwusd",   fetch_krwusd,          default=pd.Series(dtype=float, name="krwusd"))
    cnn_fg    = safe("cnn_fg",   fetch_cnn_fear_greed,  default=pd.Series(dtype=float, name="cnn_fg"))

    extras = {
        "us_margin_debt":    us_margin_debt,
        "kr_power_basket":   kr_power_basket,
        "kr_ship_basket":    kr_ship_basket,
        "m7_basket":         m7_basket,
        "us_indices_basket": us_indices_basket,
        "storage_basket":    storage_basket,
        "fed_debt":          fed_debt,   # Series (분기, 십억달러)
        "krwusd":            krwusd,     # Series (일별, 원/달러)
        "cnn_fg":            cnn_fg,     # Series (일별, 0-100)
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
    effective_window = min(window, len(s))
    if effective_window < 10:
        return None
    ma = s.rolling(effective_window).mean().iloc[-1]
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
    if len(s) < 30:
        return result

    cur = s.iloc[-1]
    ma50  = s.rolling(min(50,  len(s))).mean().iloc[-1]
    ma200 = s.rolling(min(200, len(s))).mean().iloc[-1]
    ma50_20prev  = s.rolling(min(50, len(s))).mean().iloc[-21] if len(s) > 70  else ma50
    ma200_30prev = s.rolling(min(200,len(s))).mean().iloc[-31] if len(s) > 230 else ma200
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

    kospi    = series_connected("kospi",      min_val=1000, max_daily_jump_pct=20)
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
        "mu_base100": base100("mu"),
        "sksquare_base100": base100("sksquare"),
        "credit": series_connected("credit_balance_eok", min_val=100000, max_daily_jump_pct=15),
        "sp500": sp500,
        "sp500_ma200": ma_series("sp500", 200),
        "nasdaq": nasdaq,
        "nasdaq_ma200": ma_series("nasdaq", 200),
        "vix": series_connected("vix", min_val=5, max_val=100, max_daily_jump_pct=60),
        "nvda": series_connected("nvda", min_val=10, max_daily_jump_pct=40),
        "ust10y": series_connected("ust10y", min_val=0, max_val=10, max_daily_jump_pct=20),
        "cor1m": cor1m,
        "sec_반도체": base100("sec_반도체"),
        "sec_방산조선": base100("sec_방산조선"),
        "sec_금융": base100("sec_금융"),
        "sec_2차전지": base100("sec_2차전지"),
        "sec_바이오": base100("sec_바이오"),
        "sec_전기": base100("sec_전기"),
        "sp500_range": y_range([sp500, ma_series("sp500", 200)]),
        "nasdaq_range": y_range([nasdaq, ma_series("nasdaq", 200)]),
        "kospi_range": y_range([kospi, ma_series("kospi", 200)]),
        "kosdaq_range": y_range([kosdaq, ma_series("kosdaq", 200)]),
    }

    # --- 추가 데이터 (extras) ---
    # 0-a. 섹터별 단기 모멘텀 (1주/1개월/3개월 수익률) — df에서 직접 계산
    SECTOR_LABELS = {
        "sec_반도체": "반도체", "sec_방산조선": "방산·조선",
        "sec_바이오": "바이오", "sec_2차전지": "2차전지",
        "sec_금융":   "금융",   "sec_전기":    "전력기기",
    }
    PERIODS = {"1주": 5, "1개월": 21, "3개월": 63}
    sector_mom = {}  # {sector_label: {period: return_pct}}
    for col, label in SECTOR_LABELS.items():
        s = pd.to_numeric(df[col], errors="coerce").dropna() if col in df.columns else pd.Series(dtype=float)
        if s.empty:
            continue
        rets = {}
        for pname, ndays in PERIODS.items():
            if len(s) > ndays:
                rets[pname] = round((float(s.iloc[-1]) / float(s.iloc[-(ndays+1)]) - 1) * 100, 2)
        if rets:
            sector_mom[label] = rets
    js_data["sector_mom"] = sector_mom   # {label: {period: pct}}

    # 0-b. 원/달러 환율 (FRED DEXKOUS)
    krwusd_ser = extras.get("krwusd", pd.Series(dtype=float))
    if isinstance(krwusd_ser, pd.Series) and not krwusd_ser.empty:
        s = pd.to_numeric(krwusd_ser, errors="coerce").dropna().sort_index()
        cutoff_kr = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
        s = s[s.index >= cutoff_kr]
        js_data["krwusd_dates"] = [d.strftime("%Y-%m-%d") for d in s.index]
        js_data["krwusd_vals"]  = [round(float(v), 2) for v in s]
    else:
        js_data["krwusd_dates"] = []
        js_data["krwusd_vals"]  = []

    # 0-c. CNN Fear & Greed
    cnn_fg = extras.get("cnn_fg", pd.Series(dtype=float))
    if isinstance(cnn_fg, pd.Series) and not cnn_fg.empty:
        s = pd.to_numeric(cnn_fg, errors="coerce").dropna().sort_index()
        js_data["cnn_fg_dates"] = [d.strftime("%Y-%m-%d") for d in s.index]
        js_data["cnn_fg_vals"]  = [round(float(v), 1) for v in s]
    else:
        js_data["cnn_fg_dates"] = []
        js_data["cnn_fg_vals"]  = []
    us_margin_debt = extras.get("us_margin_debt", {}) or {}
    if us_margin_debt:
        sorted_md = sorted(us_margin_debt.items())
        js_data["margin_dates"] = [d.strftime("%Y-%m") for d, _ in sorted_md]
        js_data["margin_vals"] = [round(v, 1) for _, v in sorted_md]
    else:
        js_data["margin_dates"] = []
        js_data["margin_vals"] = []

    # 2. 한국 전력기기 대표주 Base 100 + 코스피/코스닥 (최근 3개월)
    kr_power_basket = extras.get("kr_power_basket", {}) or {}
    kr_power_dates = []
    kr_power_series = {}
    if kr_power_basket:
        anchor = None
        if "KOSPI" in kr_power_basket and not kr_power_basket["KOSPI"].empty:
            anchor = kr_power_basket["KOSPI"]
        else:
            for _, s in kr_power_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=90)
            anchor = anchor[anchor.index >= cutoff]
            kr_power_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in kr_power_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    kr_power_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["kr_power_dates"] = kr_power_dates
    js_data["kr_power_series"] = kr_power_series

    # 3. M7 + AVGO + TSM Base 100 + S&P500 (최근 3개월)
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
            cutoff = TODAY - dt.timedelta(days=90)
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

    # 3-2. 한국 조선주 바스켓 Base 100 (최근 3개월)
    kr_ship_basket = extras.get("kr_ship_basket", {}) or {}
    kr_ship_dates = []
    kr_ship_series = {}
    if kr_ship_basket:
        anchor = None
        if "KOSPI" in kr_ship_basket and not kr_ship_basket["KOSPI"].empty:
            anchor = kr_ship_basket["KOSPI"]
        else:
            for _, s in kr_ship_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=90)
            anchor = anchor[anchor.index >= cutoff]
            kr_ship_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in kr_ship_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    kr_ship_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["kr_ship_dates"] = kr_ship_dates
    js_data["kr_ship_series"] = kr_ship_series

    # 3-3. 미국 주요 지수 Base 100 (최근 3개월)
    us_indices_basket = extras.get("us_indices_basket", {}) or {}
    us_idx_dates = []
    us_idx_series = {}
    if us_indices_basket:
        anchor = None
        if "SP500" in us_indices_basket and not us_indices_basket["SP500"].empty:
            anchor = us_indices_basket["SP500"]
        else:
            for _, s in us_indices_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=90)
            anchor = anchor[anchor.index >= cutoff]
            us_idx_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in us_indices_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    us_idx_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["us_idx_dates"] = us_idx_dates
    js_data["us_idx_series"] = us_idx_series

    # 3-4. 스토리지 종목 Base 100 (최근 3개월)
    storage_basket = extras.get("storage_basket", {}) or {}
    storage_dates = []
    storage_series = {}
    if storage_basket:
        anchor = None
        if "SP500" in storage_basket and not storage_basket["SP500"].empty:
            anchor = storage_basket["SP500"]
        else:
            for _, s in storage_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=90)
            anchor = anchor[anchor.index >= cutoff]
            storage_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in storage_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    storage_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["storage_dates"] = storage_dates
    js_data["storage_series"] = storage_series

    # 3-1. 연준 보유 국채 (FDHBFRBN, 분기별, 십억달러)
    fed_debt = extras.get("fed_debt", pd.Series(dtype=float))
    fed_debt_dates = []
    fed_debt_vals = []
    if isinstance(fed_debt, pd.Series) and not fed_debt.empty:
        s = pd.to_numeric(fed_debt, errors="coerce").dropna().sort_index()
        fed_debt_dates = [d.strftime("%Y-%m-%d") for d in s.index]
        fed_debt_vals = [round(float(v), 1) for v in s]
    js_data["fed_debt_dates"] = fed_debt_dates
    js_data["fed_debt_vals"] = fed_debt_vals



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
  <div class="chart"><div id="c_kr_sector_mom" style="height:360px;"></div></div>
  <div class="chart"><div id="c_kr_krwusd" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_power" style="height:680px;"></div></div>
  <div class="chart"><div id="c_kr_ship" style="height:680px;"></div></div>
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
  <div class="chart"><div id="c_us_storage" style="height:680px;"></div></div>
  <div class="chart"><div id="c_us_indices" style="height:480px;"></div></div>
  <div class="chart-grid">
    <div class="chart"><div id="c_us_vix" style="height:280px;"></div></div>
  </div>
  <div class="chart"><div id="c_us_rate" style="height:280px;"></div></div>
  <div class="chart"><div id="c_us_cor1m" style="height:300px;"></div></div>
  <div class="chart"><div id="c_us_margin" style="height:320px;"></div></div>
  <div class="chart"><div id="c_us_m7" style="height:900px;"></div></div>
  <div class="chart"><div id="c_us_cnn_fg" style="height:320px;"></div></div>
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


// === 섹터별 단기 모멘텀 (1주/1개월/3개월 수익률) ===
(function() {{
  const id = 'c_kr_sector_mom';
  const el = document.getElementById(id);
  if (!el) return;
  const mom = D.sector_mom || {{}};
  const labels = Object.keys(mom);
  if (labels.length === 0) {{ showEmpty(id); return; }}
  try {{
    const periods = ['1주', '1개월', '3개월'];
    const periodColors = ['#185FA5', '#1D9E75', '#D85A30'];
    // 1개월 수익률 기준으로 섹터 정렬
    const sorted = labels.slice().sort((a, b) => {{
      const va = (mom[a] && mom[a]['1개월'] !== undefined) ? mom[a]['1개월'] : 0;
      const vb = (mom[b] && mom[b]['1개월'] !== undefined) ? mom[b]['1개월'] : 0;
      return va - vb;  // ascending (가장 큰 게 오른쪽/위)
    }});
    const traces = periods.map((p, pi) => ({{
      y: sorted,
      x: sorted.map(s => (mom[s] && mom[s][p] !== undefined) ? mom[s][p] : null),
      type: 'bar', orientation: 'h', name: p,
      marker: {{color: periodColors[pi], opacity: 0.82}},
      hovertemplate: `%{{y}}<br>${{p}} %{{x:+.2f}}%<extra></extra>`
    }}));
    // 0% 기준선 + 색상 오버레이를 위한 막대 색 커스텀 (1개월 기준)
    const oneM = traces[1];
    oneM.marker = {{
      color: sorted.map(s => {{
        const v = mom[s] && mom[s]['1개월'] !== undefined ? mom[s]['1개월'] : 0;
        return v >= 0 ? 'rgba(29,158,117,0.85)' : 'rgba(163,45,45,0.85)';
      }}),
      opacity: 0.85
    }};
    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: '한국 섹터별 모멘텀 · 파랑=1주 · 초록=1개월 · 주황=3개월', font: {{size: 13}}}},
      barmode: 'group',
      xaxis: {{ticksuffix: '%', gridcolor: '#F3F4F6', zeroline: true, zerolinecolor: '#bbb', zerolinewidth: 1.5}},
      yaxis: {{gridcolor: '#F3F4F6', automargin: true}},
      legend: {{orientation: 'h', y: 1.08, x: 1.0, xanchor: 'right', yanchor: 'bottom', font: {{size: 12}}}},
      margin: {{t: 50, r: 30, b: 30, l: 85}}
    }}), {{displayModeBar: false, responsive: true}});
  }} catch(e) {{ console.error('sector mom plot:', e); showEmpty(id); }}
}})();

// === 원/달러 환율 vs 코스피 ===
(function() {{
  const id = 'c_kr_krwusd';
  const el = document.getElementById(id);
  if (!el) return;
  const hasKRW  = D.krwusd_dates && D.krwusd_dates.length > 0 && hasValues(D.krwusd_vals);
  const hasKSPI = D.dates && D.dates.length > 0 && hasValues(D.kospi);
  if (!hasKRW) {{ showEmpty(id, '원/달러 환율 데이터 없음 (FRED)'); return; }}
  try {{
    const traces = [];
    if (hasKRW) traces.push({{
      x: D.krwusd_dates, y: D.krwusd_vals,
      type: 'scatter', mode: 'lines', name: '원/달러 (↑원화약세)',
      connectgaps: true,
      line: {{color: '#D85A30', width: 2.2}},
      hovertemplate: '%{{x}}<br>%{{y:.0f}}원<extra></extra>'
    }});
    if (hasKSPI) traces.push({{
      x: D.dates, y: D.kospi,
      type: 'scatter', mode: 'lines', name: '코스피', yaxis: 'y2',
      connectgaps: true,
      line: {{color: '#185FA5', width: 2.0}},
      hovertemplate: '%{{x}}<br>코스피 %{{y:,.0f}}<extra></extra>'
    }});
    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: '원/달러 환율 vs 코스피 · 환율↑ = 원화 약세', font: {{size: 13}}}},
      yaxis:  {{title: '원/달러 (₩)', gridcolor: '#F3F4F6', side: 'left'}},
      yaxis2: {{title: '코스피', overlaying: 'y', side: 'right', showgrid: false}},
      legend: {{orientation: 'h', y: -0.15}},
      margin: {{t: 45, r: 60, b: 45, l: 60}}
    }}), {{displayModeBar: false, responsive: true}});
  }} catch(e) {{ console.error('krwusd plot:', e); showEmpty(id); }}
}})();

safePlot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', connectgaps: false, line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
], '코스피 vs 신용잔고', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '잔고(억)', overlaying: 'y', side: 'right'}}}});

// === 코스피 + 코스닥 + 전력기기 대표주 누적 추세 (Base 100) ===
(function() {{
  const id = 'c_kr_power';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.kr_power_dates || D.kr_power_dates.length === 0 || !D.kr_power_series || Object.keys(D.kr_power_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    const colorMap = {{
      'KOSPI': '#6B7280',
      'KOSDAQ': '#1E3A8A',
      'LS_ELECTRIC': '#111827',
      'HD_HE': '#0EA5E9',
      'HYOSUNG_HI': '#DC2626',
      'SANIL': '#10B981'
    }};
    const labelMap = {{
      'KOSPI': '코스피',
      'KOSDAQ': '코스닥',
      'LS_ELECTRIC': 'LS ELECTRIC',
      'HD_HE': 'HD현대일렉트릭',
      'HYOSUNG_HI': '효성중공업',
      'SANIL': '산일전기'
    }};
    const order = ['KOSPI', 'KOSDAQ', 'LS_ELECTRIC', 'HD_HE', 'HYOSUNG_HI', 'SANIL'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.kr_power_series[t];
      if (!vals || !hasValues(vals)) return;
      const isKospi = t === 'KOSPI';
      const isKosdaq = t === 'KOSDAQ';
      traces.push({{
        x: D.kr_power_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,
        line: {{
          color: colorMap[t] || '#666',
          width: (isKospi || isKosdaq) ? 2.6 : 2.4,
          dash: isKospi ? 'dot' : (isKosdaq ? 'dash' : 'solid'),
          shape: 'spline',
          smoothing: 1.0
        }},
        hoverlabel: {{font: {{size: 13}}}}
      }});
    }});
    if (traces.length === 0) {{ showEmpty(id); return; }}

    const ranking = traces
      .map(t => {{
        const lastVal = [...t.y].reverse().find(v => v !== null && v !== undefined);
        return {{key: t._key, name: t.name, last: lastVal}};
      }})
      .filter(x => x.last !== undefined && x.last !== null)
      .sort((a, b) => b.last - a.last);

    const lineHeight = 0.05;
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
      title: {{text: '코스피 + 코스닥 + 전력기기 대표주 누적 추세 (Base 100)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('kr power plot:', e); showEmpty(id); }}
}})();

// === 코스피 + 코스닥 + 핵심 조선주 누적 추세 (Base 100) ===
(function() {{
  const id = 'c_kr_ship';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.kr_ship_dates || D.kr_ship_dates.length === 0 || !D.kr_ship_series || Object.keys(D.kr_ship_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    const colorMap = {{
      'KOSPI':      '#6B7280',
      'KOSDAQ':     '#1E3A8A',
      'HD_KSH':     '#DC2626',
      'HANWHA_OC':  '#F59E0B',
      'SAMSUNG_HI': '#10B981',
      'HD_HHI':    '#7C3AED'
    }};
    const labelMap = {{
      'KOSPI':      '코스피',
      'KOSDAQ':     '코스닥',
      'HD_KSH':     'HD한국조선해양',
      'HANWHA_OC':  '한화오션',
      'SAMSUNG_HI': '삼성중공업',
      'HD_HHI':    'HD현대중공업'
    }};
    const order = ['KOSPI', 'KOSDAQ', 'HD_KSH', 'HANWHA_OC', 'SAMSUNG_HI', 'HD_HHI'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.kr_ship_series[t];
      if (!vals || !hasValues(vals)) return;
      const isKospi = t === 'KOSPI';
      const isKosdaq = t === 'KOSDAQ';
      traces.push({{
        x: D.kr_ship_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,
        line: {{
          color: colorMap[t] || '#666',
          width: (isKospi || isKosdaq) ? 2.6 : 2.4,
          dash: isKospi ? 'dot' : (isKosdaq ? 'dash' : 'solid'),
          shape: 'spline',
          smoothing: 1.0
        }},
        hoverlabel: {{font: {{size: 13}}}}
      }});
    }});
    if (traces.length === 0) {{ showEmpty(id); return; }}

    const ranking = traces
      .map(t => {{
        const lastVal = [...t.y].reverse().find(v => v !== null && v !== undefined);
        return {{key: t._key, name: t.name, last: lastVal}};
      }})
      .filter(x => x.last !== undefined && x.last !== null)
      .sort((a, b) => b.last - a.last);

    const lineHeight = 0.05;
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
      title: {{text: '코스피 + 코스닥 + 핵심 조선주 누적 추세 (Base 100)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('kr ship plot:', e); showEmpty(id); }}
}})();

safePlot('c_kr_semi', [
  {{x: D.dates, y: D.kospi_base100, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#888', width: 1.2, dash: 'dot'}}}},
  {{x: D.dates, y: D.samsung_base100, type: 'scatter', mode: 'lines', name: '삼성전자', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.hynix_base100, type: 'scatter', mode: 'lines', name: 'SK하이닉스', connectgaps: true, line: {{color: '#534AB7', width: 2.2}}}},
  {{x: D.dates, y: D.mu_base100, type: 'scatter', mode: 'lines', name: '마이크론', connectgaps: true, line: {{color: '#10B981', width: 2.2}}}},
  {{x: D.dates, y: D.sksquare_base100, type: 'scatter', mode: 'lines', name: 'SK스퀘어', connectgaps: true, line: {{color: '#DC2626', width: 2.2}}}}
], '반도체 누적 추세 (Base 100)', {{yaxis: {{title: 'Base 100'}}}});

safePlot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', mode: 'lines', name: '반도체', connectgaps: false, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates, y: D.sec_방산조선, type: 'scatter', mode: 'lines', name: '방산조선', connectgaps: false, line: {{color: '#534AB7', width: 2.0}}}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', mode: 'lines', name: '바이오', connectgaps: false, line: {{color: '#2AA198', width: 1.8}}}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', mode: 'lines', name: '2차전지', connectgaps: false, line: {{color: '#BA7517', width: 1.8}}}},
  {{x: D.dates, y: D.sec_금융, type: 'scatter', mode: 'lines', name: '금융', connectgaps: false, line: {{color: '#888', width: 1.8}}}},
  {{x: D.dates, y: D.sec_전기, type: 'scatter', mode: 'lines', name: '전기', connectgaps: false, line: {{color: '#DC2626', width: 2.0}}}}
], '업종 바구니 누적 추세 (Base 100)', {{yaxis: {{title: 'Base 100'}}}});

// === 스토리지 종목 누적 추세 (STX, WDC + S&P500, 나스닥 Base 100) ===
(function() {{
  const id = 'c_us_storage';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.storage_dates || D.storage_dates.length === 0 || !D.storage_series || Object.keys(D.storage_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    const colorMap = {{
      'SP500': '#6B7280',
      'IXIC':  '#1E3A8A',
      'STX':   '#DC2626',
      'WDC':   '#F59E0B'
    }};
    const labelMap = {{
      'SP500': 'S&P500',
      'IXIC':  '나스닥',
      'STX':   '씨게이트 STX',
      'WDC':   'WDC'
    }};
    const order = ['SP500', 'IXIC', 'STX', 'WDC'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.storage_series[t];
      if (!vals || !hasValues(vals)) return;
      const isSP = t === 'SP500';
      const isNas = t === 'IXIC';
      traces.push({{
        x: D.storage_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,
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

    const ranking = traces
      .map(t => {{
        const lastVal = [...t.y].reverse().find(v => v !== null && v !== undefined);
        return {{key: t._key, name: t.name, last: lastVal}};
      }})
      .filter(x => x.last !== undefined && x.last !== null)
      .sort((a, b) => b.last - a.last);

    const lineHeight = 0.05;
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
      title: {{text: '씨게이트 STX + WDC vs 나스닥 + S&P500 누적 추세 (Base 100)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('storage plot:', e); showEmpty(id); }}
}})();

// === 미국 주요 지수 누적 추세 (S&P500 + 나스닥 + 러셀2000 + 필라델피아반도체 Base 100) ===
(function() {{
  const id = 'c_us_indices';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.us_idx_dates || D.us_idx_dates.length === 0 || !D.us_idx_series || Object.keys(D.us_idx_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    const colorMap = {{
      'SP500': '#185FA5',
      'IXIC':  '#534AB7',
      'RUT':   '#1D9E75',
      'SOX':   '#DC2626'
    }};
    const labelMap = {{
      'SP500': 'S&P 500',
      'IXIC':  '나스닥',
      'RUT':   '러셀 2000',
      'SOX':   '필라델피아 반도체'
    }};
    const order = ['SP500', 'IXIC', 'RUT', 'SOX'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.us_idx_series[t];
      if (!vals || !hasValues(vals)) return;
      const isSP = t === 'SP500';
      const isNas = t === 'IXIC';
      traces.push({{
        x: D.us_idx_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,
        line: {{
          color: colorMap[t] || '#666',
          width: 2.4,
          dash: isSP ? 'dot' : (isNas ? 'dash' : 'solid'),
          shape: 'spline',
          smoothing: 1.0
        }},
        hoverlabel: {{font: {{size: 13}}}}
      }});
    }});
    if (traces.length === 0) {{ showEmpty(id); return; }}

    const ranking = traces
      .map(t => {{
        const lastVal = [...t.y].reverse().find(v => v !== null && v !== undefined);
        return {{key: t._key, name: t.name, last: lastVal}};
      }})
      .filter(x => x.last !== undefined && x.last !== null)
      .sort((a, b) => b.last - a.last);

    const lineHeight = 0.05;
    const rankAnnotations = [];
    ranking.forEach((x, i) => {{
      const color = colorMap[x.key] || '#333';
      rankAnnotations.push({{
        xref: 'paper', yref: 'paper',
        x: 0.012, y: 0.985 - (i * lineHeight),
        xanchor: 'left', yanchor: 'top',
        align: 'left', showarrow: false,
        text: '<b>' + (i + 1) + '위</b> ' + x.name + '<b> ' + x.last.toFixed(1) + '</b>',
        font: {{size: 15, color: color, family: 'system-ui'}}
      }});
    }});

    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: 'S&P 500 + 나스닥 + 러셀2000 + 필라델피아반도체 누적 추세 (Base 100)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('us indices plot:', e); showEmpty(id); }}
}})();

safePlot('c_us_vix', [{{x: D.dates, y: D.vix, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VIX', connectgaps: false, line: {{color: '#A32D2D', width: 2.2}}, fillcolor: 'rgba(163,45,45,0.10)'}}], 'VIX 공포지수');

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

// === CNN Fear & Greed Index ===
(function() {{
  const id = 'c_us_cnn_fg';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.cnn_fg_dates || D.cnn_fg_dates.length === 0 || !hasValues(D.cnn_fg_vals)) {{
    showEmpty(id, 'CNN Fear & Greed 데이터 없음'); return;
  }}
  try {{
    // 구간별 배경색: 0-25 극단적 공포(빨강), 25-45 공포, 45-55 중립, 55-75 탐욕, 75-100 극단적 탐욕(초록)
    const colorArr = D.cnn_fg_vals.map(v => {{
      if (v === null) return '#aaa';
      if (v <= 25)  return '#A32D2D';
      if (v <= 45)  return '#D85A30';
      if (v <= 55)  return '#BA7517';
      if (v <= 75)  return '#8DB85C';
      return '#1D9E75';
    }});
    const latest = D.cnn_fg_vals[D.cnn_fg_vals.length - 1];
    const latestColor = latest <= 25 ? '#A32D2D' : latest <= 45 ? '#D85A30' : latest <= 55 ? '#BA7517' : latest <= 75 ? '#8DB85C' : '#1D9E75';
    const latestLabel = latest <= 25 ? '극단적 공포' : latest <= 45 ? '공포' : latest <= 55 ? '중립' : latest <= 75 ? '탐욕' : '극단적 탐욕';
    Plotly.newPlot(id, [{{
      x: D.cnn_fg_dates, y: D.cnn_fg_vals,
      type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'Fear & Greed',
      connectgaps: true,
      line: {{color: '#534AB7', width: 2.2}},
      fillcolor: 'rgba(83,74,183,0.10)',
      hovertemplate: '%{{x}}<br>%{{y:.1f}}<extra></extra>'
    }}], Object.assign({{}}, base, {{
      title: {{
        text: `CNN Fear & Greed Index · 현재 ${{latest !== null ? latest.toFixed(0) : 'N/A'}} <span style="color:${{latestColor}}">(${{latestLabel}})</span>`,
        font: {{size: 14}}
      }},
      yaxis: {{title: '지수 (0=극단공포, 100=극단탐욕)', range: [0, 100], gridcolor: '#F3F4F6'}},
      shapes: [
        {{type:'rect',xref:'paper',x0:0,x1:1,y0:0,y1:25,fillcolor:'rgba(163,45,45,0.06)',line:{{width:0}}}},
        {{type:'rect',xref:'paper',x0:0,x1:1,y0:75,y1:100,fillcolor:'rgba(29,158,117,0.06)',line:{{width:0}}}},
        {{type:'line',xref:'paper',x0:0,x1:1,y0:50,y1:50,line:{{color:'#888',width:1,dash:'dot'}}}},
        {{type:'line',xref:'paper',x0:0,x1:1,y0:25,y1:25,line:{{color:'#A32D2D',width:1,dash:'dot'}}}},
        {{type:'line',xref:'paper',x0:0,x1:1,y0:75,y1:75,line:{{color:'#1D9E75',width:1,dash:'dot'}}}}
      ],
      annotations: [
        {{xref:'paper',yref:'y',x:1.01,y:87,text:'극단탐욕',showarrow:false,font:{{size:10,color:'#1D9E75'}},xanchor:'left'}},
        {{xref:'paper',yref:'y',x:1.01,y:50,text:'중립',showarrow:false,font:{{size:10,color:'#888'}},xanchor:'left'}},
        {{xref:'paper',yref:'y',x:1.01,y:12,text:'극단공포',showarrow:false,font:{{size:10,color:'#A32D2D'}},xanchor:'left'}}
      ],
      margin: {{t:45,r:70,b:35,l:55}}
    }}), {{displayModeBar:false, responsive:true}});
  }} catch(e) {{ console.error('cnn fg plot:', e); showEmpty(id); }}
}})();

// === 연준 보유 국채 (Federal Debt Held by Federal Reserve Banks, FDHBFRBN) ===
(function() {{
  const id = 'c_us_gl';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.fed_debt_dates || D.fed_debt_dates.length === 0 || !hasValues(D.fed_debt_vals)) {{
    showEmpty(id); return;
  }}
  try {{
    // 최신값 대비 색상: 고점 대비 하락 중이면 파랑(QT), 상승 중이면 빨강(QE)
    const vals = D.fed_debt_vals;
    const peak = Math.max(...vals.filter(v => v !== null));
    const latest = vals[vals.length - 1];
    const isQT = latest < peak * 0.98;
    const barColor = isQT ? '#185FA5' : '#A32D2D';

    Plotly.newPlot(id, [{{
      x: D.fed_debt_dates,
      y: D.fed_debt_vals,
      type: 'bar',
      name: '연준 보유 국채',
      marker: {{color: barColor, opacity: 0.85}},
      hovertemplate: '%{{x}}<br>%{{y:.0f}}십억달러<extra></extra>'
    }}], Object.assign({{}}, base, {{
      title: {{text: '연준 보유 국채 (FDHBFRBN) · 십억 달러 · 분기별<br><span style="font-size:11px;color:#888">출처: U.S. Department of the Treasury via FRED®</span>', font: {{size: 14}}}},
      yaxis: {{title: '십억 USD (Billions)', gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      bargap: 0.3,
      margin: {{t: 60, r: 30, b: 40, l: 65}}
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('fed debt plot:', e); showEmpty(id); }}
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
        extras = {"us_margin_debt": {}, "m7_basket": {}, "fed_debt": pd.Series(dtype=float), "krwusd": pd.Series(dtype=float), "cnn_fg": pd.Series(dtype=float)}

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
