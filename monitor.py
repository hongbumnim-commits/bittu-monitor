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



def fetch_macro_us():
    targets = {
        "DGS2":"ust2y","T10Y2Y":"yield_spread","DFII10":"real_yield",
        "DFF":"fed_rate","BAMLH0A0HYM2":"hy_spread",
        "CPILFESL":"core_cpi_lvl","PCEPILFE":"core_pce_lvl",
        "UNRATE":"unrate","WALCL":"fed_assets","PAYEMS":"nfp",
        "UMCSENT":"mich","DTWEXBGS":"dxy",
    }
    result = {}
    for sid, name in targets.items():
        s = fetch_fred_series(sid, name, days=LOOKBACK_DAYS*5)
        if not s.empty: result[name] = s
    print(f"  macro_us: {len(result)}/{len(targets)} series")
    return result

def fetch_macro_kr():
    targets = [
        (["IRSTCI01KRM156N"], "kr_base_rate"),
        (["IRLTLT01KRM156N","IRDNKR01KRM156N"], "kr_10y"),
    ]
    result = {}
    for codes, name in targets:
        for sid in codes:
            s = fetch_fred_series(sid, name, days=LOOKBACK_DAYS*8)
            if not s.empty and len(s)>5:
                result[name]=s; break
    print(f"  macro_kr: {len(result)}/{len(targets)} series")
    return result

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


def fetch_fdr_multi(symbols, name=None, days=LOOKBACK_DAYS):
    """여러 심볼 포맷을 순서대로 시도해 첫 성공 결과를 반환 (해외 거래소 대응).
    예: 키옥시아(도쿄) → '285A.T'(야후) / '285A.JP'(stooq) / '285A' 순으로 시도."""
    for sym in symbols:
        try:
            s = fetch_fdr(sym, name=name, days=days)
            if s is not None and not s.empty:
                print(f"  [{name or sym}] '{sym}' 심볼로 수집 성공 ({len(s)}건)")
                return s
        except Exception as e:
            print(f"  [warn] fetch_fdr_multi '{sym}' 실패: {e}")
            continue
    return pd.Series(dtype=float, name=name or (symbols[0] if symbols else ""))


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
    """
    import urllib.parse
    from io import StringIO

    # 1) CBOE 공식 CDN
    cboe_urls = [
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_History.csv",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_Historical_Data.csv",
    ]
    for url in cboe_urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if r.status_code != 200 or len(r.text) < 50:
                continue
            content = r.text
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
            date_col = None
            for c in df_cboe.columns:
                if re.search(r"date", str(c), re.IGNORECASE):
                    date_col = c
                    break
            if date_col is None:
                continue
            close_col = None
            for c in df_cboe.columns:
                cl = str(c).lower()
                if "close" in cl or cl == "value":
                    close_col = c
                    break
            if close_col is None:
                other_cols = [c for c in df_cboe.columns if c != date_col]
                if len(other_cols) >= 1:
                    close_col = other_cols[-1]
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

    # 3) Yahoo chart API
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


def fetch_sk_nav_info():
    """야후 파이낸스 API를 이용해 SK하이닉스와 SK스퀘어의 가격 및 시총을 조회하고 NAV 할인율을 계산"""
    try:
        url = "https://query2.finance.yahoo.com/v7/finance/quote?symbols=000660.KS,402340.KS"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        res = {}
        if "quoteResponse" in data and "result" in data["quoteResponse"]:
            for res_obj in data['quoteResponse']['result']:
                sym = res_obj.get('symbol')
                res[sym] = {
                    'price': res_obj.get('regularMarketPrice', 0),
                    'marketCap': res_obj.get('marketCap', 0)
                }

        hynix = res.get('000660.KS', {})
        square = res.get('402340.KS', {})

        hynix_price = hynix.get('price', 0)
        hynix_cap = hynix.get('marketCap', 0)
        square_price = square.get('price', 0)
        square_cap = square.get('marketCap', 0)

        if hynix_price > 0 and square_cap > 0:
            shares_held = 146113730  # SK스퀘어의 하이닉스 보유 주식수 (20.5%)
            nav_value = hynix_price * shares_held
            discount = (nav_value - square_cap) / nav_value * 100
            return {
                'hynix_price': hynix_price,
                'hynix_cap': hynix_cap,
                'square_price': square_price,
                'square_cap': square_cap,
                'discount': discount
            }
    except Exception as e:
        print(f"  [warn] fetch_sk_nav_info failed: {e}")
    return None


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
    first = df.iloc[0]
    valid_cols = first[(first > 0) & first.notna()].index
    if len(valid_cols) == 0:
        return pd.Series(dtype=float)
    normalized = df[valid_cols].div(first[valid_cols]) * 100
    return normalized.mean(axis=1)


# ================================================================
# 미국 신용잔고 - FINRA Margin Statistics (월별)
# ================================================================
FINRA_MARGIN_URLS = [
    "https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx",
    "https://www.finra.org/sites/default/files/margin-statistics.xlsx",
]

def fetch_us_margin_debt():
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


# ── PCB / 반도체 패키징 바스켓 (코스피200 + 삼성전기·이수페타시스·하이닉스·삼성전자·심텍·코리아써키트·대덕전자·LG이노텍)
KR_PCB_STOCKS = {
    "KS200":    ("KS200",  "코스피200"),
    "SEMCO":    ("009150", "삼성전기"),
    "ISUPE":    ("007660", "이수페타시스"),
    "HYNIX":    ("000660", "SK하이닉스"),
    "SEC":      ("005930", "삼성전자"),
    "SIMTEC":   ("222800", "심텍"),
    "KCIRCUIT": ("007810", "코리아써키트"),
    "DAEDUK":   ("008060", "대덕전자"),
    "LGINNO":   ("011070", "LG이노텍"),
}


def fetch_kr_pcb_basket():
    """코스피200 + 국내 PCB/반도체 패키징 대표주 6개월 누적 추세용 바스켓."""
    result = {}
    for ticker, (source_symbol, _) in KR_PCB_STOCKS.items():
        s = safe(
            f"kr_pcb_{ticker}",
            lambda ts=source_symbol, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS),
            default=pd.Series(dtype=float),
        )
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  kr pcb basket: {len(result)} tickers")
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
    "SP500":     "S&P500",
    "IXIC":      "나스닥",
    "STX":       "씨게이트 STX",
    "SNDK":      "샌디스크 SNDK",
    "SKSQUARE":  "SK스퀘어",
    "BE":        "블룸에너지 BE",
}


def fetch_storage_basket():
    """씨게이트·SNDK + S&P500 + 나스닥 Base 100 바스켓."""
    # 이제 실제 상장된 SNDK를 직접 호출합니다.
    SYMBOL_MAP = {"SP500": "US500", "IXIC": "IXIC", "STX": "STX", "SNDK": "SNDK", "SKSQUARE": "402340", "BE": "BE"}
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
# EPS 탭 — 종목별 TTM EPS vs 주가 추이 (Base 100, '24/01 기준)
# ================================================================

EPS_BASE_DATE = "2025-01-01"   # Base 100 기준일

# ================================================================
# EPS 탭 — 분기별 EPS 하드코딩 데이터 (HTS 재무추이 기준)
# data: [(year, month, eps_per_share), ...]  오름차순 정렬
# 추정치(회색 행) 포함 — 분기말 해당 월의 마지막 날을 날짜로 사용
# ================================================================
EPS_QUARTERLY_DATA = {
    # ── 한국 ──────────────────────────────────────────────────────
    "005930": {
        "name": "삼성전자", "fdr": "005930",
        "data": [
            (2024,3,975),(2024,6,1420),(2024,9,1440),(2024,12,1115),
            (2025,3,1186),(2025,6,733),(2025,9,1783),(2025,12,2864),
            (2026,6,10474),(2026,9,12509),(2026,12,13556),
        ],
    },
    "009150": {
        "name": "삼성전기", "fdr": "009150",
        "data": [
            (2024,3,2360),(2024,6,2222),(2024,9,1485),(2024,12,2685),
            (2025,3,1723),(2025,6,1672),(2025,9,2833),(2025,12,2871),
            (2026,6,3816),(2026,9,4794),(2026,12,4512),
        ],
    },
    "353200": {
        "name": "대덕전자", "fdr": "353200",
        "data": [
            (2024,3,38),(2024,6,237),(2024,9,100),(2024,12,85),
            (2025,3,-111),(2025,6,86),(2025,9,452),(2025,12,497),
            (2026,6,875),(2026,9,972),(2026,12,925),
        ],
    },
    "000660": {
        "name": "SK하이닉스", "fdr": "000660",
        "data": [
            (2024,3,2636),(2024,6,5660),(2024,9,7897),(2024,12,10990),
            (2025,3,11136),(2025,6,9612),(2025,9,17301),(2025,12,20906),
            (2026,6,68109),(2026,9,82163),(2026,12,88184),(2027,3,90446),
        ],
    },
    "267260": {
        "name": "HD현대일렉트릭", "fdr": "267260",
        "data": [
            (2024,3,2607),(2024,6,4479),(2024,9,3242),(2024,12,3586),
            (2025,3,4274),(2025,6,3951),(2025,9,5301),(2025,12,6797),
            (2026,6,6210),(2026,9,7122),(2026,12,8481),(2027,3,6627),
        ],
    },
    "062040": {
        "name": "산일전기", "fdr": "062040",
        "data": [
            (2024,9,794),(2024,12,887),
            (2025,3,1026),(2025,6,1244),(2025,9,1210),
            (2026,6,1551),(2026,9,1725),(2026,12,1852),(2027,3,1810),
        ],
    },
    "298040": {
        "name": "효성중공업", "fdr": "298040",
        "data": [
            (2024,3,2323),(2024,6,3861),(2024,9,7927),(2024,12,9766),
            (2025,3,10956),(2025,6,9922),(2025,9,16109),(2025,12,18769),
            (2026,6,22505),(2026,9,22842),(2026,12,26080),(2027,3,18682),
        ],
    },
    "010120": {
        "name": "LS ELECTRIC", "fdr": "010120",
        "data": [
            (2024,3,523),(2024,6,427),(2024,9,224),(2024,12,418),
            (2025,3,466),(2025,6,447),(2025,9,443),(2025,12,555),
            (2026,6,745),(2026,9,815),(2026,12,981),(2027,3,904),
        ],
    },
    # ── 미국 ──────────────────────────────────────────────────────
    "SNDK": {
        "name": "샌디스크", "fdr": "SNDK",
        "data": [
            (2024,3,0.19),(2024,6,0.83),(2024,9,1.46),(2024,12,0.72),
            (2025,3,-13.33),(2025,6,-0.16),(2025,9,0.75),(2025,12,5.15),
            (2026,3,23.03),(2026,6,32.32),(2026,9,40.63),(2026,12,44.93),(2027,3,34.21),
        ],
    },
    "MU": {
        "name": "마이크론", "fdr": "MU",
        "data": [
            (2024,2,0.71),(2024,5,0.30),(2024,8,0.79),(2024,11,1.67),
            (2025,2,1.41),(2025,5,1.68),(2025,8,2.83),(2025,11,4.60),
            (2026,2,12.07),(2026,5,19.16),(2026,8,20.85),(2026,11,22.28),(2027,2,21.80),
        ],
    },
    "STX": {
        "name": "씨게이트", "fdr": "STX",
        "data": [
            (2024,3,0.12),(2024,6,2.39),(2024,9,1.41),(2024,12,1.55),
            (2025,3,1.57),(2025,6,2.24),(2025,9,2.43),(2025,12,2.60),
            (2026,3,3.27),(2026,6,2.97),(2026,9,3.09),(2026,12,3.15),(2027,3,3.12),
        ],
    },
    "GOOGL": {
        "name": "구글", "fdr": "GOOGL",
        "data": [
            (2024,3,1.89),(2024,6,1.89),(2024,9,2.12),(2024,12,2.15),
            (2025,3,2.81),(2025,6,2.31),(2025,9,2.87),(2025,12,2.82),
            (2026,3,5.11),(2026,6,2.81),(2026,9,2.99),(2026,12,3.20),(2027,3,3.53),
        ],
    },
    "NVDA": {
        "name": "엔비디아", "fdr": "NVDA",
        "data": [
            (2024,1,0.49),(2024,4,0.60),(2024,7,0.67),(2024,10,0.78),
            (2025,1,0.89),(2025,4,0.76),(2025,7,1.08),(2025,10,1.30),
            (2026,1,1.76),(2026,4,1.74),(2026,7,1.93),(2026,10,2.11),(2027,1,2.29),
        ],
    },
    "MSFT": {
        "name": "마이크로소프트", "fdr": "MSFT",
        "data": [
            (2024,3,2.94),(2024,6,2.95),(2024,9,3.30),(2024,12,3.23),
            (2025,3,3.46),(2025,6,3.65),(2025,9,3.72),(2025,12,5.16),
            (2026,3,4.27),(2026,6,4.16),(2026,9,4.65),(2026,12,4.88),(2027,3,4.92),
        ],
    },
    "AVGO": {
        "name": "브로드컴", "fdr": "AVGO",
        "data": [
            (2024,1,0.28),(2024,4,0.44),(2024,7,-0.40),(2024,10,0.90),
            (2025,1,1.14),(2025,4,1.03),(2025,7,0.85),(2025,10,1.74),
            (2026,1,1.50),(2026,4,2.36),(2026,7,3.02),(2026,10,3.70),(2027,1,3.97),
        ],
    },
    "AMD": {
        "name": "AMD", "fdr": "AMD",
        "data": [
            (2024,3,0.07),(2024,6,0.16),(2024,9,0.47),(2024,12,0.29),
            (2025,3,0.44),(2025,6,0.54),(2025,9,0.75),(2025,12,0.92),
            (2026,3,0.84),(2026,6,1.59),(2026,9,1.88),(2026,12,2.48),(2027,3,2.43),
        ],
    },
    "INTC": {
        "name": "인텔", "fdr": "INTC",
        "data": [
            (2024,6,-0.38),(2024,9,-3.88),(2024,12,-0.03),
            (2025,3,-0.19),(2025,6,-0.67),(2025,9,0.90),(2025,12,-0.12),
            (2026,3,-0.73),(2026,6,0.20),(2026,9,0.26),(2026,12,0.29),(2027,3,0.24),
        ],
    },
    "QCOM": {
        "name": "퀄컴", "fdr": "QCOM",
        "data": [
            (2025,3,2.52),(2025,6,2.43),(2025,9,-2.89),(2025,12,2.78),
            (2026,3,6.88),(2026,6,2.22),(2026,9,2.39),(2026,12,2.60),(2027,3,2.53),
        ],
    },
    "ARM": {
        "name": "ARM홀딩스", "fdr": "ARM",
        "data": [
            (2024,6,0.29),(2024,9,0.26),(2024,12,0.39),(2025,3,0.31),
            (2025,6,0.24),(2025,9,0.29),(2025,12,0.37),(2026,3,0.36),
            (2026,6,0.34),(2026,9,0.39),(2026,12,0.46),(2027,3,0.50),
        ],
    },
} 

# ================================================================
# EPS 추이2 — 기준일 2026/01, 실적(solid) + 가이던스(dotted), ~2028년
# actual: 실제 발표 분기 EPS  /  guidance: 컨센서스·회사 가이던스 추정치
# ================================================================

EPS2_BASE_DATE = "2026-01-01"

EPS_QUARTERLY_DATA_V2 = {
    # ── 한국 ──────────────────────────────────────────────────────
    "005930": {
        "name": "삼성전자", "fdr": "005930",
        "actual": [
            (2025,3,1186),(2025,6,733),(2025,9,1783),(2025,12,2864),
        ],
        "guidance": [
            (2026,6,10474),(2026,9,12509),(2026,12,13556),
            (2027,3,15000),(2027,6,17000),(2027,9,18500),(2027,12,20000),
            (2028,3,19000),(2028,6,20500),
        ],
    },
    "009150": {
        "name": "삼성전기", "fdr": "009150",
        "actual": [
            (2025,3,1723),(2025,6,1672),(2025,9,2833),(2025,12,2871),
        ],
        "guidance": [
            (2026,6,3816),(2026,9,4794),(2026,12,4512),
            (2027,3,4800),(2027,6,5200),(2027,9,5600),(2027,12,6000),
            (2028,3,5800),(2028,6,6200),
        ],
    },
    "353200": {
        "name": "대덕전자", "fdr": "353200",
        "actual": [
            (2025,3,-111),(2025,6,86),(2025,9,452),(2025,12,497),
        ],
        "guidance": [
            (2026,6,875),(2026,9,972),(2026,12,925),
            (2027,3,1000),(2027,6,1150),(2027,9,1250),(2027,12,1350),
            (2028,3,1300),(2028,6,1400),
        ],
    },
    "000660": {
        "name": "SK하이닉스", "fdr": "000660",
        "actual": [
            (2025,3,11136),(2025,6,9612),(2025,9,17301),(2025,12,20906),
        ],
        "guidance": [
            (2026,6,68109),(2026,9,82163),(2026,12,88184),(2027,3,90446),
            (2027,6,93000),(2027,9,96000),(2027,12,99000),
            (2028,3,100000),(2028,6,103000),
        ],
    },
    "267260": {
        "name": "HD현대일렉트릭", "fdr": "267260",
        "actual": [
            (2025,3,4274),(2025,6,3951),(2025,9,5301),(2025,12,6797),
        ],
        "guidance": [
            (2026,6,6210),(2026,9,7122),(2026,12,8481),(2027,3,6627),
            (2027,6,7500),(2027,9,8500),(2027,12,9500),
            (2028,3,9000),(2028,6,10000),
        ],
    },
    "062040": {
        "name": "산일전기", "fdr": "062040",
        "actual": [
            (2025,3,1026),(2025,6,1244),(2025,9,1210),
        ],
        "guidance": [
            (2026,6,1551),(2026,9,1725),(2026,12,1852),(2027,3,1810),
            (2027,6,1950),(2027,9,2050),(2027,12,2150),
            (2028,3,2100),(2028,6,2200),
        ],
    },
    "298040": {
        "name": "효성중공업", "fdr": "298040",
        "actual": [
            (2025,3,10956),(2025,6,9922),(2025,9,16109),(2025,12,18769),
        ],
        "guidance": [
            (2026,6,22505),(2026,9,22842),(2026,12,26080),(2027,3,18682),
            (2027,6,22000),(2027,9,25000),(2027,12,28000),
            (2028,3,26000),(2028,6,29000),
        ],
    },
    "010120": {
        "name": "LS ELECTRIC", "fdr": "010120",
        "actual": [
            (2025,3,466),(2025,6,447),(2025,9,443),(2025,12,555),
        ],
        "guidance": [
            (2026,6,745),(2026,9,815),(2026,12,981),(2027,3,904),
            (2027,6,980),(2027,9,1060),(2027,12,1120),
            (2028,3,1080),(2028,6,1150),
        ],
    },
    # ── 미국 ──────────────────────────────────────────────────────
    "SNDK": {
        "name": "샌디스크", "fdr": "SNDK",
        "actual": [
            (2025,3,-13.33),(2025,6,-0.16),(2025,9,0.75),(2025,12,5.15),
            (2026,3,23.03),
        ],
        "guidance": [
            (2026,6,32.32),(2026,9,40.63),(2026,12,44.93),(2027,3,34.21),
            (2027,6,38.00),(2027,9,42.00),(2027,12,46.00),
            (2028,3,45.00),(2028,6,49.00),
        ],
    },
    "MU": {
        "name": "마이크론", "fdr": "MU",
        "actual": [
            (2025,2,1.41),(2025,5,1.68),(2025,8,2.83),(2025,11,4.60),
            (2026,2,12.07),
        ],
        "guidance": [
            (2026,5,19.16),(2026,8,20.85),(2026,11,22.28),(2027,2,21.80),
            (2027,5,22.50),(2027,8,23.50),(2027,11,24.00),
            (2028,2,24.50),(2028,5,25.00),
        ],
    },
    "STX": {
        "name": "씨게이트", "fdr": "STX",
        "actual": [
            (2025,3,1.57),(2025,6,2.24),(2025,9,2.43),(2025,12,2.60),
            (2026,3,3.27),
        ],
        "guidance": [
            (2026,6,2.97),(2026,9,3.09),(2026,12,3.15),(2027,3,3.12),
            (2027,6,3.25),(2027,9,3.40),(2027,12,3.55),
            (2028,3,3.60),(2028,6,3.75),
        ],
    },
    "GOOGL": {
        "name": "구글", "fdr": "GOOGL",
        "actual": [
            (2025,3,2.81),(2025,6,2.31),(2025,9,2.87),(2025,12,2.82),
            (2026,3,5.11),
        ],
        "guidance": [
            (2026,6,2.81),(2026,9,2.99),(2026,12,3.20),(2027,3,3.53),
            (2027,6,3.80),(2027,9,4.00),(2027,12,4.20),
            (2028,3,4.10),(2028,6,4.40),
        ],
    },
    "NVDA": {
        "name": "엔비디아", "fdr": "NVDA",
        "actual": [
            (2025,1,0.89),(2025,4,0.76),(2025,7,1.08),(2025,10,1.30),
            (2026,1,1.76),
        ],
        "guidance": [
            (2026,4,1.74),(2026,7,1.93),(2026,10,2.11),(2027,1,2.29),
            (2027,4,2.60),(2027,7,2.90),(2027,10,3.20),
            (2028,1,3.50),(2028,4,3.80),
        ],
    },
    "MSFT": {
        "name": "마이크로소프트", "fdr": "MSFT",
        "actual": [
            (2025,3,3.46),(2025,6,3.65),(2025,9,3.72),(2025,12,5.16),
            (2026,3,4.27),
        ],
        "guidance": [
            (2026,6,4.16),(2026,9,4.65),(2026,12,4.88),(2027,3,4.92),
            (2027,6,5.20),(2027,9,5.50),(2027,12,5.80),
            (2028,3,6.10),(2028,6,6.40),
        ],
    },
    "AVGO": {
        "name": "브로드컴", "fdr": "AVGO",
        "actual": [
            (2025,1,1.14),(2025,4,1.03),(2025,7,0.85),(2025,10,1.74),
            (2026,1,1.50),
        ],
        "guidance": [
            (2026,4,2.36),(2026,7,3.02),(2026,10,3.70),(2027,1,3.97),
            (2027,4,4.30),(2027,7,4.70),(2027,10,5.10),
            (2028,1,5.50),(2028,4,5.90),
        ],
    },
    "AMD": {
        "name": "AMD", "fdr": "AMD",
        "actual": [
            (2025,3,0.44),(2025,6,0.54),(2025,9,0.75),(2025,12,0.92),
            (2026,3,0.84),
        ],
        "guidance": [
            (2026,6,1.59),(2026,9,1.88),(2026,12,2.48),(2027,3,2.43),
            (2027,6,2.70),(2027,9,3.00),(2027,12,3.30),
            (2028,3,3.20),(2028,6,3.50),
        ],
    },
    "INTC": {
        "name": "인텔", "fdr": "INTC",
        "actual": [
            (2025,3,-0.19),(2025,6,-0.67),(2025,9,0.90),(2025,12,-0.12),
            (2026,3,-0.73),
        ],
        "guidance": [
            (2026,6,0.20),(2026,9,0.26),(2026,12,0.29),(2027,3,0.24),
            (2027,6,0.28),(2027,9,0.33),(2027,12,0.38),
            (2028,3,0.42),(2028,6,0.45),
        ],
    },
    "QCOM": {
        "name": "퀄컴", "fdr": "QCOM",
        "actual": [
            (2025,6,2.43),(2025,9,-2.89),(2025,12,2.78),
            (2026,3,6.88),
        ],
        "guidance": [
            (2026,6,2.22),(2026,9,2.39),(2026,12,2.60),(2027,3,2.53),
            (2027,6,2.70),(2027,9,2.85),(2027,12,3.00),
            (2028,3,3.10),(2028,6,3.20),
        ],
    },
    "ARM": {
        "name": "ARM홀딩스", "fdr": "ARM",
        "actual": [
            (2025,6,0.24),(2025,9,0.29),(2025,12,0.37),
            (2026,3,0.36),
        ],
        "guidance": [
            (2026,6,0.34),(2026,9,0.39),(2026,12,0.46),(2027,3,0.50),
            (2027,6,0.48),(2027,9,0.55),(2027,12,0.62),
            (2028,3,0.65),(2028,6,0.68),
        ],
    },
}


def _build_eps2_combined(actual_data, guidance_data):
    """
    actual + guidance 데이터를 합쳐 하나의 연속 일별 시계열을 만든 뒤
    EPS2_BASE_DATE 기준 Base 100으로 정규화.

    반환:
      act_dates, act_vals   — 실적 구간 (solid, 마지막 actual 날짜까지)
      guid_dates, guid_vals — 가이던스 구간 (dotted, 마지막 actual 이후)
    둘 다 같은 base_val로 정규화되어 차트에서 자연스럽게 이어짐.
    """
    import calendar

    base_ts  = pd.Timestamp(EPS2_BASE_DATE)
    today_ts = pd.Timestamp(TODAY)

    def to_ts_pairs(data):
        out = []
        for year, month, eps in sorted(data):
            ld = calendar.monthrange(year, month)[1]
            out.append((pd.Timestamp(dt.date(year, month, ld)), float(eps)))
        return out

    act_pairs  = to_ts_pairs(actual_data)
    guid_pairs = to_ts_pairs(guidance_data)
    all_pairs  = sorted(act_pairs + guid_pairs, key=lambda x: x[0])

    if not all_pairs:
        return [], [], [], []

    all_s = pd.Series(
        [p[1] for p in all_pairs],
        index=pd.DatetimeIndex([p[0] for p in all_pairs])
    ).sort_index()

    # 전체 범위: BASE_DATE ~ max(오늘, 마지막 데이터)
    end_ts   = max(today_ts, all_s.index.max())
    start_ts = min(all_s.index.min(), base_ts)
    full_daily = (all_s.reindex(pd.date_range(start_ts, end_ts, freq="D"))
                       .interpolate(method="linear").bfill())

    # Base 100 기준값 (2026-01-01 시점)
    if base_ts in full_daily.index:
        base_val = float(full_daily[base_ts])
    else:
        after = full_daily[full_daily.index >= base_ts]
        base_val = float(after.iloc[0]) if not after.empty else None
    if not base_val or pd.isna(base_val) or base_val == 0:
        return [], [], [], []

    normed = (full_daily / base_val * 100)
    normed = normed[normed.index >= base_ts]

    # Actual 구간: BASE_DATE ~ min(마지막 actual 날짜, 오늘)
    # 단, 마지막 actual이 BASE_DATE 이전이면 actual 구간은 없고 전체가 guidance
    if act_pairs:
        last_act_ts  = max(p[0] for p in act_pairs)
        if last_act_ts >= base_ts:
            # actual 데이터가 2026 이후까지 있음 → 해당 날짜까지 solid
            actual_end = min(last_act_ts, today_ts)
            act_part   = normed[normed.index <= actual_end]
            guid_part  = normed[normed.index >= last_act_ts]  # 접점 포함
        else:
            # 모든 actual이 2026 이전 → 2026부터는 전부 guidance(dotted)
            act_part  = pd.Series(dtype=float)
            guid_part = normed
    else:
        act_part  = pd.Series(dtype=float)
        guid_part = normed

    def fmt(s):
        if s.empty: return [], []
        return ([d.date().strftime("%Y-%m-%d") for d in s.index],
                [round(float(v), 2) for v in s])

    return *fmt(act_part), *fmt(guid_part)
    """
    분기별 EPS [(year, month, eps), ...] → 일별 선형보간 시계열.
    - 분기말(해당 월 마지막 날) 기준으로 날짜 생성
    - 분기 사이(추정치 포함): 선형 보간으로 매끄러운 곡선
    - 인덱스를 '데이터 마지막 날'까지 확장 후 오늘 날짜로 잘라냄
      → 오늘이 미래 분기 데이터 이전이어도 추정치 구간을 정확히 보간
    - BASE_DATE 이전: bfill
    Returns: pd.Series, index=date 객체
    """
    import calendar
    if not data_list:
        return pd.Series(dtype=float)

    pairs = []
    for year, month, eps in sorted(data_list):
        last_day = calendar.monthrange(year, month)[1]
        pairs.append((pd.Timestamp(dt.date(year, month, last_day)), float(eps)))

    ts_idx = pd.DatetimeIndex([p[0] for p in pairs])
    values  = [p[1] for p in pairs]
    s = pd.Series(values, index=ts_idx).sort_index()

    today_ts = pd.Timestamp(TODAY)
    base_ts  = pd.Timestamp(EPS_BASE_DATE)

    # 인덱스: BASE_DATE ~ max(오늘, 마지막 데이터 날짜)
    # 추정치 포함 미래 구간까지 확장해야 보간이 제대로 됨
    end_ts   = max(today_ts, s.index.max())
    start_ts = min(s.index.min(), base_ts)

    daily_idx = pd.date_range(start_ts, end_ts, freq="D")
    s_daily   = s.reindex(daily_idx)

    # 알려진 분기점 사이: 선형 보간 / 처음 이전: bfill
    s_daily = s_daily.interpolate(method="linear").bfill()

    # 오늘 이후 미래 구간 제거 → 오늘까지만 표시
    s_daily = s_daily[s_daily.index <= today_ts]
    # BASE_DATE 이전 제거
    s_daily = s_daily[s_daily.index >= base_ts]

    s_daily.index = s_daily.index.date
    return s_daily


def _fetch_price_since(fdr_symbol, start=EPS_BASE_DATE, days=None):
    """Base Date 이후 주가 시계열 반환."""
    try:
        start_date = start
        end_date = TODAY.strftime("%Y-%m-%d")
        df = fdr.DataReader(fdr_symbol, start_date, end_date)
        if df is None or df.empty:
            return pd.Series(dtype=float)
        s = df["Close"]
        s.index = pd.to_datetime(s.index).date
        return s.dropna()
    except Exception as e:
        print(f"  [warn] price {fdr_symbol}: {e}")
        return pd.Series(dtype=float)


def _build_daily_eps_from_quarterly(data_list):
    """
    분기별 EPS [(year, month, eps), ...] → 일별 선형보간 시계열.
    인덱스를 EPS2와 동일하게 문자열 YYYY-MM-DD로 반환.
    """
    import calendar as _cal
    if not data_list:
        return pd.Series(dtype=float)

    pairs = []
    for year, month, eps in sorted(data_list):
        ld = _cal.monthrange(year, month)[1]
        pairs.append((pd.Timestamp(dt.date(year, month, ld)), float(eps)))

    s = pd.Series([p[1] for p in pairs],
                  index=pd.DatetimeIndex([p[0] for p in pairs])).sort_index()

    today_ts = pd.Timestamp(TODAY)
    base_ts  = pd.Timestamp(EPS_BASE_DATE)
    end_ts   = max(today_ts, s.index.max())
    start_ts = min(s.index.min(), base_ts)

    s_daily = (s.reindex(pd.date_range(start_ts, end_ts, freq="D"))
                .interpolate(method="linear").bfill())
    s_daily = s_daily[s_daily.index <= today_ts]
    s_daily = s_daily[s_daily.index >= base_ts]

    # 인덱스 → 문자열 (타입 문제 원천 차단)
    s_daily.index = [d.strftime("%Y-%m-%d") for d in s_daily.index]
    return s_daily


def _normalize_series_to_b100(s, base_date_str):
    """
    문자열 인덱스 Series를 base_date_str 기준 Base 100으로 정규화.
    (dates_list, vals_list) 튜플 반환.
    """
    if s is None or s.empty:
        return [], []
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return [], []
    # 인덱스가 문자열이 아니면 변환
    try:
        new_idx = [i.strftime("%Y-%m-%d") if hasattr(i, "strftime") else str(i)[:10]
                   for i in s.index]
        s = pd.Series(s.values, index=new_idx)
    except Exception:
        pass
    s = s.sort_index()
    s = s[s.index >= base_date_str]
    if s.empty:
        return [], []
    base_val = float(s.iloc[0])
    if pd.isna(base_val) or base_val == 0:
        return [], []
    if base_val < 0:
        normed = 100 + (s - base_val) / abs(base_val) * 100
    else:
        normed = s / base_val * 100
    return list(normed.index), [round(float(v), 2) for v in normed]


def fetch_eps_basket():
    """
    EPS 추이 탭용 데이터 수집.
    EPS / 주가 모두 Python에서 직접 Base 100 정규화해서 반환.
    (JS의 _norm_b100 의존도를 제거해 EPS선 미표시 버그 해결)
    """
    base_str = EPS_BASE_DATE  # "2025-01-01"
    result = {}
    for ticker, meta in EPS_QUARTERLY_DATA.items():
        name    = meta["name"]
        fdr_sym = meta["fdr"]
        print(f"  eps_basket: {name} ({ticker})")

        # EPS 일별 시계열 (문자열 인덱스)
        eps_s = safe(
            f"eps_build_{ticker}",
            lambda d=meta["data"]: _build_daily_eps_from_quarterly(d),
            default=pd.Series(dtype=float),
        )
        # 주가 시계열
        price_s = safe(
            f"eps_price_{ticker}",
            lambda fs=fdr_sym: _fetch_price_since(fs),
            default=pd.Series(dtype=float),
        )

        # Python에서 직접 Base 100 정규화
        eps_dates, eps_vals = _normalize_series_to_b100(eps_s, base_str)
        px_dates,  px_vals  = _normalize_series_to_b100(price_s, base_str)

        # 최신 EPS 원본값 (적자 표시용)
        eps_raw_latest = None
        if eps_vals:
            eps_raw_latest = eps_vals[-1]
        eps_neg_base = bool(eps_vals and eps_vals[0] < 0)  # base 시점 음수 여부

        result[ticker] = {
            "name":           name,
            "price_dates":    px_dates,
            "price_vals":     px_vals,
            "eps_dates":      eps_dates,
            "eps_vals":       eps_vals,
            "eps_raw_latest": eps_raw_latest,
            "eps_neg_base":   eps_neg_base,
        }

    print(f"  eps_basket: {len(result)}/{len(EPS_QUARTERLY_DATA)} tickers")
    return result


def fetch_eps2_basket():
    """
    EPS 추이2 탭용 (기준일 2026-01-01, 가이던스 포함 ~2028).
    반환: {ticker: {name, price, act_dates, act_vals, guid_dates, guid_vals}}
    """
    result = {}
    for ticker, meta in EPS_QUARTERLY_DATA_V2.items():
        name    = meta["name"]
        fdr_sym = meta["fdr"]
        print(f"  eps2_basket: {name} ({ticker})")

        # 가격 (2026-01-01 이후)
        price_s = safe(
            f"eps2_price_{ticker}",
            lambda fs=fdr_sym: _fetch_price_since(fs, start=EPS2_BASE_DATE),
            default=pd.Series(dtype=float),
        )
        # EPS actual + guidance
        act_d, act_v, guid_d, guid_v = safe(
            f"eps2_build_{ticker}",
            lambda a=meta["actual"], g=meta["guidance"]: _build_eps2_combined(a, g),
            default=([], [], [], []),
        )

        result[ticker] = {
            "name":       name,
            "price":      price_s,
            "act_dates":  act_d,
            "act_vals":   act_v,
            "guid_dates": guid_d,
            "guid_vals":  guid_v,
        }

    print(f"  eps2_basket: {len(result)}/{len(EPS_QUARTERLY_DATA_V2)} tickers")
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
    "kospi", "kosdaq", "samsung", "hynix", "mu", "sksquare", "sndk", "kioxia",
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
    # 이제 진짜 SNDK 티커로 데이터를 수집합니다.
    sndk = safe("sndk", lambda: fetch_fdr("SNDK", "sndk"), default=pd.Series(dtype=float, name="sndk"))
    # 키옥시아(도쿄 285A) — NAND 순수 플레이. FDR 도쿄 심볼 포맷이 버전마다 달라 다중 시도.
    kioxia = safe("kioxia", lambda: fetch_fdr_multi(["285A.T", "285A.JP", "285A"], "kioxia"),
                  default=pd.Series(dtype=float, name="kioxia"))

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
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix, "mu": mu, "sksquare": sksquare, "sndk": sndk, "kioxia": kioxia,
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
    kr_pcb_basket = safe("kr_pcb_basket", fetch_kr_pcb_basket, default={})
    m7_basket = safe("m7_plus", fetch_m7_plus_basket, default={})
    us_indices_basket = safe("us_indices", fetch_us_indices_basket, default={})
    storage_basket = safe("storage", fetch_storage_basket, default={})
    fed_debt  = safe("fed_debt", fetch_fed_debt,       default=pd.Series(dtype=float, name="fed_debt"))
    krwusd    = safe("krwusd",   fetch_krwusd,          default=pd.Series(dtype=float, name="krwusd"))
    cnn_fg    = safe("cnn_fg",   fetch_cnn_fear_greed,  default=pd.Series(dtype=float, name="cnn_fg"))

    macro_us = safe("macro_us", fetch_macro_us, default={})
    macro_kr = safe("macro_kr", fetch_macro_kr, default={})
    sk_nav = safe("sk_nav", fetch_sk_nav_info, default=None)
    eps_basket  = safe("eps_basket",  fetch_eps_basket,  default={})
    eps2_basket = safe("eps2_basket", fetch_eps2_basket, default={})

    extras = {
        "us_margin_debt":us_margin_debt,"kr_power_basket":kr_power_basket,
        "kr_ship_basket":kr_ship_basket,"kr_pcb_basket":kr_pcb_basket,"m7_basket":m7_basket,
        "us_indices_basket":us_indices_basket,"storage_basket":storage_basket,
        "fed_debt":fed_debt,"krwusd":krwusd,"cnn_fg":cnn_fg,
        "macro_us":macro_us,"macro_kr":macro_kr,
        "sk_nav":sk_nav,"eps_basket":eps_basket,"eps2_basket":eps2_basket,
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

    def base100_custom(df_target, c, *, max_daily_jump_pct=25):
        if df_target.empty or c not in df_target.columns:
            return []
        s = sanitize_series(df_target[c], min_val=1, max_daily_jump_pct=max_daily_jump_pct).ffill()
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

    # --- 특정 차트들을 위한 3개월(90일) 전용 데이터셋 분리 ---
    cutoff_3m = TODAY - dt.timedelta(days=90)
    df_3m = df_plot[df_plot["date"] >= cutoff_3m].copy() if not df_plot.empty else pd.DataFrame()
    dates_3m = [d.strftime("%Y-%m-%d") for d in df_3m["date"]] if not df_3m.empty else []

    # --- 반도체 차트 3종용: 6개월(182일) / 1년(365일) 윈도우 ---
    cutoff_6m = TODAY - dt.timedelta(days=182)
    df_6m = df_plot[df_plot["date"] >= cutoff_6m].copy() if not df_plot.empty else pd.DataFrame()
    dates_6m = [d.strftime("%Y-%m-%d") for d in df_6m["date"]] if not df_6m.empty else []

    cutoff_1y = TODAY - dt.timedelta(days=365)
    df_1y = df_plot[df_plot["date"] >= cutoff_1y].copy() if not df_plot.empty else pd.DataFrame()
    dates_1y = [d.strftime("%Y-%m-%d") for d in df_1y["date"]] if not df_1y.empty else []

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
        "credit": series_connected("credit_balance_eok", min_val=100000, max_daily_jump_pct=15),
        "sp500": sp500,
        "sp500_ma200": ma_series("sp500", 200),
        "nasdaq": nasdaq,
        "nasdaq_ma200": ma_series("nasdaq", 200),
        "vix": series_connected("vix", min_val=5, max_val=100, max_daily_jump_pct=60),
        "nvda": series_connected("nvda", min_val=10, max_daily_jump_pct=40),
        "ust10y": series_connected("ust10y", min_val=0, max_val=10, max_daily_jump_pct=20),
        "cor1m": cor1m,
        "sp500_range": y_range([sp500, ma_series("sp500", 200)]),
        "nasdaq_range": y_range([nasdaq, ma_series("nasdaq", 200)]),
        "kospi_range": y_range([kospi, ma_series("kospi", 200)]),
        "kosdaq_range": y_range([kosdaq, ma_series("kosdaq", 200)]),

        # --- 3개월 Base 100 변수들 (요청 차트 전용) ---
        # max_daily_jump_pct=80: 신고가 종목(키옥시아/샌디스크/마이크론 등)의 하루 급등이
        # 기존 25% 캡에 깎여 수익률이 과소 표시되던 문제 수정. 80%면 현실적 일중 변동은
        # 전부 보존하면서, 누락 0(미씽) 같은 데이터 글리치만 걸러냄.
        "dates_3m": dates_3m,
        "kospi_base100_3m": base100_custom(df_3m, "kospi", max_daily_jump_pct=80),
        "samsung_base100_3m": base100_custom(df_3m, "samsung", max_daily_jump_pct=80),
        "hynix_base100_3m": base100_custom(df_3m, "hynix", max_daily_jump_pct=80),
        "mu_base100_3m": base100_custom(df_3m, "mu", max_daily_jump_pct=80),
        "sksquare_base100_3m": base100_custom(df_3m, "sksquare", max_daily_jump_pct=80),
        "sndk_base100_3m": base100_custom(df_3m, "sndk", max_daily_jump_pct=80),  # 샌디스크
        "kioxia_base100_3m": base100_custom(df_3m, "kioxia", max_daily_jump_pct=80),  # 키옥시아(도쿄 285A)
        # 6개월 Base 100 (반도체 차트)
        "dates_6m": dates_6m,
        "kospi_base100_6m": base100_custom(df_6m, "kospi", max_daily_jump_pct=80),
        "samsung_base100_6m": base100_custom(df_6m, "samsung", max_daily_jump_pct=80),
        "hynix_base100_6m": base100_custom(df_6m, "hynix", max_daily_jump_pct=80),
        "mu_base100_6m": base100_custom(df_6m, "mu", max_daily_jump_pct=80),
        "sndk_base100_6m": base100_custom(df_6m, "sndk", max_daily_jump_pct=80),
        "kioxia_base100_6m": base100_custom(df_6m, "kioxia", max_daily_jump_pct=80),
        "sksquare_base100_6m": base100_custom(df_6m, "sksquare", max_daily_jump_pct=80),
        # 1년 Base 100 (반도체 차트)
        "dates_1y": dates_1y,
        "kospi_base100_1y": base100_custom(df_1y, "kospi", max_daily_jump_pct=80),
        "samsung_base100_1y": base100_custom(df_1y, "samsung", max_daily_jump_pct=80),
        "hynix_base100_1y": base100_custom(df_1y, "hynix", max_daily_jump_pct=80),
        "mu_base100_1y": base100_custom(df_1y, "mu", max_daily_jump_pct=80),
        "sndk_base100_1y": base100_custom(df_1y, "sndk", max_daily_jump_pct=80),
        "kioxia_base100_1y": base100_custom(df_1y, "kioxia", max_daily_jump_pct=80),
        "sksquare_base100_1y": base100_custom(df_1y, "sksquare", max_daily_jump_pct=80),
        "sec_반도체_3m": base100_custom(df_3m, "sec_반도체"),
        "sec_방산조선_3m": base100_custom(df_3m, "sec_방산조선"),
        "sec_바이오_3m": base100_custom(df_3m, "sec_바이오"),
        "sec_2차전지_3m": base100_custom(df_3m, "sec_2차전지"),
        "sec_금융_3m": base100_custom(df_3m, "sec_금융"),
        "sec_전기_3m": base100_custom(df_3m, "sec_전기"),
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

    # 3-2b. PCB / 반도체 패키징 바스켓 Base 100 (최근 6개월)
    kr_pcb_basket = extras.get("kr_pcb_basket", {}) or {}
    kr_pcb_dates = []
    kr_pcb_series = {}
    if kr_pcb_basket:
        anchor = None
        if "KS200" in kr_pcb_basket and not kr_pcb_basket["KS200"].empty:
            anchor = kr_pcb_basket["KS200"]
        else:
            for _, s in kr_pcb_basket.items():
                if not s.empty:
                    anchor = s
                    break
        if anchor is not None:
            cutoff = TODAY - dt.timedelta(days=180)
            anchor = anchor[anchor.index >= cutoff]
            kr_pcb_dates = [d.strftime("%Y-%m-%d") for d in anchor.index]
            for ticker, s in kr_pcb_basket.items():
                s2 = s[s.index >= cutoff]
                s2_reidx = s2.reindex(anchor.index).ffill().bfill()
                first_val = s2_reidx.dropna().iloc[0] if not s2_reidx.dropna().empty else None
                if first_val and first_val > 0:
                    normed = (s2_reidx / first_val) * 100
                    kr_pcb_series[ticker] = [None if pd.isna(v) else round(float(v), 2) for v in normed]
    js_data["kr_pcb_dates"] = kr_pcb_dates
    js_data["kr_pcb_series"] = kr_pcb_series

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

    _mfus = extras.get("macro_us", {}) or {}
    def _ms(nm, lm=4):
        s = _mfus.get(nm)
        if s is None or not isinstance(s, pd.Series) or s.empty:
            return {"dates": [], "vals": []}
        s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
        s = s[s.index >= TODAY - dt.timedelta(days=LOOKBACK_DAYS * lm)]
        return {"dates":[d.strftime("%Y-%m-%d") for d in s.index],
                "vals":[round(float(v),4) for v in s]}
    def _ms_yoy(nm, lm=4):
        s = _mfus.get(nm)
        if s is None or not isinstance(s, pd.Series) or s.empty:
            return {"dates": [], "vals": []}
        s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
        yoy = s.pct_change(periods=12, fill_method=None).dropna() * 100
        yoy = yoy[yoy.index >= TODAY - dt.timedelta(days=LOOKBACK_DAYS * lm)]
        return {"dates":[d.strftime("%Y-%m-%d") for d in yoy.index],
                "vals":[round(float(v),2) for v in yoy]}
    js_data["m_ust2y"]      = _ms("ust2y")
    js_data["m_yield_sprd"] = _ms("yield_spread")
    js_data["m_real_yield"] = _ms("real_yield")
    js_data["m_fed_rate"]   = _ms("fed_rate")
    js_data["m_hy_sprd"]    = _ms("hy_spread")
    js_data["m_core_cpi"]   = _ms_yoy("core_cpi_lvl")
    js_data["m_core_pce"]   = _ms_yoy("core_pce_lvl")
    js_data["m_unrate"]     = _ms("unrate")
    js_data["m_fed_assets"] = _ms("fed_assets", lm=8)
    js_data["m_nfp"]        = _ms("nfp")
    js_data["m_mich"]       = _ms("mich")
    js_data["m_dxy"]        = _ms("dxy")
    _mfkr = extras.get("macro_kr", {}) or {}
    def _mks(nm):
        s = _mfkr.get(nm)
        if s is None or not isinstance(s, pd.Series) or s.empty:
            return {"dates": [], "vals": []}
        s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
        s = s[s.index >= TODAY - dt.timedelta(days=LOOKBACK_DAYS * 5)]
        return {"dates":[d.strftime("%Y-%m-%d") for d in s.index],
                "vals":[round(float(v),4) for v in s]}
    js_data["m_kr_base"] = _mks("kr_base_rate")
    js_data["m_kr_10y"]  = _mks("kr_10y")

    # ── EPS 탭 데이터 처리 ────────────────────────────────────────────
    eps_basket = extras.get("eps_basket", {}) or {}
    eps_js = {}

    # fetch_eps_basket에서 이미 Python에서 정규화된 데이터를 직접 사용
    # (render_dashboard의 _norm_b100을 거치지 않음 → EPS선 미표시 버그 해결)
    for ticker, data in eps_basket.items():
        eps_js[ticker] = {
            "name":           data.get("name", ticker),
            "price_dates":    data.get("price_dates", []),
            "price_vals":     data.get("price_vals",  []),
            "eps_dates":      data.get("eps_dates",   []),
            "eps_vals":       data.get("eps_vals",    []),
            "eps_raw_latest": data.get("eps_raw_latest"),
            "eps_neg_base":   data.get("eps_neg_base", False),
        }

    js_data["eps_basket"] = eps_js

    # ── EPS 추이2 탭 데이터 (기준일 2026-01-01, 가이던스 포함) ──────────
    eps2_basket = extras.get("eps2_basket", {}) or {}
    eps2_js = {}
    base2_str = EPS2_BASE_DATE

    for ticker, data in eps2_basket.items():
        name    = data.get("name", ticker)
        price_s = data.get("price", pd.Series(dtype=float))
        act_d   = data.get("act_dates", [])
        act_v   = data.get("act_vals",  [])
        guid_d  = data.get("guid_dates",[])
        guid_v  = data.get("guid_vals", [])

        # Price Base 100 (2026-01-01 기준) — 가격만 _normalize_series_to_b100 사용
        pd2_dates, pd2_vals = _normalize_series_to_b100(price_s, base2_str)

        # 최신 EPS값 (가이던스 포함 마지막 값)
        raw_latest = guid_v[-1] if guid_v else (act_v[-1] if act_v else None)

        eps2_js[ticker] = {
            "name":       name,
            "price_dates": pd2_dates,
            "price_vals":  pd2_vals,
            "act_dates":  act_d,
            "act_vals":   act_v,
            "guid_dates": guid_d,
            "guid_vals":  guid_v,
            "eps_b100_latest": raw_latest,
        }

    js_data["eps2_basket"] = eps2_js

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

    # --- NAV 할인율 섹션 HTML 동적 생성 ---
    sk_nav = extras.get("sk_nav")
    sk_nav_html = ""
    if sk_nav:
        sk_nav_html = f"""
      <div class="section-title">SK스퀘어 NAV 할인율 (당일 실시간/종가 기준)</div>
      <div class="regime-grid" style="margin-bottom: 20px;">
        <div class="regime-card" style="border-top-color: #185FA5;">
          <div class="regime-name">SK하이닉스</div>
          <div class="regime-value">{sk_nav['hynix_price']:,.0f}원</div>
          <div class="regime-reasons">시총: {sk_nav['hynix_cap']/1000000000000:,.1f}조원</div>
        </div>
        <div class="regime-card" style="border-top-color: #DC2626;">
          <div class="regime-name">SK스퀘어</div>
          <div class="regime-value">{sk_nav['square_price']:,.0f}원</div>
          <div class="regime-reasons">시총: {sk_nav['square_cap']/1000000000000:,.1f}조원</div>
        </div>
        <div class="regime-card" style="border-top-color: #7C3AED;">
          <div class="regime-name">NAV 할인율</div>
          <div class="regime-value">{sk_nav['discount']:.2f}%</div>
          <div class="regime-reasons">보유지분 1.46억주 적용</div>
        </div>
      </div>
        """

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
  <div class="tab" data-tab="macro">📡 매크로</div>
  <div class="tab" data-tab="eps">📊 EPS 추이</div>
  <div class="tab" data-tab="eps2">📈 EPS 추이2</div>
</div>

<div id="pane-kr" class="pane active">
  {regime_kr_html}
  {sk_nav_html}
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
  <div class="chart"><div id="c_kr_pcb" style="height:680px;"></div></div>
  <div class="chart"><div id="c_kr_semi_1y" style="height:560px;"></div></div>
  <div class="chart"><div id="c_kr_semi_6m" style="height:560px;"></div></div>
  <div class="chart"><div id="c_kr_semi_3m" style="height:560px;"></div></div>
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

<div id="pane-macro" class="pane">
  <div class="section-title">📡 매크로 대시보드 — 매일 FRED 자동 업데이트</div>
  <div class="section-title" style="font-size:12px;color:#666;margin-top:4px">🇺🇸 미국 핵심 지표</div>
  <div class="section-title" style="font-size:11px;color:#888">📈 금리 · 수익률 곡선</div>
  <div class="chart-grid">
    <div class="chart"><div id="m_rate_curve" style="height:300px;"></div></div>
    <div class="chart"><div id="m_yield_sprd" style="height:300px;"></div></div>
  </div>
  <div class="chart-grid">
    <div class="chart"><div id="m_real_yield" style="height:270px;"></div></div>
    <div class="chart"><div id="m_fed_rate"   style="height:270px;"></div></div>
  </div>
  <div class="section-title" style="font-size:11px;color:#888">💳 신용 · 달러</div>
  <div class="chart-grid">
    <div class="chart"><div id="m_hy_sprd" style="height:270px;"></div></div>
    <div class="chart"><div id="m_dxy"     style="height:270px;"></div></div>
  </div>
  <div class="section-title" style="font-size:11px;color:#888">🔥 인플레이션</div>
  <div class="chart-grid">
    <div class="chart"><div id="m_core_cpi" style="height:270px;"></div></div>
    <div class="chart"><div id="m_core_pce" style="height:270px;"></div></div>
  </div>
  <div class="section-title" style="font-size:11px;color:#888">🏭 고용 · 경기</div>
  <div class="chart-grid">
    <div class="chart"><div id="m_unrate"  style="height:270px;"></div></div>
    <div class="chart"><div id="m_nfp_mom" style="height:270px;"></div></div>
  </div>
  <div class="chart"><div id="m_mich" style="height:240px;"></div></div>
  <div class="section-title" style="font-size:11px;color:#888">🏦 연준</div>
  <div class="chart"><div id="m_fed_assets" style="height:270px;"></div></div>
  <div class="section-title" style="font-size:12px;color:#666;margin-top:18px">🇰🇷 한국 핵심 지표</div>
  <div class="chart-grid">
    <div class="chart"><div id="m_kr_base"   style="height:260px;"></div></div>
    <div class="chart"><div id="m_kr_10y"    style="height:260px;"></div></div>
  </div>
  <div class="chart"><div id="m_kr_krwusd" style="height:280px;"></div></div>
</div>

<div id="pane-eps" class="pane">
  <div class="section-title">📊 EPS vs 주가 추이 — 분기별 EPS · Base 100 ('25/01 = 100)</div>
  <div style="font-size:12px;color:#888;margin-bottom:18px;">
    진한 선 = 주가 · 연한 선 = 분기 EPS (선형보간, 추정치 포함) · Base 100 기준: 2024년 1월 · EPS 음영: 이익 개선 전 구간
  </div>
  <div class="section-title" style="font-size:13px;color:#555;margin-bottom:8px;">🇰🇷 한국</div>
  <div class="chart-grid">
    <div class="chart"><div id="eps_005930" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_009150" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_353200" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_000660" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_267260" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_062040" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_298040" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_010120" style="height:320px;"></div></div>
  </div>
  <div class="section-title" style="font-size:13px;color:#555;margin:18px 0 8px;">🇺🇸 미국</div>
  <div class="chart-grid">
    <div class="chart"><div id="eps_SNDK"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_MU"    style="height:320px;"></div></div>
    <div class="chart"><div id="eps_STX"   style="height:320px;"></div></div>
    <div class="chart"><div id="eps_GOOGL" style="height:320px;"></div></div>
    <div class="chart"><div id="eps_NVDA"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_MSFT"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_AVGO"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_AMD"   style="height:320px;"></div></div>
    <div class="chart"><div id="eps_INTC"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_QCOM"  style="height:320px;"></div></div>
    <div class="chart"><div id="eps_ARM"   style="height:320px;"></div></div>
  </div>
</div>

<div id="pane-eps2" class="pane">
  <div class="section-title">📈 EPS 추이2 — 실적(실선) + 가이던스(점선) · Base 100 ('26/01 = 100)</div>
  <div style="font-size:12px;color:#888;margin-bottom:18px;">
    진한 실선 = 주가 · 연한 실선 = 분기 EPS (실제 발표) · 연한 점선 = EPS 가이던스/컨센서스 (~2028년) · 적자구간 붉은 음영
  </div>
  <div class="section-title" style="font-size:13px;color:#555;margin-bottom:8px;">🇰🇷 한국</div>
  <div class="chart-grid">
    <div class="chart"><div id="eps2_005930" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_009150" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_353200" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_000660" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_267260" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_062040" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_298040" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_010120" style="height:340px;"></div></div>
  </div>
  <div class="section-title" style="font-size:13px;color:#555;margin:18px 0 8px;">🇺🇸 미국</div>
  <div class="chart-grid">
    <div class="chart"><div id="eps2_SNDK"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_MU"    style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_STX"   style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_GOOGL" style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_NVDA"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_MSFT"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_AVGO"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_AMD"   style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_INTC"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_QCOM"  style="height:340px;"></div></div>
    <div class="chart"><div id="eps2_ARM"   style="height:340px;"></div></div>
  </div>
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

// === 코스피200 + PCB / 반도체 패키징 대표주 누적 추세 (Base 100, 최근 6개월) ===
(function() {{
  const id = 'c_kr_pcb';
  const el = document.getElementById(id);
  if (!el) return;
  if (!D.kr_pcb_dates || D.kr_pcb_dates.length === 0 || !D.kr_pcb_series || Object.keys(D.kr_pcb_series).length === 0) {{
    showEmpty(id);
    return;
  }}
  try {{
    const colorMap = {{
      'KS200':    '#9CA3AF',
      'SEMCO':    '#0EA5E9',
      'ISUPE':    '#10B981',
      'HYNIX':    '#534AB7',
      'SEC':      '#185FA5',
      'SIMTEC':   '#F59E0B',
      'KCIRCUIT': '#EF4444',
      'DAEDUK':   '#8B5CF6',
      'LGINNO':   '#EC4899'
    }};
    const labelMap = {{
      'KS200':    '코스피200',
      'SEMCO':    '삼성전기',
      'ISUPE':    '이수페타시스',
      'HYNIX':    'SK하이닉스',
      'SEC':      '삼성전자',
      'SIMTEC':   '심텍',
      'KCIRCUIT': '코리아써키트',
      'DAEDUK':   '대덕전자',
      'LGINNO':   'LG이노텍'
    }};
    const order = ['KS200','SEC','HYNIX','SEMCO','ISUPE','SIMTEC','KCIRCUIT','DAEDUK','LGINNO'];
    const traces = [];
    order.forEach(t => {{
      const vals = D.kr_pcb_series[t];
      if (!vals || !hasValues(vals)) return;
      const isIdx = t === 'KS200';
      traces.push({{
        x: D.kr_pcb_dates,
        y: vals,
        type: 'scatter',
        mode: 'lines',
        name: labelMap[t] || t,
        connectgaps: true,
        _key: t,
        line: {{
          color: colorMap[t] || '#666',
          width: isIdx ? 1.8 : 2.3,
          dash: isIdx ? 'dot' : 'solid',
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

    const lineHeight = 0.049;
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
        font: {{size: 14, color: color, family: 'system-ui'}}
      }});
    }});

    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: '코스피200 + PCB / 반도체 패키징 대표주 누적 추세 (Base 100, 최근 6개월)', font: {{size: 16}}}},
      yaxis: {{title: {{text: 'Base 100', font: {{size: 13}}}}, gridcolor: '#F3F4F6'}},
      xaxis: {{gridcolor: '#F3F4F6'}},
      legend: {{orientation: 'h', y: -0.08, x: 0.5, xanchor: 'center', font: {{size: 12}}}},
      margin: {{t: 60, r: 40, b: 90, l: 60}},
      annotations: rankAnnotations,
      hovermode: 'x unified'
    }}), {{displayModeBar: false, responsive: true}});
  }} catch (e) {{ console.error('kr pcb plot:', e); showEmpty(id); }}
}})();

// --- 반도체 누적 추세 3종 (1년 / 6개월 / 3개월) ---
// 1년·6개월은 로그축: 키옥시아 등 수십 배 오른 종목이 선형축에선 나머지 선을 바닥에 깔아버림.
// 로그축에서는 '같은 세로 간격 = 같은 수익률'이라 종목 간 상대 성과 비교가 정확해짐.
safePlot('c_kr_semi_1y', [
  {{x: D.dates_1y, y: D.kospi_base100_1y, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#888', width: 1.2, dash: 'dot'}}}},
  {{x: D.dates_1y, y: D.samsung_base100_1y, type: 'scatter', mode: 'lines', name: '삼성전자', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates_1y, y: D.hynix_base100_1y, type: 'scatter', mode: 'lines', name: 'SK하이닉스', connectgaps: true, line: {{color: '#534AB7', width: 2.2}}}},
  {{x: D.dates_1y, y: D.mu_base100_1y, type: 'scatter', mode: 'lines', name: '마이크론', connectgaps: true, line: {{color: '#10B981', width: 2.2}}}},
  {{x: D.dates_1y, y: D.sndk_base100_1y, type: 'scatter', mode: 'lines', name: 'SNDK(샌디스크)', connectgaps: true, line: {{color: '#F59E0B', width: 2.2}}}},
  {{x: D.dates_1y, y: D.kioxia_base100_1y, type: 'scatter', mode: 'lines', name: '키옥시아(285A)', connectgaps: true, line: {{color: '#C026D3', width: 2.2}}}},
  {{x: D.dates_1y, y: D.sksquare_base100_1y, type: 'scatter', mode: 'lines', name: 'SK스퀘어', connectgaps: true, line: {{color: '#DC2626', width: 2.2}}}}
], '반도체 누적 추세 (Base 100, 최근 1년 · 로그축)', {{yaxis: {{title: 'Base 100', type: 'log'}}}});

safePlot('c_kr_semi_6m', [
  {{x: D.dates_6m, y: D.kospi_base100_6m, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#888', width: 1.2, dash: 'dot'}}}},
  {{x: D.dates_6m, y: D.samsung_base100_6m, type: 'scatter', mode: 'lines', name: '삼성전자', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates_6m, y: D.hynix_base100_6m, type: 'scatter', mode: 'lines', name: 'SK하이닉스', connectgaps: true, line: {{color: '#534AB7', width: 2.2}}}},
  {{x: D.dates_6m, y: D.mu_base100_6m, type: 'scatter', mode: 'lines', name: '마이크론', connectgaps: true, line: {{color: '#10B981', width: 2.2}}}},
  {{x: D.dates_6m, y: D.sndk_base100_6m, type: 'scatter', mode: 'lines', name: 'SNDK(샌디스크)', connectgaps: true, line: {{color: '#F59E0B', width: 2.2}}}},
  {{x: D.dates_6m, y: D.kioxia_base100_6m, type: 'scatter', mode: 'lines', name: '키옥시아(285A)', connectgaps: true, line: {{color: '#C026D3', width: 2.2}}}},
  {{x: D.dates_6m, y: D.sksquare_base100_6m, type: 'scatter', mode: 'lines', name: 'SK스퀘어', connectgaps: true, line: {{color: '#DC2626', width: 2.2}}}}
], '반도체 누적 추세 (Base 100, 최근 6개월 · 로그축)', {{yaxis: {{title: 'Base 100', type: 'log'}}}});

safePlot('c_kr_semi_3m', [
  {{x: D.dates_3m, y: D.kospi_base100_3m, type: 'scatter', mode: 'lines', name: '코스피', connectgaps: true, line: {{color: '#888', width: 1.2, dash: 'dot'}}}},
  {{x: D.dates_3m, y: D.samsung_base100_3m, type: 'scatter', mode: 'lines', name: '삼성전자', connectgaps: true, line: {{color: '#185FA5', width: 2.2}}}},
  {{x: D.dates_3m, y: D.hynix_base100_3m, type: 'scatter', mode: 'lines', name: 'SK하이닉스', connectgaps: true, line: {{color: '#534AB7', width: 2.2}}}},
  {{x: D.dates_3m, y: D.mu_base100_3m, type: 'scatter', mode: 'lines', name: '마이크론', connectgaps: true, line: {{color: '#10B981', width: 2.2}}}},
  {{x: D.dates_3m, y: D.sndk_base100_3m, type: 'scatter', mode: 'lines', name: 'SNDK(샌디스크)', connectgaps: true, line: {{color: '#F59E0B', width: 2.2}}}},
  {{x: D.dates_3m, y: D.kioxia_base100_3m, type: 'scatter', mode: 'lines', name: '키옥시아(285A)', connectgaps: true, line: {{color: '#C026D3', width: 2.2}}}},
  {{x: D.dates_3m, y: D.sksquare_base100_3m, type: 'scatter', mode: 'lines', name: 'SK스퀘어', connectgaps: true, line: {{color: '#DC2626', width: 2.2}}}}
], '반도체 누적 추세 (Base 100, 최근 3개월)', {{yaxis: {{title: 'Base 100'}}}});

// === 스토리지 종목 누적 추세 (STX, SNDK + S&P500, 나스닥 Base 100) ===
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
      'SP500':    '#6B7280',
      'IXIC':     '#1E3A8A',
      'STX':      '#DC2626',
      'SNDK':     '#F59E0B',
      'SKSQUARE': '#7C3AED',
      'BE':       '#10B981'
    }};
    const labelMap = {{
      'SP500':    'S&P500',
      'IXIC':     '나스닥',
      'STX':      '씨게이트 STX',
      'SNDK':     '샌디스크 SNDK',
      'SKSQUARE': 'SK스퀘어',
      'BE':       '블룸에너지 BE'
    }};
    const order = ['SP500', 'IXIC', 'STX', 'SNDK', 'SKSQUARE', 'BE'];
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
        font: {{size: 14, color: color, family: 'system-ui'}}
      }});
    }});

    Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
      title: {{text: '씨게이트 STX + 샌디스크 SNDK + SK스퀘어 + 블룸에너지 vs 나스닥 + S&P500 누적 추세 (Base 100)', font: {{size: 15}}}},
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


// ============================================================
// 📡 MACRO TAB — 신호등 + 기준선 (밴드 없음, 스케일 안전)
// ============================================================
(function(){{
  function gm(k){{ return D['m_'+k]||{{dates:[],vals:[]}}; }}
  function hasV(a){{ return Array.isArray(a)&&a.some(function(v){{return v!==null&&!isNaN(v);}}); }}
  function lat(a){{ var f=(a||[]).filter(function(v){{return v!==null&&!isNaN(v);}}); return f.length?f[f.length-1]:null; }}
  function fv(v,d){{ return (v===null||v===undefined||isNaN(v))?'N/A':Number(v).toFixed(d===undefined?2:d); }}

  function sig(v,low,high,ls){{
    if(v===null||isNaN(v)) return {{e:'⚪',l:'N/A'}};
    if(v>=high) return {{e:'🔴',l:ls[2]||'위험'}};
    if(v>=low)  return {{e:'🟡',l:ls[1]||'주의'}};
    return {{e:'🟢',l:ls[0]||'안정'}};
  }}

  function ln(x,y,name,color,dash){{
    return {{x:x,y:y,name:name,type:'scatter',mode:'lines',connectgaps:true,
      line:{{color:color,width:2.2,dash:dash||'solid'}},
      hovertemplate:'%{{x}}<br>%{{y:.2f}}<extra></extra>'}};
  }}
  function ar(x,y,name,color){{
    var fc=color.replace('rgb(','rgba(').replace(')',',0.15)');
    return {{x:x,y:y,name:name,type:'scatter',mode:'lines',connectgaps:true,fill:'tozeroy',
      line:{{color:color,width:2.0}},fillcolor:fc,
      hovertemplate:'%{{x}}<br>%{{y:.2f}}<extra></extra>'}};
  }}

  // 수평 기준선 shapes (데이터 좌표) — y-axis range 자동, 절대 스케일 안망침
  function hlines(items){{
    return items.map(function(h){{
      return {{type:'line',xref:'paper',x0:0,x1:1,y0:h.y,y1:h.y,
        line:{{color:h.c||'#bbb',width:1,dash:h.d||'dot'}}}};
    }});
  }}
  function hlabels(items){{
    return items.map(function(h){{
      return {{xref:'paper',yref:'y',x:1.01,y:h.y,text:h.t,showarrow:false,
        font:{{size:10,color:h.c||'#888'}},xanchor:'left'}};
    }});
  }}

  var BM={{t:50,r:96,b:36,l:60}};
  function mplt(id,traces,title,extra){{
    var el=document.getElementById(id);
    if(!el) return;
    var valid=(traces||[]).filter(function(t){{return hasV(t.y);}});
    if(!valid.length){{showEmpty(id,title+' — FRED 수집 중');return;}}
    try{{
      Plotly.newPlot(id,valid,
        Object.assign({{}},base,{{title:{{text:title,font:{{size:13}}}},margin:BM}},extra||{{}}),
        {{displayModeBar:false,responsive:true}});
    }}catch(e){{console.error(id,e);showEmpty(id);}}
  }}

  // 1. 국채 2Y + 10Y
  (function(){{
    var t2=gm('ust2y'), l2=lat(t2.vals), l10=lat(D.ust10y||[]);
    var s=l10>=4.5?{{e:'🔴',l:'위험 (성장주 압박)'}}:l10>=4.0?{{e:'🟡',l:'주의 (긴축 부담)'}}:{{e:'🟢',l:'안정'}};
    mplt('m_rate_curve',
      [ln(t2.dates,t2.vals,'2년물','#E24B4A'),ln(D.dates,D.ust10y,'10년물','#185FA5','dash')],
      '미국 국채금리   2년물 '+fv(l2)+'%  /  10년물 '+fv(l10)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'금리 (%)',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:2.0,c:'#1D9E75'}},{{y:4.0,c:'#BA7517'}},{{y:4.5,c:'#E24B4A'}},{{y:5.0,c:'#8B0000'}}]),
        annotations:hlabels([{{y:2.0,t:'저금리(2%)',c:'#1D9E75'}},{{y:4.0,t:'주의(4%)',c:'#BA7517'}},{{y:4.5,t:'위험(4.5%)',c:'#E24B4A'}},{{y:5.0,t:'충격(5%)',c:'#8B0000'}}])}});
  }})();

  // 2. 10Y-2Y 스프레드
  (function(){{
    var sp=gm('yield_sprd'), lv=lat(sp.vals);
    if(!sp.vals.length){{showEmpty('m_yield_sprd','10Y-2Y 스프레드 — 수집 중');return;}}
    var s=lv<-0.5?{{e:'🔴',l:'역전 심화 (침체 강신호)'}}:lv<0?{{e:'🟡',l:'역전 중 (침체 경보)'}}:lv<0.5?{{e:'🟢',l:'정상화 진입'}}:{{e:'🟢',l:'정상'}};
    var cols=sp.vals.map(function(v){{return v===null?'#aaa':v>=0?'rgba(29,158,117,0.75)':'rgba(226,75,74,0.75)';}});
    Plotly.newPlot('m_yield_sprd',
      [{{x:sp.dates,y:sp.vals,type:'bar',name:'10Y-2Y (%p)',marker:{{color:cols}},hovertemplate:'%{{x}}<br>%{{y:.2f}}%p<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'10Y-2Y 스프레드   '+fv(lv)+'%p   '+s.e+' '+s.l,font:{{size:13}}}},
        margin:BM,
        yaxis:{{title:'%p',gridcolor:'#F3F4F6',zeroline:true,zerolinecolor:'#333',zerolinewidth:2}},
        shapes:hlines([{{y:-0.5,c:'#E24B4A'}},{{y:0,c:'#333',d:'solid'}},{{y:0.5,c:'#1D9E75'}}]),
        annotations:hlabels([{{y:-0.5,t:'심화(-0.5%p)',c:'#E24B4A'}},{{y:0,t:'역전기준',c:'#E24B4A'}},{{y:0.5,t:'정상(+0.5%p)',c:'#1D9E75'}}]),
        bargap:0.3
      }}),{{displayModeBar:false,responsive:true}});
  }})();

  // 3. 실질금리
  (function(){{
    var rv=gm('real_yield'), lv=lat(rv.vals);
    var s=sig(lv,0,2.0,['완화 (성장주 유리)','정상 긴축','과도 긴축 (성장주 압박)']);
    mplt('m_real_yield',[ar(rv.dates,rv.vals,'실질금리 (TIPS 10Y)','rgb(83,74,183)')],
      '실질금리 (TIPS 10Y)   '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6',zeroline:true,zerolinecolor:'#888',zerolinewidth:1.5}},
        shapes:hlines([{{y:0,c:'#888',d:'solid'}},{{y:2.0,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:0,t:'0% 중립',c:'#888'}},{{y:2.0,t:'과도긴축(2%)',c:'#E24B4A'}}])}});
  }})();

  // 4. Fed Funds Rate
  (function(){{
    var fd=gm('fed_rate'), lv=lat(fd.vals);
    var s=lv>=4.5?{{e:'🔴',l:'강한 긴축'}}:lv>=3.0?{{e:'🟡',l:'긴축 구간'}}:lv>=2.5?{{e:'🟢',l:'중립 근처'}}:{{e:'🟢',l:'완화'}};
    mplt('m_fed_rate',[ln(fd.dates,fd.vals,'Fed Funds Rate','#1D9E75')],
      'Fed 기준금리 (실효)   현재 '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:2.5,c:'#1D9E75'}},{{y:4.0,c:'#BA7517'}},{{y:5.0,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:2.5,t:'중립(2.5%)',c:'#1D9E75'}},{{y:4.0,t:'긴축(4%)',c:'#BA7517'}},{{y:5.0,t:'강긴축(5%)',c:'#E24B4A'}}])}});
  }})();

  // 5. 하이일드 스프레드 (FRED: % 단위, 3.5%≈350bp)
  (function(){{
    var hy=gm('hy_sprd');
    if(!hy.vals.length){{showEmpty('m_hy_sprd','HY 스프레드 — 수집 중');return;}}
    var lv=lat(hy.vals), bp=lv!==null?Math.round(lv*100):null;
    var s=sig(lv,3.5,6.0,['안정 (크레딧 양호)','주의 (리스크 증가)','위험 (크레딧 경색)']);
    Plotly.newPlot('m_hy_sprd',
      [{{x:hy.dates,y:hy.vals,type:'scatter',mode:'lines',connectgaps:true,fill:'tozeroy',
        name:'HY OAS (%)',line:{{color:'#D85A30',width:2.2}},fillcolor:'rgba(216,90,48,0.10)',
        hovertemplate:'%{{x}}<br>%{{y:.2f}}% (~%{{y:.2f}}×100bp)<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'하이일드 OAS   '+fv(lv)+'% (~'+(bp||'N/A')+'bp)   '+s.e+' '+s.l,font:{{size:13}}}},
        margin:BM,yaxis:{{title:'OAS (%)',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:3.0,c:'#1D9E75'}},{{y:3.5,c:'#BA7517'}},{{y:6.0,c:'#E24B4A'}},{{y:10.0,c:'#8B0000'}}]),
        annotations:hlabels([{{y:3.0,t:'안정(<3%=300bp)',c:'#1D9E75'}},{{y:3.5,t:'주의(3.5%=350bp)',c:'#BA7517'}},{{y:6.0,t:'위험(6%=600bp)',c:'#E24B4A'}},{{y:10.0,t:'위기(10%=1000bp)',c:'#8B0000'}}])
      }}),{{displayModeBar:false,responsive:true}});
  }})();

  // 6. 달러 광의지수
  (function(){{
    var dx=gm('dxy'), lv=lat(dx.vals);
    var s=lv>=120?{{e:'🔴',l:'극강세 (원화 위험)'}}:lv>=110?{{e:'🟡',l:'강세 (원화 약세 압력)'}}:lv<=95?{{e:'🟢',l:'약세 (원화 강세)'}}:{{e:'🟢',l:'중립'}};
    mplt('m_dxy',[ln(dx.dates,dx.vals,'달러 광의지수','#BA7517')],
      '달러 광의지수 (Broad TWD)   '+fv(lv,1)+'   '+s.e+' '+s.l,
      {{yaxis:{{title:'지수',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:95,c:'#1D9E75'}},{{y:110,c:'#BA7517'}},{{y:120,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:95,t:'약세(<95)',c:'#1D9E75'}},{{y:110,t:'강세(110)',c:'#BA7517'}},{{y:120,t:'극강세(120)',c:'#E24B4A'}}])}});
  }})();

  // 7. Core CPI YoY
  (function(){{
    var cc=gm('core_cpi'), lv=lat(cc.vals);
    var s=sig(lv,2.5,3.5,['안정','주의 (목표 초과)','과열 (긴축 지속)']);
    var c=lv>3.5?'rgb(226,75,74)':lv>2.5?'rgb(186,117,23)':'rgb(29,158,117)';
    mplt('m_core_cpi',[ar(cc.dates,cc.vals,'Core CPI YoY',c)],
      'Core CPI YoY   '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'YoY %',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:2.0,c:'#1D9E75'}},{{y:2.5,c:'#BA7517'}},{{y:3.5,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:2.0,t:'목표(2%)',c:'#1D9E75'}},{{y:2.5,t:'주의(2.5%)',c:'#BA7517'}},{{y:3.5,t:'과열(3.5%)',c:'#E24B4A'}}])}});
  }})();

  // 8. Core PCE YoY
  (function(){{
    var cp=gm('core_pce'), lv=lat(cp.vals);
    var s=sig(lv,2.5,3.0,['안정 (Fed 목표 근접)','주의 (인하 지연)','과열 (인하 불가)']);
    mplt('m_core_pce',[ar(cp.dates,cp.vals,'Core PCE YoY','rgb(83,74,183)')],
      'Core PCE YoY (Fed 목표)   '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'YoY %',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:2.0,c:'#1D9E75'}},{{y:2.5,c:'#BA7517'}},{{y:3.0,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:2.0,t:'목표(2%)',c:'#1D9E75'}},{{y:2.5,t:'주의(2.5%)',c:'#BA7517'}},{{y:3.0,t:'과열(3%)',c:'#E24B4A'}}])}});
  }})();

  // 9. 실업률
  (function(){{
    var ur=gm('unrate'), lv=lat(ur.vals);
    var s=lv>=5.5?{{e:'🔴',l:'침체 위험'}}:lv>=4.5?{{e:'🟡',l:'주의 (증가 추세)'}}:lv<=3.5?{{e:'🟡',l:'과열 (인플레 압박)'}}:{{e:'🟢',l:'정상 (3.5~4.5%)'}};
    mplt('m_unrate',[ar(ur.dates,ur.vals,'실업률','rgb(24,95,165)')],
      '미국 실업률   '+fv(lv,1)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:3.5,c:'#BA7517'}},{{y:4.0,c:'#1D9E75'}},{{y:4.5,c:'#BA7517'}},{{y:5.5,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:3.5,t:'과열(<3.5%)',c:'#BA7517'}},{{y:4.0,t:'정상(4%)',c:'#1D9E75'}},{{y:4.5,t:'주의(4.5%)',c:'#BA7517'}},{{y:5.5,t:'침체(5.5%)',c:'#E24B4A'}}])}});
  }})();

  // 10. 비농업 고용 MoM
  // FRED PAYEMS 단위: 천명. 차이 150 = +15만명
  (function(){{
    var nf=gm('nfp');
    if(!nf.vals.length){{showEmpty('m_nfp_mom','비농업 고용 — 수집 중');return;}}
    var mom=nf.vals.map(function(v,i){{
      return (i===0||v===null||nf.vals[i-1]===null)?null:parseFloat((v-nf.vals[i-1]).toFixed(1));
    }});
    var cols=mom.map(function(v){{
      return v===null?'#aaa':v>=150?'rgba(29,158,117,0.80)':v>=0?'rgba(29,158,117,0.45)':'rgba(226,75,74,0.75)';
    }});
    var filt=mom.filter(function(v){{return v!==null;}});
    var latV=filt.length?filt[filt.length-1]:null;
    var s=latV===null?{{e:'⚪',l:'N/A'}}:latV<0?{{e:'🔴',l:'고용 감소'}}:latV<100?{{e:'🟡',l:'약한 증가 (<10만)'}}:{{e:'🟢',l:'양호 (>15만)'}};
    var disp=latV!==null?(latV>0?'+':'')+latV.toFixed(0)+'k명':'N/A';
    Plotly.newPlot('m_nfp_mom',
      [{{x:nf.dates,y:mom,type:'bar',name:'NFP MoM (천명)',marker:{{color:cols}},
        hovertemplate:'%{{x}}<br>%{{y:+.0f}}k명<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'비농업 고용 MoM   최근 '+disp+'   '+s.e+' '+s.l,font:{{size:13}}}},
        margin:{{t:50,r:96,b:36,l:75}},
        yaxis:{{title:'천명 (k)',gridcolor:'#F3F4F6',zeroline:true,zerolinecolor:'#555',zerolinewidth:1.5}},
        shapes:hlines([{{y:150,c:'#1D9E75'}},{{y:100,c:'#BA7517'}},{{y:0,c:'#555',d:'solid'}}]),
        annotations:hlabels([{{y:150,t:'양호(+150k)',c:'#1D9E75'}},{{y:100,t:'보통(+100k)',c:'#BA7517'}},{{y:0,t:'기준선',c:'#555'}}]),
        bargap:0.35
      }}),{{displayModeBar:false,responsive:true}});
  }})();

  // 11. 미시간 소비자신뢰지수
  (function(){{
    var mc=gm('mich'), lv=lat(mc.vals);
    var s=lv<60?{{e:'🔴',l:'극비관 (침체 신호)'}}:lv<70?{{e:'🟡',l:'비관 (소비 위축)'}}:lv<85?{{e:'🟢',l:'보통'}}:{{e:'🟢',l:'낙관'}};
    mplt('m_mich',[ar(mc.dates,mc.vals,'미시간 소비자신뢰','rgb(29,158,117)')],
      '미시간 소비자신뢰지수   '+fv(lv,1)+'   '+s.e+' '+s.l,
      {{yaxis:{{title:'지수',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:60,c:'#E24B4A'}},{{y:70,c:'#BA7517'}},{{y:85,c:'#1D9E75'}}]),
        annotations:hlabels([{{y:60,t:'침체신호(60)',c:'#E24B4A'}},{{y:70,t:'비관(70)',c:'#BA7517'}},{{y:85,t:'보통(85)',c:'#1D9E75'}}])}});
  }})();

  // 12. 연준 자산
  (function(){{
    var fa=gm('fed_assets');
    if(!fa.vals.length){{showEmpty('m_fed_assets','연준 자산 — 수집 중');return;}}
    var vals=fa.vals.filter(function(v){{return v!==null;}});
    var peak=Math.max.apply(null,vals), lv=lat(fa.vals);
    var pct=lv&&peak?lv/peak*100:null;
    var isQT=lv&&lv<peak*0.98;
    var c=isQT?'#185FA5':'#E24B4A';
    Plotly.newPlot('m_fed_assets',
      [{{x:fa.dates,y:fa.vals,type:'scatter',mode:'lines',connectgaps:true,fill:'tozeroy',
        line:{{color:c,width:2.2}},fillcolor:isQT?'rgba(24,95,165,0.10)':'rgba(226,75,74,0.10)',
        hovertemplate:'%{{x}}<br>$%{{y:,.0f}}B<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'연준 자산 규모   $'+(lv?(lv/1000).toFixed(1)+'T':'N/A')+'   '+(isQT?'🔵 QT 진행 중 (고점 대비 '+fv(pct,0)+'%)':'🔴 QE/확대'),font:{{size:13}}}},
        margin:{{t:50,r:40,b:36,l:80}},yaxis:{{title:'십억 USD',gridcolor:'#F3F4F6'}}
      }}),{{displayModeBar:false,responsive:true}});
  }})();

  // ── 한국 매크로 ──────────────────────────────────────────────────
  function gmk(k){{ return D['m_kr_'+k]||{{dates:[],vals:[]}}; }}

  // KR 기준금리
  (function(){{
    var kb=gmk('base'), lv=lat(kb.vals);
    if(!kb.vals.length){{showEmpty('m_kr_base','한국 기준금리 — 수집 중');return;}}
    var s=lv>=3.5?{{e:'🔴',l:'고금리'}}:lv>=2.5?{{e:'🟡',l:'긴축'}}:lv<=1.5?{{e:'🟢',l:'완화'}}:{{e:'🟢',l:'중립'}};
    mplt('m_kr_base',[ln(kb.dates,kb.vals,'한국 기준금리','#1D9E75')],
      '한국 기준금리   '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:1.5,c:'#1D9E75'}},{{y:2.5,c:'#BA7517'}},{{y:3.5,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:1.5,t:'완화(1.5%)',c:'#1D9E75'}},{{y:2.5,t:'중립(2.5%)',c:'#BA7517'}},{{y:3.5,t:'긴축(3.5%)',c:'#E24B4A'}}])}});
  }})();

  // KR 10년물
  (function(){{
    var k10=gmk('10y'), lv=lat(k10.vals);
    if(!k10.vals.length){{showEmpty('m_kr_10y','한국 10년물 — 수집 중');return;}}
    var s=lv>=4.0?{{e:'🔴',l:'고금리'}}:lv>=3.0?{{e:'🟡',l:'긴축'}}:{{e:'🟢',l:'안정'}};
    mplt('m_kr_10y',[ln(k10.dates,k10.vals,'한국 10년물','#185FA5')],
      '한국 국채 10년물   '+fv(lv)+'%   '+s.e+' '+s.l,
      {{yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:3.0,c:'#BA7517'}},{{y:4.0,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:3.0,t:'주의(3%)',c:'#BA7517'}},{{y:4.0,t:'위험(4%)',c:'#E24B4A'}}])}});
  }})();

  // 원/달러
  (function(){{
    var kw={{dates:D.krwusd_dates||[],vals:D.krwusd_vals||[]}};
    var lv=lat(kw.vals);
    var s=lv>=1450?{{e:'🔴',l:'위험 (외국인 이탈)'}}:lv>=1400?{{e:'🟡',l:'주의 (약세 심화)'}}:lv<=1250?{{e:'🟢',l:'원화 강세'}}:{{e:'🟢',l:'안정'}};
    mplt('m_kr_krwusd',[ln(kw.dates,kw.vals,'원/달러','#D85A30')],
      '원/달러 환율   '+fv(lv,0)+'원   '+s.e+' '+s.l,
      {{yaxis:{{title:'원/달러 (₩)',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:1250,c:'#1D9E75'}},{{y:1350,c:'#888'}},{{y:1400,c:'#BA7517'}},{{y:1450,c:'#E24B4A'}}]),
        annotations:hlabels([{{y:1250,t:'강세(1250)',c:'#1D9E75'}},{{y:1350,t:'보통(1350)',c:'#888'}},{{y:1400,t:'주의(1400)',c:'#BA7517'}},{{y:1450,t:'위험(1450)',c:'#E24B4A'}}])}});
  }})();

  // 미국장 탭 VIX 신호등 (기존 safePlot 대체)
  (function(){{
    var el=document.getElementById('c_us_vix');
    if(!el||!D.dates||!hasV(D.vix)) return;
    var lv=lat(D.vix);
    var s=lv>=40?{{e:'🔴',l:'공황'}}:lv>=30?{{e:'🔴',l:'위험'}}:lv>=20?{{e:'🟡',l:'주의'}}:lv>=15?{{e:'🟢',l:'안정'}}:{{e:'🟢',l:'극도 낙관'}};
    Plotly.newPlot('c_us_vix',
      [{{x:D.dates,y:D.vix,type:'scatter',mode:'lines',fill:'tozeroy',name:'VIX',connectgaps:false,
        line:{{color:'#A32D2D',width:2.2}},fillcolor:'rgba(163,45,45,0.10)',hovertemplate:'%{{x}}<br>VIX %{{y:.1f}}<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'VIX 공포지수   현재 '+fv(lv,1)+'   '+s.e+' '+s.l,font:{{size:13}}}},
        yaxis:{{title:'VIX',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:15,c:'#1D9E75'}},{{y:20,c:'#BA7517'}},{{y:30,c:'#E24B4A'}},{{y:40,c:'#8B0000'}}]),
        annotations:hlabels([{{y:15,t:'안정(<15)',c:'#1D9E75'}},{{y:20,t:'주의(20)',c:'#BA7517'}},{{y:30,t:'위험(30)',c:'#E24B4A'}},{{y:40,t:'공황(40)',c:'#8B0000'}}])
      }}),{{displayModeBar:false,responsive:true}});
  }})();

  // 미국장 탭 10년물 신호등 (기존 safePlot 대체)
  (function(){{
    var el=document.getElementById('c_us_rate');
    if(!el||!D.dates||!hasV(D.ust10y)) return;
    var lv=lat(D.ust10y);
    var s=lv>=4.5?{{e:'🔴',l:'위험'}}:lv>=4.0?{{e:'🟡',l:'주의'}}:{{e:'🟢',l:'안정'}};
    Plotly.newPlot('c_us_rate',
      [{{x:D.dates,y:D.ust10y,type:'scatter',mode:'lines',name:'10년물',connectgaps:false,
        line:{{color:'#BA7517',width:2.2}},hovertemplate:'%{{x}}<br>%{{y:.2f}}%<extra></extra>'}}],
      Object.assign({{}},base,{{
        title:{{text:'미국 10년물 국채금리   현재 '+fv(lv)+'%   '+s.e+' '+s.l,font:{{size:13}}}},
        yaxis:{{title:'%',ticksuffix:'%',gridcolor:'#F3F4F6'}},
        shapes:hlines([{{y:3.0,c:'#1D9E75'}},{{y:4.0,c:'#BA7517'}},{{y:4.5,c:'#E24B4A'}},{{y:5.0,c:'#8B0000'}}]),
        annotations:hlabels([{{y:3.0,t:'안정(<3%)',c:'#1D9E75'}},{{y:4.0,t:'주의(4%)',c:'#BA7517'}},{{y:4.5,t:'위험(4.5%)',c:'#E24B4A'}},{{y:5.0,t:'충격(5%)',c:'#8B0000'}}])
      }}),{{displayModeBar:false,responsive:true}});
  }})();

}})(); // end macro charts

// ════════════════════════════════════════════════════════
// EPS 추이2 탭 — 실적(solid) + 가이던스(dotted) Base 100 '26/01
// ════════════════════════════════════════════════════════
(function() {{
  var EPS2_ORDER = [
    '005930','009150','353200','000660',
    '267260','062040','298040','010120',
    'SNDK','MU','STX','GOOGL',
    'NVDA','MSFT','AVGO','AMD',
    'INTC','QCOM','ARM'
  ];
  var C = {{
    '005930':{{p:'#0A2A6E',e:'#7B9FD4'}},'009150':{{p:'#B22222',e:'#E89090'}},
    '353200':{{p:'#1A7A4A',e:'#7DC0A0'}},'000660':{{p:'#6A0DAD',e:'#BF92CB'}},
    '267260':{{p:'#CC5500',e:'#EBA87A'}},'062040':{{p:'#1A6B8A',e:'#7ABFDA'}},
    '298040':{{p:'#7B4F00',e:'#C4906B'}},'010120':{{p:'#2E4057',e:'#8AA0B5'}},
    'SNDK':{{p:'#E06B00',e:'#F5C38A'}},'MU':{{p:'#007A7A',e:'#7DE4D0'}},
    'STX':{{p:'#A00000',e:'#E08080'}},'GOOGL':{{p:'#1A56DB',e:'#9CB8FB'}},
    'NVDA':{{p:'#5A8A00',e:'#B8DB7A'}},'MSFT':{{p:'#0078D4',e:'#7BD1F7'}},
    'AVGO':{{p:'#990000',e:'#E87878'}},'AMD':{{p:'#C00020',e:'#F08090'}},
    'INTC':{{p:'#0071C5',e:'#80B8E2'}},'QCOM':{{p:'#3253DC',e:'#9AAEED'}},
    'ARM':{{p:'#2B3F6C',e:'#8D9DC0'}},
  }};

  var basket2 = D.eps2_basket || {{}};

  EPS2_ORDER.forEach(function(ticker) {{
    var id = 'eps2_' + ticker;
    var el = document.getElementById(id);
    if (!el) return;

    var d = basket2[ticker];
    if (!d) {{ showEmpty(id, ticker + ' — 데이터 수집 중'); return; }}

    var col = C[ticker] || {{p:'#333', e:'#999'}};
    var traces = [];

    // 주가 (solid, 진한색)
    if (hasValues(d.price_vals)) {{
      traces.push({{
        x: d.price_dates, y: d.price_vals,
        type:'scatter', mode:'lines', name: d.name + ' 주가',
        connectgaps:true,
        line:{{color:col.p, width:2.6, dash:'solid'}},
        hovertemplate:'%{{x}}<br>주가 %{{y:.1f}}<extra></extra>'
      }});
    }}

    // EPS 실적 (solid, 연한색)
    if (hasValues(d.act_vals)) {{
      traces.push({{
        x: d.act_dates, y: d.act_vals,
        type:'scatter', mode:'lines', name: d.name + ' EPS실적',
        connectgaps:true,
        line:{{color:col.e, width:2.2, dash:'solid'}},
        hovertemplate:'%{{x}}<br>EPS실적 %{{y:.1f}}<extra></extra>'
      }});
    }}

    // EPS 가이던스 (dot, 연한색)
    if (hasValues(d.guid_vals)) {{
      traces.push({{
        x: d.guid_dates, y: d.guid_vals,
        type:'scatter', mode:'lines', name: d.name + ' EPS가이던스',
        connectgaps:true,
        line:{{color:col.e, width:2.2, dash:'dot'}},
        hovertemplate:'%{{x}}<br>가이던스 %{{y:.1f}}<extra></extra>'
      }});
    }}

    if (traces.length === 0) {{ showEmpty(id, d.name + ' — 데이터 없음'); return; }}

    // 제목
    function lastV(arr) {{
      if (!arr||!arr.length) return null;
      for (var i=arr.length-1;i>=0;i--) if(arr[i]!==null&&arr[i]!==undefined) return arr[i];
      return null;
    }}
    var pxLast   = lastV(d.price_vals);
    var epsLast  = lastV(d.guid_vals) || lastV(d.act_vals);
    var title = d.name + ' — 주가 vs EPS';
    if (pxLast!==null)  title += '  주가 ' + pxLast.toFixed(1);
    if (epsLast!==null) title += '  EPS ' + epsLast.toFixed(1);

    // Base 100 기준선 + EPS<100 구간 음영
    var shapes = [{{
      type:'line', xref:'paper', x0:0, x1:1,
      yref:'y', y0:100, y1:100,
      line:{{color:'#ccc', width:1, dash:'dot'}}
    }}];

    // 가이던스 시작점 수직선
    if (d.guid_dates && d.guid_dates.length > 0) {{
      shapes.push({{
        type:'line', xref:'x', x0:d.guid_dates[0], x1:d.guid_dates[0],
        yref:'paper', y0:0, y1:1,
        line:{{color:'#aaa', width:1, dash:'dash'}}
      }});
    }}

    // EPS<100 적자구간 음영
    var allEpsDates = (d.act_dates||[]).concat(d.guid_dates||[]);
    var allEpsVals  = (d.act_vals||[]).concat(d.guid_vals||[]);
    var inLoss=false, lossStart=null;
    for (var i=0;i<allEpsVals.length;i++) {{
      var v=allEpsVals[i];
      if (v!==null&&v<100&&!inLoss) {{ inLoss=true; lossStart=allEpsDates[i]; }}
      else if ((v===null||v>=100)&&inLoss) {{
        inLoss=false;
        shapes.push({{type:'rect',xref:'x',yref:'paper',
          x0:lossStart,x1:allEpsDates[i-1]||allEpsDates[i],y0:0,y1:1,
          fillcolor:'rgba(220,38,38,0.07)',line:{{width:0}}}});
      }}
    }}
    if (inLoss&&lossStart&&allEpsDates.length)
      shapes.push({{type:'rect',xref:'x',yref:'paper',
        x0:lossStart,x1:allEpsDates[allEpsDates.length-1],y0:0,y1:1,
        fillcolor:'rgba(220,38,38,0.07)',line:{{width:0}}}});

    try {{
      Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
        title:{{text:title, font:{{size:12, color:col.p}}}},
        yaxis:{{title:"Base 100  ('26/01=100)", gridcolor:'#F3F4F6', zeroline:false}},
        xaxis:{{gridcolor:'#F3F4F6'}},
        shapes:shapes,
        annotations:[{{
          xref:'paper', yref:'paper', x:0.01, y:0.97,
          xanchor:'left', yanchor:'top', showarrow:false,
          text:'실선=실적  점선=가이던스',
          font:{{size:10, color:'#888'}}
        }}],
        legend:{{orientation:'h', y:-0.28, font:{{size:10}}}},
        margin:{{t:60, r:25, b:62, l:60}}
      }}), {{displayModeBar:false, responsive:true}});
    }} catch(e) {{
      console.error('eps2 plot', ticker, e);
      showEmpty(id, d.name + ' — 렌더링 실패');
    }}
  }});
}})();

// ════════════════════════════════════════════════════════
// EPS 탭 — 종목별 분기 EPS vs 주가 (Base 100, 종목별 고유 색상)
// ════════════════════════════════════════════════════════
(function() {{
  // 종목 순서
  var EPS_ORDER = [
    '005930','009150','353200','000660',
    '267260','062040','298040','010120',
    'SNDK','MU','STX','GOOGL',
    'NVDA','MSFT','AVGO','AMD',
    'INTC','QCOM','ARM'
  ];

  // 종목별 고유 색상 {{ price: 진한색, eps: 연한색 }}
  var C = {{
    '005930':{{p:'#0A2A6E',e:'#7B9FD4'}},'009150':{{p:'#B22222',e:'#E89090'}},
    '353200':{{p:'#1A7A4A',e:'#7DC0A0'}},'000660':{{p:'#6A0DAD',e:'#BF92CB'}},
    '267260':{{p:'#CC5500',e:'#EBA87A'}},'062040':{{p:'#1A6B8A',e:'#7ABFDA'}},
    '298040':{{p:'#7B4F00',e:'#C4906B'}},'010120':{{p:'#2E4057',e:'#8AA0B5'}},
    'SNDK':{{p:'#E06B00',e:'#F5C38A'}},'MU':{{p:'#007A7A',e:'#7DE4D0'}},
    'STX':{{p:'#A00000',e:'#E08080'}},'GOOGL':{{p:'#1A56DB',e:'#9CB8FB'}},
    'NVDA':{{p:'#5A8A00',e:'#B8DB7A'}},'MSFT':{{p:'#0078D4',e:'#7BD1F7'}},
    'AVGO':{{p:'#990000',e:'#E87878'}},'AMD':{{p:'#C00020',e:'#F08090'}},
    'INTC':{{p:'#0071C5',e:'#80B8E2'}},'QCOM':{{p:'#3253DC',e:'#9AAEED'}},
    'ARM':{{p:'#2B3F6C',e:'#8D9DC0'}},
  }};

  var basket = D.eps_basket || {{}};

  EPS_ORDER.forEach(function(ticker) {{
    var id  = 'eps_' + ticker;
    var el  = document.getElementById(id);
    if (!el) return;

    var d = basket[ticker];
    if (!d) {{ showEmpty(id, ticker + ' — 데이터 수집 중'); return; }}

    var col = C[ticker] || {{p:'#333', e:'#999'}};
    var hasPx  = hasValues(d.price_vals);
    var hasEPS = hasValues(d.eps_vals);

    if (!hasPx && !hasEPS) {{
      showEmpty(id, d.name + ' — 데이터 없음'); return;
    }}

    var traces = [];
    if (hasPx) traces.push({{
      x: d.price_dates, y: d.price_vals,
      type:'scatter', mode:'lines', name: d.name + ' 주가',
      connectgaps:true,
      line:{{color: col.p, width:2.6}},
      hovertemplate:'%{{x}}<br>주가 Base100 %{{y:.1f}}<extra></extra>'
    }});
    if (hasEPS) traces.push({{
      x: d.eps_dates, y: d.eps_vals,
      type:'scatter', mode:'lines', name: d.name + ' 분기EPS',
      connectgaps:true,
      line:{{color: col.e, width:2.0}},
      hovertemplate:'%{{x}}<br>EPS Base100 %{{y:.1f}}<extra></extra>'
    }});

    // 마지막 Base100 값
    function lastV(vals) {{
      if (!vals||!vals.length) return null;
      for(var i=vals.length-1;i>=0;i--) if(vals[i]!==null&&vals[i]!==undefined) return vals[i];
      return null;
    }}
    var pxLast  = lastV(d.price_vals);
    var epsLast = lastV(d.eps_vals);
    var epsRaw  = (d.eps_raw_latest!==undefined&&d.eps_raw_latest!==null) ? d.eps_raw_latest : null;

    // 제목
    var titleTxt = d.name + ' — 주가 vs EPS';
    if (pxLast!==null)  titleTxt += '  주가 ' + pxLast.toFixed(1);
    if (epsLast!==null) titleTxt += '  EPS ' + epsLast.toFixed(1);
    if (epsRaw!==null&&epsRaw<0) titleTxt += ' ▼적자구간포함';

    // Base 100 기준선 + 적자(EPS<100) 구간 음영
    var shapes = [{{
      type:'line', xref:'paper', x0:0, x1:1,
      yref:'y', y0:100, y1:100,
      line:{{color:'#ccc', width:1, dash:'dot'}}
    }}];
    if (hasEPS && d.eps_dates && d.eps_vals) {{
      var inLoss=false, lossStart=null;
      for(var i=0;i<d.eps_vals.length;i++) {{
        var v=d.eps_vals[i];
        if(v!==null&&v<100&&!inLoss){{ inLoss=true; lossStart=d.eps_dates[i]; }}
        else if((v===null||v>=100)&&inLoss) {{
          inLoss=false;
          shapes.push({{type:'rect',xref:'x',yref:'paper',
            x0:lossStart,x1:d.eps_dates[i-1]||d.eps_dates[i],y0:0,y1:1,
            fillcolor:'rgba(220,38,38,0.06)',line:{{width:0}}}});
        }}
      }}
      if(inLoss&&lossStart) shapes.push({{type:'rect',xref:'x',yref:'paper',
        x0:lossStart,x1:d.eps_dates[d.eps_dates.length-1],y0:0,y1:1,
        fillcolor:'rgba(220,38,38,0.06)',line:{{width:0}}}});
    }}

    try {{
      Plotly.newPlot(id, traces, Object.assign({{}}, base, {{
        title:{{text:titleTxt, font:{{size:12, color: col.p}}}},
        yaxis:{{title:"Base 100  ('25/01=100)", gridcolor:'#F3F4F6', zeroline:false}},
        xaxis:{{gridcolor:'#F3F4F6'}},
        shapes: shapes,
        legend:{{orientation:'h', y:-0.26, font:{{size:11}}}},
        margin:{{t:58, r:25, b:58, l:60}}
      }}), {{displayModeBar:false, responsive:true}});
    }} catch(e) {{
      console.error('eps plot error', ticker, e);
      showEmpty(id, d.name + ' — 렌더링 실패');
    }}
  }});
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
