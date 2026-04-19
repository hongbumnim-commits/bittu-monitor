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
import FinanceDataReader as fdr
from io import StringIO

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


# 전력주 바스켓 (누적 추세용)
KR_POWER_STOCKS = {
    "KS11":   "코스피",
    "KQ11":   "코스닥",
    "062040": "산일전기",
    "010120": "LS일렉트릭",
    "298040": "효성중공업",
    "267260": "HD현대일렉트릭",
}

# 공공데이터포털 서비스키 (data.go.kr) - GitHub Secret: DATA_GO_KR_API_KEY
DATA_GO_KR_KEY = os.environ.get("DATA_GO_KR_API_KEY", "")
KOFIA_BASE = "https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService"
MARKET_INDEX_BASE = "https://apis.data.go.kr/1160100/service/GetMarketIndexInfoService"


def safe(fn_name, fn, default=None, retries=1, sleep=0):
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"[warn] {fn_name}: {e}")
            if sleep:
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
    """CBOE CDN에서 COR1M 1회 시도. 실패하면 빈 Series."""
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_History.csv"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200 or len(r.text) < 50:
            return pd.Series(dtype=float, name="cor1m")
        lines = r.text.splitlines()
        header_idx = next((i for i, l in enumerate(lines[:10]) if re.search(r"date", l, re.IGNORECASE)), None)
        if header_idx is None:
            return pd.Series(dtype=float, name="cor1m")
        from io import StringIO
        df_c = pd.read_csv(StringIO("\n".join(lines[header_idx:])))
        date_col  = next((c for c in df_c.columns if re.search(r"date", str(c), re.IGNORECASE)), None)
        close_col = next((c for c in df_c.columns if "close" in str(c).lower() or str(c).lower() == "value"), None)
        if close_col is None:
            other = [c for c in df_c.columns if c != date_col]
            close_col = other[-1] if other else None
        if date_col is None or close_col is None:
            return pd.Series(dtype=float, name="cor1m")
        df_c[date_col] = pd.to_datetime(df_c[date_col], errors="coerce")
        df_c["_c"] = pd.to_numeric(df_c[close_col], errors="coerce")
        df_c = df_c.dropna(subset=[date_col, "_c"])
        cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
        df_c = df_c[df_c[date_col].dt.date >= cutoff]
        if df_c.empty:
            return pd.Series(dtype=float, name="cor1m")
        s = pd.Series(df_c["_c"].values, index=df_c[date_col].dt.date.values, name="cor1m")
        last = float(s.iloc[-1])
        if 1 < last < 100:
            print(f"  cor1m via cboe-cdn: {len(s)} rows, latest {last:.2f}")
            return s
    except Exception as e:
        print(f"  [warn] cor1m: {e}")
    return pd.Series(dtype=float, name="cor1m")


def fetch_sector_basket(tickers):
    """섹터 종목 바스켓을 Base 100 누적 추세로 반환 (병렬)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    closes = []

    def _f(t):
        try:
            return fetch_fdr(t, name=t, days=LOOKBACK_DAYS)
        except Exception:
            return pd.Series(dtype=float)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(_f, t) for t in tickers]
        for fut in as_completed(futs):
            s = fut.result()
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

def fetch_kr_power_basket():
    """전력주 개별 종목 누적 추세 — 병렬 fetch."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    result = {}

    def _fetch_one(ticker, name):
        try:
            s = fetch_fdr(ticker, name, days=LOOKBACK_DAYS)
            return ticker, s if not s.empty else pd.Series(dtype=float)
        except Exception as e:
            print(f"  [warn] kr_power_{ticker}: {e}")
            return ticker, pd.Series(dtype=float)

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, t, n): t for t, n in KR_POWER_STOCKS.items()}
        for fut in as_completed(futures):
            ticker, s = fut.result()
            if not s.empty:
                result[ticker] = s.rename(ticker)

    print(f"  kr_power basket: {len(result)} tickers")
    return result


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
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
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
    """M7 + AVGO + TSM 병렬 fetch."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    result = {}

    def _fetch_one(ticker):
        src_sym = "US500" if ticker == "SP500" else ticker
        try:
            s = fetch_fdr(src_sym, ticker, days=LOOKBACK_DAYS)
            return ticker, s if not s.empty else pd.Series(dtype=float)
        except Exception as e:
            print(f"  [warn] m7_{ticker}: {e}")
            return ticker, pd.Series(dtype=float)

    with ThreadPoolExecutor(max_workers=11) as ex:
        futures = {ex.submit(_fetch_one, t): t for t in M7_PLUS_STOCKS.keys()}
        for fut in as_completed(futures):
            ticker, s = fut.result()
            if not s.empty:
                result[ticker] = s.rename(ticker)

    print(f"  m7+ basket: {len(result)} tickers")
    return result


# ================================================================
# 한국 외국인 주식 보유금액 & 비중 - index.go.kr (월별)
# ================================================================

# ================================================================
# ================================================================
# data.go.kr 지수시세 - VKOSPI / 원달러 / 코스피 거래대금 + ESG
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
                             params=params, timeout=10)
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


MAIN_COLS = [
    "date",
    "kospi", "kosdaq", "samsung", "hynix",
    "credit_balance_eok",
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
    credit_map  = safe("credit",        fetch_credit_balance,   default={})

    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix,
        "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y,
        "cor1m": cor1m,
        **sectors
    }
    df = pd.DataFrame(series_dict)
    df = df.reset_index().rename(columns={"index": "date"})
    df["samsung_ret_pct"] = df["samsung"].pct_change(fill_method=None) * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change(fill_method=None) * 100
    df["credit_balance_eok"] = None
    df["vkospi"]            = None
    df["usdkrw"]            = None
    df["kospi_trprc"]       = None

    # 신용공여잔고
    for d, v in credit_map.items():
        if d in df["date"].values:
            df.loc[df["date"] == d, "credit_balance_eok"] = v
    # VKOSPI / 원달러 / 코스피 거래대금
        for d, v in kr_idx.get(src, {}).items():
            if d in df["date"].values:
                df.loc[df["date"] == d, col] = v

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
    kr_power_basket = safe("kr_power", fetch_kr_power_basket, default={})

    extras = {
        "us_margin_debt": us_margin_debt,         # {date: bil_usd}  월별
        "m7_basket": m7_basket,
        "kr_power_basket": kr_power_basket,
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
