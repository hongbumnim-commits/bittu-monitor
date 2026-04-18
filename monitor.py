"""
빚투 모니터 v5.3 - 깃허브 액션(Cloud) 생존 특화 버전

수정 사항:
  - Pandas 경고(FutureWarning) 문법 수정 (pct_change, concat)
  - 깃허브 IP 차단 대비 네이버 우회 스크래핑 전면 부활 (최후의 보루)
  - pykrx/stooq/yfinance 블락 시 자동 우회
"""

import os
import json
import time
import datetime as dt
import re
from pathlib import Path
from io import StringIO, BytesIO

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

try:
    import yfinance as yf
except ImportError:
    print("[Notice] yfinance가 설치되지 않았습니다.")
try:
    from pykrx import stock
except ImportError:
    print("[Notice] pykrx가 설치되지 않았습니다.")

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

# --- 공통 유틸 ---
def safe(fn_name, fn, default=None, retries=3, sleep=2):
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"[warn] {fn_name} attempt {i+1} failed: {e}")
            time.sleep(sleep)
    return default

# --- 금융 데이터 수집 ---
def fetch_fdr(symbol, name=None, days=LOOKBACK_DAYS):
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    end = TODAY.strftime("%Y-%m-%d")
    df = fdr.DataReader(symbol, start, end)
    if df.empty: return pd.Series(dtype=float, name=name or symbol)
    s = df["Close"]
    s.index = pd.to_datetime(s.index).date
    return s.rename(name or symbol)

def fetch_stooq_series(symbol, name=None, days=LOOKBACK_DAYS):
    urls = [f"https://stooq.com/q/d/l/?s={symbol}&i=d"]
    headers = {"User-Agent": "Mozilla/5.0"}
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200 or "<html" in r.text.lower()[:50]: 
                continue # 봇 차단 방어
            df = pd.read_csv(StringIO(r.text))
            if "Date" not in df.columns: continue
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date", "Close"])
            df = df[df["Date"].dt.date >= TODAY - dt.timedelta(days=days)]
            s = pd.Series(df["Close"].values, index=df["Date"].dt.date, name=name or symbol)
            return s
        except Exception: pass
    return pd.Series(dtype=float, name=name or symbol)

def fetch_fred_series(series_id, name=None, days=LOOKBACK_DAYS * 3):
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        df = fdr.DataReader(f"FRED:{series_id}", start)
        if not df.empty:
            s = pd.to_numeric(df.iloc[:, 0], errors="coerce").dropna()
            s.index = pd.to_datetime(s.index).date
            return s.rename(name or series_id)
    except Exception: pass
    return pd.Series(dtype=float, name=name or series_id)

def fetch_ust10y_fred():
    return fetch_fred_series("DGS10", "ust10y", LOOKBACK_DAYS)

def fetch_cor1m():
    try:
        r = requests.get("https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_History.csv", headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        df = pd.read_csv(StringIO(r.text))
        date_col = next(c for c in df.columns if "date" in str(c).lower())
        close_col = next((c for c in df.columns if "close" in str(c).lower()), df.columns[-1])
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["_close"] = pd.to_numeric(df[close_col], errors="coerce")
        df = df.dropna(subset=[date_col, "_close"])
        return pd.Series(df["_close"].values, index=df[date_col].dt.date.values, name="cor1m")
    except Exception: return pd.Series(dtype=float, name="cor1m")

def fetch_vkospi():
    """야후 -> 네이버 -> Stooq 순서로 끈질기게 수집"""
    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
    
    # 1. 야후 (최근 404 에러 잦음)
    try:
        ticker = yf.Ticker("^KSVKOSPI")
        hist = ticker.history(period=f"{LOOKBACK_DAYS}d")
        if not hist.empty:
            s = hist["Close"].dropna()
            s.index = s.index.date
            s = s[s.index >= cutoff]
            if not s.empty and 3 <= s.iloc[-1] <= 200:
                return s.rename("vkospi")
    except Exception: pass

    # 2. FDR (내부적으로 야후/인베스팅 사용)
    s = fetch_fdr("VKOSPI", "vkospi")
    if not s.empty and 3 <= s.iloc[-1] <= 200: return s

    # 3. 네이버 최후의 보루 (스팟값)
    url = "https://finance.naver.com/sise/sise_index.naver?code=VKOSPI"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.encoding = "euc-kr"
        soup = BeautifulSoup(r.text, "html.parser")
        elem = soup.select_one("#now_value")
        if elem: 
            v = float(elem.text.replace(",", "").strip())
            return pd.Series({TODAY: v}, name="vkospi")
    except Exception: pass

    # 4. Stooq (봇 차단 잦음)
    return fetch_stooq_series("^vkospi", "vkospi")

def fetch_foreign_flow_combined():
    """pykrx -> 네이버 순으로 수집 (깃허브 액션 차단 방어)"""
    # 1. pykrx
    try:
        result = {}
        start_str = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
        end_str = TODAY.strftime("%Y%m%d")
        for mkt in ["KOSPI", "KOSDAQ"]:
            df = stock.get_market_trading_value_by_date(start_str, end_str, mkt)
            if "외국인" in df.columns:
                for date_idx, row in df.iterrows():
                    d = date_idx.date()
                    result[d] = result.get(d, 0.0) + (float(row["외국인"]) / 1e8)
            time.sleep(1)
        if result: return {d: round(v, 1) for d, v in result.items()}
    except Exception as e:
        print(f"  [warn] pykrx block detected: {e}")

    # 2. 네이버 (pykrx 막혔을 때 우회)
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://finance.naver.com/"}
    result = {}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.encoding = "euc-kr"
        tables = pd.read_html(StringIO(r.text))
        for t in tables:
            flat = " ".join([str(c) for c in t.columns]) + " " + " ".join([str(x) for x in t.values.flatten()[:50]])
            if "외국인" in flat and ("날짜" in flat or "일자" in flat):
                for row in t.itertuples(index=False):
                    vals = [str(v) for v in row]
                    d_match = next((v for v in vals if re.search(r"\d{2,4}[./-]\d{1,2}[./-]\d{1,2}", v)), None)
                    if d_match:
                        y, m, d = re.search(r"(\d{2,4})[./-](\d{1,2})[./-](\d{1,2})", d_match).groups()
                        date_val = dt.date(int("20"+y if len(y)==2 else y), int(m), int(d))
                        nums = [int(v.replace(",", "").replace("+", "")) for v in vals if re.match(r"^-?[\d,]+$", v)]
                        if len(nums) >= 3: result[date_val] = nums[1]
    except Exception: pass
    return result

DATA_GO_KR_BASE = "https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService"
def _data_go_kr_fetch(endpoint, extra_params=None):
    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key: return []
    url = f"{DATA_GO_KR_BASE}/{endpoint}"
    params = {"serviceKey": api_key, "resultType": "json", "numOfRows": 200, "pageNo": 1}
    if extra_params: params.update(extra_params)
    try:
        r = requests.get(url, params=params, timeout=30)
        return r.json()["response"]["body"]["items"]["item"]
    except Exception: return []

def fetch_credit_balance():
    start = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    items = _data_go_kr_fetch("getGrantingOfCreditBalanceInfo", {"beginBasDt": start, "endBasDt": TODAY.strftime("%Y%m%d")})
    if isinstance(items, dict): items = [items]
    res = {}
    for it in items:
        try: res[dt.datetime.strptime(str(it.get("basDt")), "%Y%m%d").date()] = int(float(it.get("crdTrFingWhl")) / 100)
        except: pass
    return res

def fetch_securities_market_capital():
    start = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    items = _data_go_kr_fetch("getSecuritiesMarketTotalCapitalInfo", {"beginBasDt": start, "endBasDt": TODAY.strftime("%Y%m%d")})
    if isinstance(items, dict): items = [items]
    forced = {}
    for it in items:
        try:
            if it.get("brkTrdUcolMnyVsOppsTrdAmt"):
                forced[dt.datetime.strptime(str(it.get("basDt")), "%Y%m%d").date()] = int(float(it.get("brkTrdUcolMnyVsOppsTrdAmt")) / 1e8)
        except: pass
    return {"forced_sale": forced}

def fetch_sector_basket(tickers):
    closes = []
    for t in tickers:
        s = safe(f"ticker_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=LOOKBACK_DAYS), default=pd.Series(dtype=float))
        if not s.empty: closes.append(s)
    if not closes: return pd.Series(dtype=float)
    df = pd.concat(closes, axis=1).ffill().bfill()
    first = df.iloc[0]
    valid_cols = first[first > 0].index
    if len(valid_cols) == 0: return pd.Series(dtype=float)
    return (df[valid_cols].div(first[valid_cols]) * 100).mean(axis=1)

def fetch_m7_plus_basket():
    stocks = {"SP500": "US500", "IXIC": "IXIC", "AAPL": "AAPL", "MSFT": "MSFT", "GOOGL": "GOOGL", "AMZN": "AMZN", "META": "META", "NVDA": "NVDA", "TSLA": "TSLA", "AVGO": "AVGO", "TSM": "TSM"}
    result = {}
    for k, v in stocks.items():
        s = safe(f"m7_{k}", lambda sym=v, nm=k: fetch_fdr(sym, nm, days=LOOKBACK_DAYS), default=pd.Series(dtype=float))
        if not s.empty: result[k] = s
    return result

MAIN_COLS = ["date", "kospi", "kosdaq", "samsung", "hynix", "vkospi", "credit_balance_eok", "forced_sale_eok", "foreign_net_eok", "samsung_ret_pct", "hynix_ret_pct", "sp500", "nasdaq", "vix", "nvda", "ust10y", "cor1m", "sec_반도체", "sec_방산조선", "sec_바이오", "sec_2차전지", "sec_금융"]

def load_history():
    fp = DATA_DIR / "history.csv"
    if not fp.exists(): return pd.DataFrame(columns=MAIN_COLS)
    df = pd.read_csv(fp)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def save_history(df):
    fp = DATA_DIR / "history.csv"
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df.to_csv(fp, index=False)
    return df

def update_data():
    print(f"[{TODAY}] 데이터 수집 시작 (GitHub Actions 호환 모드)...")
    
    kospi = safe("kospi", lambda: fetch_fdr("KS11", "kospi"))
    kosdaq = safe("kosdaq", lambda: fetch_fdr("KQ11", "kosdaq"))
    samsung = safe("samsung", lambda: fetch_fdr("005930", "samsung"))
    hynix = safe("hynix", lambda: fetch_fdr("000660", "hynix"))
    sp500 = safe("sp500", lambda: fetch_fdr("US500", "sp500"))
    nasdaq = safe("nasdaq", lambda: fetch_fdr("IXIC", "nasdaq"))
    vix = safe("vix", lambda: fetch_fdr("VIX", "vix"))
    nvda = safe("nvda", lambda: fetch_fdr("NVDA", "nvda"))
    ust10y = safe("ust10y", fetch_ust10y_fred)
    cor1m = safe("cor1m", fetch_cor1m)
    
    sectors = {f"sec_{k}": fetch_sector_basket(v) for k, v in SECTOR_BASKETS.items()}
    
    foreign_flow = safe("foreign_flow", fetch_foreign_flow_combined, default={})
    vkospi_series = safe("vkospi", fetch_vkospi, default=pd.Series(dtype=float))
    credit_map = safe("credit", fetch_credit_balance, default={})
    market_cap = safe("market_cap", fetch_securities_market_capital, default={"forced_sale": {}})
    
    # concat 경고 해결: 빈 DataFrame 필터링
    series_list = [kospi, kosdaq, samsung, hynix, sp500, nasdaq, vix, nvda, ust10y, cor1m] + list(sectors.values())
    valid_series = [s for s in series_list if not s.empty]
    
    df = pd.concat(valid_series, axis=1) if valid_series else pd.DataFrame()
    df = df.reset_index().rename(columns={"index": "date"})
    
    df["credit_balance_eok"] = df["date"].map(credit_map)
    df["forced_sale_eok"] = df["date"].map(market_cap.get("forced_sale", {}))
    df["foreign_net_eok"] = df["date"].map(foreign_flow)
    
    if isinstance(vkospi_series, pd.Series) and not vkospi_series.empty:
        df["vkospi"] = df["date"].map(vkospi_series)
    else:
        df["vkospi"] = np.nan
    
    # pct_change 경고 해결 (fill_method=None 명시)
    if not df.empty and "samsung" in df.columns:
        df["samsung_ret_pct"] = df["samsung"].pct_change(fill_method=None) * 100
        df["hynix_ret_pct"] = df["hynix"].pct_change(fill_method=None) * 100
    
    history = load_history()
    
    # concat 경고 해결 2: all-NA 필터링
    frames_to_concat = [x.dropna(how='all', axis=1) for x in [history, df] if not x.empty]
    combined = save_history(pd.concat(frames_to_concat, ignore_index=True) if frames_to_concat else pd.DataFrame(columns=MAIN_COLS))
    
    extras = {"us_margin_debt": {}, "m7_basket": fetch_m7_plus_basket(), "kr_foreign_holding": {}, "global_liquidity": pd.Series()}
    return combined, extras

def main():
    try:
        df, extras = update_data()
        print("모니터 업데이트 완료.")
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()
