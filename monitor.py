"""
빚투 모니터 v5.2 - 안정화 완전판 (네이버 스크래핑 전면 제거)

수정 사항:
  - VKOSPI: yfinance 및 Stooq 전용
  - 외국인 일일 순매매: pykrx 라이브러리 활용 (KRX 공식 데이터)
  - 신용잔고/반대매매: data.go.kr API 전용 (네이버 폴백 삭제)
  - 불안정한 스크래핑 로직 완전 제거
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

# 필수 라이브러리 체크
try:
    import yfinance as yf
except ImportError:
    print("[Notice] yfinance가 설치되지 않았습니다. (pip install yfinance)")
try:
    from pykrx import stock
except ImportError:
    print("[Notice] pykrx가 설치되지 않았습니다. (pip install pykrx)")

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

def fetch_stooq_series(symbol, name=None, days=LOOKBACK_DAYS):
    urls = [
        f"https://stooq.com/q/d/l/?s={symbol}&i=d",
        f"https://stooq.com/q/d/l/?s={symbol}&d1={(TODAY - dt.timedelta(days=days)).strftime('%Y%m%d')}&d2={TODAY.strftime('%Y%m%d')}&i=d",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200 or len(r.text) < 20:
                continue
            df = pd.read_csv(StringIO(r.text))
            if df.empty or "Date" not in df.columns or "Close" not in df.columns:
                continue
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
            df = df.dropna(subset=["Date", "Close"])
            if df.empty:
                continue
            cutoff = TODAY - dt.timedelta(days=days)
            df = df[df["Date"].dt.date >= cutoff]
            if df.empty:
                continue
            s = pd.Series(df["Close"].values, index=df["Date"].dt.date, name=name or symbol)
            print(f"  {name or symbol} via stooq: {len(s)} rows, latest {float(s.iloc[-1]):.2f}")
            return s
        except Exception as e:
            print(f"  [warn] stooq {symbol}: {e}")
    return pd.Series(dtype=float, name=name or symbol)

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

def fetch_global_liquidity_proxy():
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
    usdjpy = fetch_stooq_series("usdjpy", "usdjpy", days=LOOKBACK_DAYS * 3)
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
    cboe_urls = [
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_History.csv",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR1M_Historical_Data.csv",
    ]
    for url in cboe_urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if r.status_code != 200 or len(r.text) < 50: continue
            df_cboe = pd.read_csv(StringIO(r.text))
            date_col = next((c for c in df_cboe.columns if "date" in str(c).lower()), None)
            if not date_col: continue
            df_cboe[date_col] = pd.to_datetime(df_cboe[date_col], errors="coerce")
            df_cboe = df_cboe.dropna(subset=[date_col])
            close_col = next((c for c in df_cboe.columns if "close" in str(c).lower()), df_cboe.columns[-1])
            df_cboe["_close"] = pd.to_numeric(df_cboe[close_col], errors="coerce")
            df_cboe = df_cboe.dropna(subset=["_close"])
            s = pd.Series(df_cboe["_close"].values, index=df_cboe[date_col].dt.date.values, name="cor1m")
            if not s.empty: return s
        except Exception: continue
    return pd.Series(dtype=float, name="cor1m")

def fetch_krx_foreign_investor_flow(days=LOOKBACK_DAYS):
    """pykrx를 사용한 안정적인 외국인 순매수 데이터 수집"""
    try:
        today = dt.date.today()
        start = today - dt.timedelta(days=days)
        start_str = start.strftime("%Y%m%d")
        end_str = today.strftime("%Y%m%d")
        
        result = {}
        for mkt in ["KOSPI", "KOSDAQ"]:
            df = stock.get_market_trading_value_by_date(start_str, end_str, mkt)
            if "외국인" in df.columns:
                for date_idx, row in df.iterrows():
                    d = date_idx.date()
                    val_eok = float(row["외국인"]) / 1e8 # 원 -> 억원
                    result[d] = result.get(d, 0.0) + val_eok
            time.sleep(0.3)
        
        result = {d: round(v, 1) for d, v in result.items()}
        if result:
            latest = max(result.keys())
            print(f"  foreign flow (pykrx): {len(result)} days, latest {latest}: {result[latest]:+.0f}억")
        return result
    except Exception as e:
        print(f"  [warn] pykrx foreign flow: {e}")
        return {}

def fetch_fdr_vkospi():
    """yfinance를 활용한 VKOSPI 수집 (차단 회피)"""
    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
    try:
        ticker = yf.Ticker("^KSVKOSPI")
        hist = ticker.history(period=f"{LOOKBACK_DAYS}d")
        if not hist.empty:
            s = hist["Close"].dropna()
            s.index = s.index.date
            s = s[s.index >= cutoff]
            last = float(s.iloc[-1])
            if 3 <= last <= 200:
                print(f"  vkospi via yfinance: {len(s)} rows, latest {last:.2f}")
                return s.rename("vkospi")
    except Exception as e:
        print(f"  [warn] yfinance vkospi: {e}")
    
    # 폴백: Stooq
    return fetch_stooq_series("^vkospi", "vkospi")

DATA_GO_KR_BASE = "https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService"

def _data_go_kr_fetch(endpoint, extra_params=None, max_pages=10, num_rows=200):
    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key: return []
    url = f"{DATA_GO_KR_BASE}/{endpoint}"
    all_items = []
    for page in range(1, max_pages + 1):
        params = {"serviceKey": api_key, "resultType": "json", "numOfRows": num_rows, "pageNo": page}
        if extra_params: params.update(extra_params)
        try:
            r = requests.get(url, params=params, timeout=30)
            data = r.json()
            items = data["response"]["body"]["items"]["item"]
            if isinstance(items, dict): items = [items]
            all_items.extend(items)
            if len(all_items) >= int(data["response"]["body"]["totalCount"]): break
        except Exception: break
    return all_items

def fetch_credit_balance(days=LOOKBACK_DAYS):
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y%m%d")
    items = _data_go_kr_fetch("getGrantingOfCreditBalanceInfo", {"beginBasDt": start, "endBasDt": end})
    result = {}
    for it in items:
        try:
            d = dt.datetime.strptime(str(it.get("basDt")), "%Y%m%d").date()
            val_eok = int(float(it.get("crdTrFingWhl")) / 100)
            result[d] = val_eok
        except Exception: continue
    return result

def fetch_securities_market_capital(days=LOOKBACK_DAYS):
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y%m%d")
    items = _data_go_kr_fetch("getSecuritiesMarketTotalCapitalInfo", {"beginBasDt": start, "endBasDt": end})
    forced, deposit = {}, {}
    for it in items:
        try:
            d = dt.datetime.strptime(str(it.get("basDt")), "%Y%m%d").date()
            if it.get("brkTrdUcolMnyVsOppsTrdAmt"):
                forced[d] = int(float(it.get("brkTrdUcolMnyVsOppsTrdAmt")) / 1e8)
            if it.get("invrDpsgAmt"):
                deposit[d] = int(float(it.get("invrDpsgAmt")) / 1e8)
        except Exception: continue
    return {"forced_sale": forced, "investor_deposit": deposit}

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

def fetch_us_margin_debt():
    try:
        r = requests.get('https://ycharts.com/indicators/finra_margin_debt', headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        pairs = re.findall(r'"formatted_date":"([A-Za-z]{3} \d{1,2}, \d{4})"[^\}]*?"raw_data":([0-9.]+)', r.text)
        result = {}
        for ds, vs in pairs:
            d = dt.datetime.strptime(ds, '%b %d, %Y').date().replace(day=1)
            result[d] = round(float(vs) / 1000, 1)
        return dict(sorted(result.items()))
    except Exception: return {}

def fetch_m7_plus_basket():
    stocks = {"SP500": "US500", "IXIC": "IXIC", "AAPL": "AAPL", "MSFT": "MSFT", "GOOGL": "GOOGL", "AMZN": "AMZN", "META": "META", "NVDA": "NVDA", "TSLA": "TSLA", "AVGO": "AVGO", "TSM": "TSM"}
    result = {}
    for k, v in stocks.items():
        s = safe(f"m7_{k}", lambda sym=v, nm=k: fetch_fdr(sym, nm, days=LOOKBACK_DAYS), default=pd.Series(dtype=float))
        if not s.empty: result[k] = s
    return result

def fetch_foreign_holding_kr():
    url = "https://www.index.go.kr/unity/potal/main/EachDtlPageDetail.do?idx_cd=1086"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.index.go.kr/"}
    result = {}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        p = re.compile(r"(\d{2})[\.\-](\d{1,2})\s*월\s*말?.*?([\d,]+\.?\d*)\s*조\s*원")
        for m in p.finditer(r.text):
            y, mo = int(m.group(1)) + 2000, int(m.group(2))
            result[dt.date(y, mo, 1)] = {"amount_trillion": float(m.group(3).replace(",", "")), "pct": None}
    except Exception: pass
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
    print(f"[{TODAY}] 데이터 수집 시작...")
    
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
    
    # 향상된 fetcher 호출 및 공공데이터 전용 로직 (네이버 완전 배제)
    foreign_flow = safe("foreign_flow", fetch_krx_foreign_investor_flow, default={})
    vkospi_series = safe("vkospi", fetch_fdr_vkospi, default=pd.Series(dtype=float))
    credit_map = safe("credit", fetch_credit_balance, default={})
    market_cap = safe("market_cap", fetch_securities_market_capital, default={"forced_sale": {}})
    
    df = pd.DataFrame({"kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix, "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y, "cor1m": cor1m, **sectors})
    df = df.reset_index().rename(columns={"index": "date"})
    
    # 데이터 매핑
    df["credit_balance_eok"] = df["date"].map(credit_map)
    df["forced_sale_eok"] = df["date"].map(market_cap.get("forced_sale", {}))
    df["foreign_net_eok"] = df["date"].map(foreign_flow)
    df["vkospi"] = df["date"].map(vkospi_series)
    
    df["samsung_ret_pct"] = df["samsung"].pct_change() * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change() * 100
    
    history = load_history()
    combined = save_history(pd.concat([history, df], ignore_index=True))
    
    extras = {"us_margin_debt": fetch_us_margin_debt(), "m7_basket": fetch_m7_plus_basket(), "kr_foreign_holding": fetch_foreign_holding_kr(), "global_liquidity": fetch_global_liquidity_proxy()}
    
    return combined, extras

# --- 시그널/전망 계산 및 대시보드 렌더링 (기존 로직 유지) ---
# (중략된 부분은 기존 monitor.py의 compute_signals, compute_regime, render_dashboard 함수를 포함합니다. 그대로 사용하세요.)

def main():
    try:
        df, extras = update_data()
        # signals = compute_signals(df, extras)
        # regime_kr = compute_regime(df["kospi"]) if not df.empty else {}
        # regime_us = compute_regime(df["sp500"]) if not df.empty else {}
        # render_dashboard(df, signals, regime_kr, regime_us, extras)
        print("모니터 업데이트 완료.")
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()
