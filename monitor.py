"""
빚투 모니터 v5 - 추세 전망 + 에러 수정

v4 에러 수정:
  - VKOSPI: FinanceDataReader로 안 됨 → 네이버 스크래핑
  - US 10Y: 야후 차트 경로 변경 → FRED API 사용

신규: 시장 추세 전망 (Regime Analysis)
  단기 1~3개월 / 중기 3~6개월 / 장기 6~12개월 각각 강세/중립/약세 판정
"""

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


M7_PLUS_STOCKS = {
    "SP500": "S&P500",
    "IXIC": "나스닥",
    "AAPL": "애플",
    "MSFT": "마이크로소프트",
    "GOOGL": "구글",
    "AMZN": "아마존",
    "META": "메타",
    "NVDA": "엔비디아",
    "TSLA": "테슬라",
    "AVGO": "브로드컴",
    "TSM": "TSMC",
}

GLOBAL_LIQUIDITY_SERIES = {
    "WALCL": "fed_assets",
    "ECBASSETSW": "ecb_assets",
    "JPNASSETS": "boj_assets",
}


def fetch_stooq_series(symbol, name=None, days=LOOKBACK_DAYS):
    """Stooq CSV fallback. 예: usdjpy -> USD/JPY"""
    from io import StringIO
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


def fetch_m7_plus_basket():
    result = {}
    for ticker in M7_PLUS_STOCKS.keys():
        source_symbol = "US500" if ticker == "SP500" else ticker
        if ticker == "IXIC":
            source_symbol = "IXIC"
        s = safe(f"m7_{ticker}", lambda ts=source_symbol, nm=ticker: fetch_fdr(ts, nm, days=LOOKBACK_DAYS), default=pd.Series(dtype=float))
        if not s.empty:
            result[ticker] = s.rename(ticker)
    print(f"  m7+ basket: {len(result)} tickers")
    return result


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
    usdjpy = fetch_stooq_series("usdjpy", "usdjpy", days=LOOKBACK_DAYS * 3)
    if usdjpy.empty:
        for sym in ["USDJPY", "JPY=X"]:
            try:
                s = fetch_fdr(sym, "usdjpy", days=LOOKBACK_DAYS * 3)
                if not s.empty:
                    usdjpy = s.rename("usdjpy")
                    break
            except Exception:
                pass
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
    S&P 500 구성종목 간 내재상관 — '모두 같이 움직일 확률'을 옵션으로 측정.
    높음(60+) = 시스템 공포, 낮음(<20) = 쏠림 극한 (역설적 위험).
    """
    import urllib.parse

    # 1) FDR로 Yahoo ^COR1M 시도
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

    # 2) Yahoo chart API 직접 호출 (강화된 헤더)
    end_ts = int(dt.datetime.now(KST).timestamp())
    start_ts = end_ts - LOOKBACK_DAYS * 86400
    symbol = urllib.parse.quote("^COR1M")
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           f"?period1={start_ts}&period2={end_ts}&interval=1d")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finance.yahoo.com/quote/%5ECOR1M/",
        "Origin": "https://finance.yahoo.com",
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        dates = [dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).date() for t in timestamps]
        s = pd.Series(closes, index=dates, name="cor1m").dropna()
        if not s.empty:
            last = float(s.iloc[-1])
            print(f"  cor1m via yahoo: {len(s)} rows, latest {last:.2f}")
            return s
    except Exception as e:
        print(f"  [warn] yahoo cor1m: {e}")

    print(f"  [warn] cor1m: all sources failed")
    return pd.Series(dtype=float, name="cor1m")


def fetch_naver_deposit():
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    r = requests.get(url, headers=headers, timeout=20)
    r.encoding = "euc-kr"
    result = {"credit_balance_eok": None, "forced_sale_eok": None}
    try:
        tables = pd.read_html(r.text, encoding="euc-kr")
        for t in tables:
            t_str = t.astype(str)
            flat = " ".join([" ".join(row) for row in t_str.values.tolist()])
            if ("신용잔고" in flat or "신용공여" in flat) and result["credit_balance_eok"] is None:
                for row in t_str.values.tolist():
                    if "신용" in " ".join(row):
                        for cell in row:
                            m = re.search(r"([\d,]+)", str(cell))
                            if m:
                                v = int(m.group(1).replace(",", ""))
                                if v > 100000:
                                    result["credit_balance_eok"] = v
                                    break
                        if result["credit_balance_eok"]:
                            break
            if "반대매매" in flat and result["forced_sale_eok"] is None:
                for row in t_str.values.tolist():
                    if "반대매매" in " ".join(row):
                        for cell in row:
                            m = re.search(r"([\d,]+)", str(cell))
                            if m:
                                try:
                                    v = int(m.group(1).replace(",", ""))
                                    if 10 <= v <= 100000:
                                        result["forced_sale_eok"] = v
                                        break
                                except ValueError:
                                    continue
                        if result["forced_sale_eok"]:
                            break
    except Exception as e:
        print(f"  [warn] deposit read_html: {e}")
    if result["credit_balance_eok"] is None or result["forced_sale_eok"] is None:
        try:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            if result["credit_balance_eok"] is None:
                m = re.search(r"신용잔고[^\d]*([\d,]+)", text)
                if m:
                    v = int(m.group(1).replace(",", ""))
                    if v > 100000:
                        result["credit_balance_eok"] = v
            if result["forced_sale_eok"] is None:
                m = re.search(r"반대매매[^\d]*([\d,]+)", text)
                if m:
                    v = int(m.group(1).replace(",", ""))
                    if 10 <= v <= 100000:
                        result["forced_sale_eok"] = v
        except Exception as e:
            print(f"  [warn] deposit regex: {e}")
    print(f"  naver deposit: credit={result['credit_balance_eok']}, forced={result['forced_sale_eok']}")
    return result


def fetch_naver_foreign_flow():
    """[DEPRECATED] 네이버 투자자 매매동향 스크래핑 — fallback용으로 유지. 최신 경로는 fetch_krx_foreign_flow()."""
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    r.encoding = "euc-kr"
    result = {}
    try:
        tables = pd.read_html(r.text, encoding="euc-kr")
        for t in tables:
            cols = [str(c) for c in t.columns]
            flat = " ".join(cols) + " " + " ".join([str(x) for x in t.values.flatten().tolist()[:50]])
            if "외국인" in flat and ("날짜" in flat or "일자" in flat):
                for row in t.itertuples(index=False):
                    row_vals = [str(v) for v in row]
                    date_match = None
                    for v in row_vals:
                        m = re.search(r"(\d{2,4})[./-](\d{1,2})[./-](\d{1,2})", v)
                        if m:
                            y, mo, d = m.groups()
                            if len(y) == 2:
                                y = "20" + y
                            try:
                                date_match = dt.date(int(y), int(mo), int(d))
                                break
                            except Exception:
                                continue
                    if not date_match:
                        continue
                    numeric_vals = []
                    for v in row_vals:
                        v_clean = v.replace(",", "").replace("+", "").strip()
                        if re.match(r"^-?\d+$", v_clean):
                            numeric_vals.append(int(v_clean))
                    if len(numeric_vals) >= 3:
                        foreign_val = numeric_vals[1] if len(numeric_vals) >= 3 else numeric_vals[0]
                        result[date_match] = foreign_val
    except Exception as e:
        print(f"  [warn] foreign flow (naver): {e}")
    print(f"  foreign flow (naver): {len(result)} days")
    return result


def fetch_krx_foreign_flow(days=LOOKBACK_DAYS):
    """
    [DEPRECATED — pykrx 경로는 KRX 유료 로그인 필요로 비활성]
    사용하지 않음. 아래 data.go.kr 경로로 대체됨. 호환성 유지용 stub.
    """
    raise NotImplementedError("KRX_ID/KRX_PW 방식은 사용하지 않습니다. 네이버 또는 다른 경로를 쓰세요.")


def fetch_krx_vkospi(days=LOOKBACK_DAYS):
    """
    [DEPRECATED — pykrx 경로는 KRX 유료 로그인 필요로 비활성]
    VKOSPI는 FinanceDataReader의 지수 조회로 대체 (fetch_fdr_vkospi).
    """
    raise NotImplementedError("pykrx 방식은 사용하지 않습니다. fetch_fdr_vkospi를 쓰세요.")


def fetch_fdr_vkospi():
    """
    VKOSPI 지수 조회. 여러 소스 순차 시도 + 값 검증.
    VKOSPI 정상 범위는 5~100 (실제로는 보통 10~60). 이 범위 밖이면 잘못된 데이터로 판단.
    """
    import urllib.parse

    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS)
    candidates = []

    # 1) FDR 여러 심볼 시도
    for symbol in ["KSVKOSPI", "^KSVKOSPI", "^VKOSPI"]:
        try:
            df = fdr.DataReader(symbol)
            if df is None or df.empty:
                continue
            s = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
            s = s.dropna()
            if s.empty:
                continue
            last = float(s.iloc[-1])
            if not (5 <= last <= 100):
                print(f"  [warn] fdr {symbol} out of range ({last:.2f}), skip")
                continue
            s.index = pd.to_datetime(s.index).date
            s = s[s.index >= cutoff]
            print(f"  vkospi via fdr ({symbol}): {len(s)} rows, latest {last:.2f}")
            return s.rename("vkospi")
        except Exception as e:
            print(f"  [warn] fdr {symbol}: {e}")

    # 2) Yahoo Finance chart API 직접 호출
    end_ts = int(dt.datetime.now(KST).timestamp())
    start_ts = end_ts - LOOKBACK_DAYS * 86400
    for symbol in ["^KSVKOSPI", "^VKOSPI"]:
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
                   f"{urllib.parse.quote(symbol)}"
                   f"?period1={start_ts}&period2={end_ts}&interval=1d")
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://finance.yahoo.com/",
            }
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json()
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            dates = [dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).date() for t in timestamps]
            s = pd.Series(closes, index=dates, name="vkospi").dropna()
            if s.empty:
                continue
            last = float(s.iloc[-1])
            if 5 <= last <= 100:
                print(f"  vkospi via yahoo ({symbol}): {len(s)} rows, latest {last:.2f}")
                return s
            else:
                print(f"  [warn] yahoo {symbol} out of range ({last:.2f})")
        except Exception as e:
            print(f"  [warn] yahoo vkospi {symbol}: {e}")

    print(f"  [warn] vkospi: all sources failed")
    return pd.Series(dtype=float, name="vkospi")


# ================================================================
# 공공데이터포털 (data.go.kr) - 금융위원회_금융투자협회종합통계정보
# ================================================================
# Base URL: https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService
# 인증: serviceKey 파라미터 (DATA_GO_KR_API_KEY 환경변수)
# 엔드포인트:
#   - getGrantingOfCreditBalanceInfo: 신용공여잔고추이
#   - getSecuritiesMarketTotalCapitalInfo: 증시자금추이 (미수금/반대매매 포함)

DATA_GO_KR_BASE = "https://apis.data.go.kr/1160100/service/GetKofiaStatisticsInfoService"


def _data_go_kr_fetch(endpoint, extra_params=None, max_pages=10, num_rows=200):
    """
    공공데이터포털 API 공통 호출 함수. JSON 응답 → list[dict] 반환.
    페이징 자동 처리 (최대 max_pages 페이지).
    """
    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key:
        raise RuntimeError("DATA_GO_KR_API_KEY 환경변수가 설정되지 않았습니다")

    url = f"{DATA_GO_KR_BASE}/{endpoint}"
    all_items = []
    for page in range(1, max_pages + 1):
        params = {
            "serviceKey": api_key,
            "resultType": "json",
            "numOfRows": num_rows,
            "pageNo": page,
        }
        if extra_params:
            params.update(extra_params)
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        try:
            data = r.json()
        except ValueError:
            # 에러 응답이 XML로 올 수 있음
            print(f"  [warn] {endpoint} non-json response: {r.text[:200]}")
            break
        # 응답 구조: {"response": {"header": {...}, "body": {"items": {"item": [...]}, ...}}}
        try:
            body = data["response"]["body"]
            items_container = body.get("items", {})
            if not items_container:
                break
            items = items_container.get("item", []) if isinstance(items_container, dict) else items_container
            if isinstance(items, dict):
                items = [items]  # 단일 항목이 dict로 올 때
            if not items:
                break
            all_items.extend(items)
            # 더 가져올 게 있는지 확인
            total = int(body.get("totalCount", 0))
            if len(all_items) >= total:
                break
        except (KeyError, TypeError) as e:
            print(f"  [warn] {endpoint} parse error: {e}")
            break
    return all_items


def fetch_credit_balance(days=LOOKBACK_DAYS):
    """
    신용공여잔고추이 (getGrantingOfCreditBalanceInfo).
    일자별 신용거래융자 전체 잔고(백만원 → 억원 변환) 반환.
    
    Returns:
        dict {date: credit_balance_eok}  — 네이버 호환 포맷
    """
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y%m%d")
    items = _data_go_kr_fetch(
        "getGrantingOfCreditBalanceInfo",
        extra_params={"beginBasDt": start, "endBasDt": end},
        max_pages=5,
        num_rows=500,
    )
    result = {}
    for it in items:
        try:
            bas_dt = str(it.get("basDt", "")).strip()
            if len(bas_dt) != 8:
                continue
            d = dt.datetime.strptime(bas_dt, "%Y%m%d").date()
            # crdTrFingWhl: 신용거래융자 전체 (백만원 단위) → 억원 변환
            val_mil = it.get("crdTrFingWhl")
            if val_mil is None:
                continue
            val_eok = int(float(val_mil) / 100)  # 백만원 → 억원
            result[d] = val_eok
        except (ValueError, TypeError) as e:
            continue
    print(f"  credit balance (data.go.kr): {len(result)} days")
    return result


def fetch_securities_market_capital(days=LOOKBACK_DAYS):
    """
    증시자금추이 (getSecuritiesMarketTotalCapitalInfo).
    미수금 대비 반대매매 금액(원 단위 → 억원 변환) 등 반환.
    
    Returns:
        dict {
          'forced_sale': {date: forced_sale_eok},
          'investor_deposit': {date: deposit_eok},  # 예탁금
        }
    """
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y%m%d")
    items = _data_go_kr_fetch(
        "getSecuritiesMarketTotalCapitalInfo",
        extra_params={"beginBasDt": start, "endBasDt": end},
        max_pages=5,
        num_rows=500,
    )
    forced = {}
    deposit = {}
    for it in items:
        try:
            bas_dt = str(it.get("basDt", "")).strip()
            if len(bas_dt) != 8:
                continue
            d = dt.datetime.strptime(bas_dt, "%Y%m%d").date()
            # brkTrdUcolMnyVsOppsTrdAmt: 위탁매매 미수금 대비 실제반대매매금액 (원 단위)
            forced_raw = it.get("brkTrdUcolMnyVsOppsTrdAmt")
            if forced_raw is not None:
                forced[d] = int(float(forced_raw) / 1e8)  # 원 → 억원
            # invrDpsgAmt: 투자자 예탁금 (원 단위)
            deposit_raw = it.get("invrDpsgAmt")
            if deposit_raw is not None:
                deposit[d] = int(float(deposit_raw) / 1e8)
        except (ValueError, TypeError) as e:
            continue
    print(f"  securities market (data.go.kr): forced={len(forced)}, deposit={len(deposit)}")
    return {"forced_sale": forced, "investor_deposit": deposit}


def fetch_vkospi_naver():
    """[DEPRECATED] 네이버 VKOSPI 단일 스팟값 스크래핑 — fallback용."""
    url = "https://finance.naver.com/sise/sise_index.naver?code=VKOSPI"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    r.encoding = "euc-kr"
    soup = BeautifulSoup(r.text, "html.parser")
    try:
        elem = soup.select_one("#now_value")
        if elem:
            v = float(elem.text.replace(",", "").strip())
            return v
    except Exception:
        pass
    text = soup.get_text(" ", strip=True)
    m = re.search(r"VKOSPI[^\d]{0,20}([\d]+\.[\d]+)", text)
    if m:
        return float(m.group(1))
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
    # 각 종목을 첫 유효값 기준 Base 100으로 정규화 후 평균
    first = df.iloc[0]
    # 0 또는 NaN이 있는 컬럼 제외
    valid_cols = first[(first > 0) & first.notna()].index
    if len(valid_cols) == 0:
        return pd.Series(dtype=float)
    normalized = df[valid_cols].div(first[valid_cols]) * 100
    return normalized.mean(axis=1)


MAIN_COLS = [
    "date",
    "kospi", "kosdaq", "samsung", "hynix", "vkospi",
    "credit_balance_eok", "forced_sale_eok", "foreign_net_eok",
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
    # 1. 신용공여잔고 — data.go.kr 우선, 실패 시 네이버 폴백 (단일값만)
    def _credit_balance_with_fallback():
        try:
            return fetch_credit_balance()  # {date: eok}
        except Exception as e:
            print(f"  [info] data.go.kr credit failed, fallback to naver: {e}")
            dep = fetch_naver_deposit()
            # 네이버는 단일 스팟값만 반환 → 최신일 하나만
            if dep.get("credit_balance_eok"):
                return {TODAY: dep["credit_balance_eok"]}
            return {}

    # 2. 증시자금추이 (반대매매, 예탁금) — data.go.kr 우선, 실패 시 네이버
    def _market_capital_with_fallback():
        try:
            return fetch_securities_market_capital()  # {'forced_sale': {...}, 'investor_deposit': {...}}
        except Exception as e:
            print(f"  [info] data.go.kr market capital failed, fallback to naver: {e}")
            dep = fetch_naver_deposit()
            result = {"forced_sale": {}, "investor_deposit": {}}
            if dep.get("forced_sale_eok"):
                result["forced_sale"][TODAY] = dep["forced_sale_eok"]
            return result

    # 3. 외국인 순매수 — 네이버만 (공공데이터에 없음)
    def _foreign_flow():
        try:
            return fetch_naver_foreign_flow()
        except Exception as e:
            print(f"  [warn] foreign flow failed: {e}")
            return {}

    # 4. VKOSPI — FDR 우선, 실패 시 네이버 스팟값
    def _vkospi_with_fallback():
        series = fetch_fdr_vkospi()
        if not series.empty:
            return {"type": "series", "data": series}
        try:
            v = fetch_vkospi_naver()
            if v is not None:
                return {"type": "spot", "data": v}
        except Exception as e:
            print(f"  [warn] naver vkospi also failed: {e}")
        return {"type": "none", "data": None}

    credit_map = safe("credit", _credit_balance_with_fallback, default={})
    market_cap = safe("market_cap", _market_capital_with_fallback, default={"forced_sale": {}, "investor_deposit": {}})
    foreign_flow = safe("foreign", _foreign_flow, default={})
    vkospi_result = safe("vkospi", _vkospi_with_fallback, default={"type": "none", "data": None})

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
    df["foreign_net_eok"] = None
    df["vkospi"] = None

    # 신용공여잔고 채우기 (전체 시계열)
    for cdate, cval in credit_map.items():
        if cdate in df["date"].values:
            df.loc[df["date"] == cdate, "credit_balance_eok"] = cval

    # 반대매매 채우기 (전체 시계열)
    for fdate, fval in market_cap.get("forced_sale", {}).items():
        if fdate in df["date"].values:
            df.loc[df["date"] == fdate, "forced_sale_eok"] = fval

    # VKOSPI 채우기
    if vkospi_result["type"] == "series":
        vseries = vkospi_result["data"]
        for vdate, vval in vseries.items():
            if vdate in df["date"].values:
                df.loc[df["date"] == vdate, "vkospi"] = float(vval)
    elif vkospi_result["type"] == "spot" and not df.empty:
        latest_date = df["date"].max()
        df.loc[df["date"] == latest_date, "vkospi"] = float(vkospi_result["data"])

    # 외국인 순매수 채우기
    for date, val in foreign_flow.items():
        if date in df["date"].values:
            df.loc[df["date"] == date, "foreign_net_eok"] = val

    for c in MAIN_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[MAIN_COLS]

    history = load_history()
    combined = pd.concat([history, df], ignore_index=True)
    combined = save_history(combined)
    print(f"  saved: {len(combined)} rows, latest: {combined['date'].max() if not combined.empty else 'none'}")
    return combined


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


def compute_signals(df):
    if df.empty:
        return {"kr": {}, "us": {}, "score_kr": 0, "max_kr": 18, "score_us": 0, "max_us": 15,
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

    foreign_ser = pd.to_numeric(df["foreign_net_eok"], errors="coerce").dropna()
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

    vkospi_ser = pd.to_numeric(df["vkospi"], errors="coerce").dropna()
    kr["KR6"] = {"name": "VKOSPI 공포지수", "level": 0, "description": "데이터 수집 중 (네이버)"}
    if len(vkospi_ser):
        v = float(vkospi_ser.iloc[-1])
        kr["KR6"] = {
            "name": "VKOSPI 공포지수",
            "level": level_from_gap(v, [20, 30, 40]),
            "description": f"현재 {v:.1f}"
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
    # 양방향 경고 지표 — 높을 때(시스템 공포)와 낮을 때(쏠림 극한) 모두 위험 신호
    cor_ser = pd.to_numeric(df["cor1m"], errors="coerce").dropna()
    us["US6"] = {"name": "내재상관 COR1M", "level": 0, "description": "데이터 수집 중"}
    if len(cor_ser):
        v = float(cor_ser.iloc[-1])
        # 양방향 위험 판정
        if v >= 60:
            lvl = 3; why = "시스템 공포 (동반 하락 예상)"
        elif v >= 45:
            lvl = 2; why = "상관 급등 — 섹터 차별화 소멸"
        elif v >= 30:
            lvl = 1; why = "상승 추세 — 주의 필요"
        elif v <= 10:
            lvl = 3; why = "쏠림 극한 — 변동성 폭발 위험"
        elif v <= 15:
            lvl = 2; why = "극도의 쏠림 — 경계"
        elif v <= 20:
            lvl = 1; why = "낮음 — 소수 종목 주도"
        else:
            lvl = 0; why = "정상 범위 (20~30)"
        us["US6"] = {
            "name": "CBOE 1M 내재상관 (COR1M)",
            "level": lvl,
            "description": f"현재 {v:.1f} · {why}"
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

    def plot_series_frame(series_dict, lookback=250):
        if not series_dict:
            return pd.DataFrame()
        rows = []
        for key, s in series_dict.items():
            if s is None or len(s) == 0:
                continue
            tmp = pd.Series(s).copy()
            tmp.index = pd.to_datetime(tmp.index, errors="coerce")
            tmp = tmp.dropna()
            if tmp.empty:
                continue
            tmp.index = tmp.index.date
            rows.append(tmp.rename(key))
        if not rows:
            return pd.DataFrame()
        return pd.concat(rows, axis=1).sort_index().tail(lookback)

    def to_points(s, *, ffill=False):
        s = pd.to_numeric(s, errors="coerce")
        if ffill:
            s = s.ffill()
        return [None if pd.isna(v) else round(float(v), 2) for v in s]

    def cum_base100_frame(frame):
        out = pd.DataFrame(index=frame.index)
        for c in frame.columns:
            s = pd.to_numeric(frame[c], errors="coerce").ffill()
            valid = s.dropna()
            if valid.empty or valid.iloc[0] == 0:
                continue
            out[c] = s / valid.iloc[0] * 100
        return out

    dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else []
    kr_rel = pd.DataFrame(index=df_plot["date"] if not df_plot.empty else [])
    if not df_plot.empty:
        kr_rel["KOSPI"] = pd.to_numeric(df_plot["kospi"], errors="coerce")
        kr_rel["KOSDAQ"] = pd.to_numeric(df_plot["kosdaq"], errors="coerce")
        kr_rel = cum_base100_frame(kr_rel)

    semi_frame = pd.DataFrame(index=df_plot["date"] if not df_plot.empty else [])
    if not df_plot.empty:
        semi_frame["코스피"] = pd.to_numeric(df_plot["kospi"], errors="coerce")
        semi_frame["삼성전자"] = pd.to_numeric(df_plot["samsung"], errors="coerce")
        semi_frame["SK하이닉스"] = pd.to_numeric(df_plot["hynix"], errors="coerce")
        semi_frame = cum_base100_frame(semi_frame)

    sector_frame = pd.DataFrame(index=df_plot["date"] if not df_plot.empty else [])
    for c in ["sec_반도체", "sec_방산조선", "sec_바이오", "sec_2차전지", "sec_금융"]:
        if not df_plot.empty and c in df_plot.columns:
            sector_frame[c.replace("sec_","")] = pd.to_numeric(df_plot[c], errors="coerce")
    sector_frame = cum_base100_frame(sector_frame) if not sector_frame.empty else sector_frame

    m7_frame = plot_series_frame(extras.get("m7_basket", {}), lookback=250)
    m7_base = cum_base100_frame(m7_frame) if not m7_frame.empty else pd.DataFrame()
    gl_frame = plot_series_frame({
        "global_liquidity": extras.get("global_liquidity", pd.Series(dtype=float)),
        "usdjpy": extras.get("usdjpy", pd.Series(dtype=float)),
    }, lookback=500)

    js_data = {
        "dates": dates,
        "vkospi": to_points(df_plot["vkospi"], ffill=False) if not df_plot.empty and "vkospi" in df_plot else [],
        "credit": to_points(df_plot["credit_balance_eok"], ffill=False) if not df_plot.empty and "credit_balance_eok" in df_plot else [],
        "forced": to_points(df_plot["forced_sale_eok"], ffill=False) if not df_plot.empty and "forced_sale_eok" in df_plot else [],
        "foreign": to_points(df_plot["foreign_net_eok"], ffill=False) if not df_plot.empty and "foreign_net_eok" in df_plot else [],
        "vix": to_points(df_plot["vix"], ffill=False) if not df_plot.empty and "vix" in df_plot else [],
        "ust10y": to_points(df_plot["ust10y"], ffill=False) if not df_plot.empty and "ust10y" in df_plot else [],
        "cor1m": to_points(df_plot["cor1m"], ffill=False) if not df_plot.empty and "cor1m" in df_plot else [],
        "kr_dates": [d.strftime("%Y-%m-%d") for d in kr_rel.index] if not kr_rel.empty else [],
        "kospi_rel": to_points(kr_rel["KOSPI"]) if "KOSPI" in kr_rel else [],
        "kosdaq_rel": to_points(kr_rel["KOSDAQ"]) if "KOSDAQ" in kr_rel else [],
        "semi_dates": [d.strftime("%Y-%m-%d") for d in semi_frame.index] if not semi_frame.empty else [],
        "semi_kospi": to_points(semi_frame["코스피"]) if "코스피" in semi_frame else [],
        "semi_samsung": to_points(semi_frame["삼성전자"]) if "삼성전자" in semi_frame else [],
        "semi_hynix": to_points(semi_frame["SK하이닉스"]) if "SK하이닉스" in semi_frame else [],
        "sector_dates": [d.strftime("%Y-%m-%d") for d in sector_frame.index] if not sector_frame.empty else [],
        "sec_반도체": to_points(sector_frame["반도체"]) if "반도체" in sector_frame else [],
        "sec_방산조선": to_points(sector_frame["방산조선"]) if "방산조선" in sector_frame else [],
        "sec_바이오": to_points(sector_frame["바이오"]) if "바이오" in sector_frame else [],
        "sec_2차전지": to_points(sector_frame["2차전지"]) if "2차전지" in sector_frame else [],
        "sec_금융": to_points(sector_frame["금융"]) if "금융" in sector_frame else [],
        "m7_dates": [d.strftime("%Y-%m-%d") for d in m7_base.index] if not m7_base.empty else [],
        "m7": {c: to_points(m7_base[c]) for c in m7_base.columns} if not m7_base.empty else {},
        "gl_dates": [d.strftime("%Y-%m-%d") for d in gl_frame.index] if not gl_frame.empty else [],
        "global_liquidity": to_points(gl_frame["global_liquidity"], ffill=True) if "global_liquidity" in gl_frame else [],
        "usdjpy": to_points(gl_frame["usdjpy"], ffill=True) if "usdjpy" in gl_frame else [],
    }

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

    html = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>빚투 모니터</title><script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>
body {font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;max-width:1320px;margin:0 auto;padding:24px;color:#222;background:#fafafa;}
h1 {font-size:26px;font-weight:600;margin:0 0 6px;} .meta {color:#888;font-size:13px;margin-bottom:20px;}
.tabs {display:flex;gap:4px;margin-bottom:20px;border-bottom:2px solid #eee;} .tab {padding:12px 22px;cursor:pointer;font-weight:500;font-size:15px;color:#666;border-bottom:2px solid transparent;margin-bottom:-2px;} .tab.active {color:#222;border-bottom-color:#222;}
.pane {display:none;} .pane.active {display:block;} .overall {background:white;padding:18px 22px;border-radius:12px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center;}
.overall-label {font-size:13px;color:#888;} .overall-value {font-size:28px;font-weight:600;}
.regime-block,.chart,.sig {background:white;padding:14px 16px;border-radius:12px;} .regime-block {padding:18px 20px;margin-bottom:20px;}
.regime-title {font-size:15px;font-weight:600;margin-bottom:14px;color:#333;} .regime-grid {display:grid;grid-template-columns:repeat(3,1fr);gap:12px;}
.regime-card {background:#fafafa;padding:14px 16px;border-radius:10px;border-top:4px solid;} .regime-name {font-size:12px;color:#888;margin-bottom:4px;} .regime-value {font-size:20px;font-weight:600;margin-bottom:2px;} .regime-score {font-size:11px;color:#aaa;margin-bottom:8px;} .regime-reasons {font-size:12px;color:#555;line-height:1.6;}
.signal-grid {display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:20px;} .sig {border-top:4px solid;} .sig-name {font-size:12px;color:#888;margin-bottom:4px;} .sig-status {font-size:17px;font-weight:600;margin-bottom:6px;} .sig-desc {font-size:12px;color:#555;line-height:1.4;}
.chart {margin-bottom:14px;} .chart-grid {display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;} .section-title {font-size:16px;font-weight:600;margin:24px 0 10px;color:#444;} .footer {font-size:11px;color:#aaa;margin-top:32px;padding-top:16px;border-top:1px solid #eee;}
@media (max-width:768px) { .regime-grid,.chart-grid {grid-template-columns:1fr;} }
</style></head><body>
<h1>빚투 모니터</h1><div class="meta">업데이트 __LAST_DATE__</div>
<div class="tabs"><div class="tab active" data-tab="kr">🇰🇷 한국장 (__KR_LABEL__)</div><div class="tab" data-tab="us">🇺🇸 미국장 (__US_LABEL__)</div></div>
<div id="pane-kr" class="pane active">__REGIME_KR__<div class="section-title">단기 위험 신호 (일간)</div><div class="overall" style="border-left:6px solid __KR_COLOR__;"><div><div class="overall-label">한국 위험도 점수</div><div class="overall-value" style="color:__KR_COLOR__;">__KR_LABEL__</div></div><div style="font-size:14px;color:#888;">__KR_SCORE__ / __KR_MAX__점</div></div><div class="signal-grid">__KR_CARDS__</div>
<div class="section-title">차트</div><div class="chart-grid"><div class="chart"><div id="c_kr_rel" style="height:340px;"></div></div><div class="chart"><div id="c_kr_vkospi" style="height:340px;"></div></div></div>
<div class="chart"><div id="c_kr_credit" style="height:320px;"></div></div><div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div><div class="chart"><div id="c_kr_foreign" style="height:300px;"></div></div><div class="chart"><div id="c_kr_semi" style="height:320px;"></div></div><div class="chart"><div id="c_kr_sector" style="height:340px;"></div></div></div>
<div id="pane-us" class="pane">__REGIME_US__<div class="section-title">단기 위험 신호 (일간)</div><div class="overall" style="border-left:6px solid __US_COLOR__;"><div><div class="overall-label">미국 위험도 점수</div><div class="overall-value" style="color:__US_COLOR__;">__US_LABEL__</div></div><div style="font-size:14px;color:#888;">__US_SCORE__ / __US_MAX__점</div></div><div class="signal-grid">__US_CARDS__</div>
<div class="section-title">차트</div><div class="chart"><div id="c_us_m7" style="height:1560px;"></div></div>__US_EXTRA_CHARTS__</div>
<div class="footer">투자 권유 아님</div>
<script>
const D = __JSON__;
const base = {margin:{t:40,r:55,b:35,l:55},font:{family:'system-ui'},paper_bgcolor:'white',plot_bgcolor:'white',legend:{orientation:'h',y:-0.18},hovermode:'x unified'};
function bindTabs(){document.querySelectorAll('.tab').forEach(function(tab){tab.addEventListener('click',function(){document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});document.querySelectorAll('.pane').forEach(function(p){p.classList.remove('active')});tab.classList.add('active');document.getElementById('pane-'+tab.dataset.tab).classList.add('active');});});}
function noData(id,msg){document.getElementById(id).innerHTML='<div style="height:100%;display:flex;align-items:center;justify-content:center;color:#999;">'+msg+'</div>';}
function safePlot(id,traces,title,extra){try{var hasData=(traces||[]).some(function(t){return (t.y||[]).some(function(v){return v!==null&&v!==undefined;});}); if(!hasData){noData(id,'데이터 수집 실패 또는 데이터 없음');return;} Plotly.newPlot(id,traces,Object.assign({},base,{title:{text:title,font:{size:13}}},extra||{}),{displaylogo:false,responsive:true});}catch(e){console.error(id,e);noData(id,'차트 렌더 실패');}}
bindTabs();
function line(color,width){return {color:color,width:width||2};}
safePlot('c_kr_rel', [
{x:D.kr_dates,y:D.kospi_rel,type:'scatter',mode:'lines',name:'코스피',connectgaps:true,line:line('#185FA5',2.8)},
{x:D.kr_dates,y:D.kosdaq_rel,type:'scatter',mode:'lines',name:'코스닥',connectgaps:true,line:line('#534AB7',2.5)}], '코스피 vs 코스닥 상대 추세 (Base 100)', {yaxis:{title:'Base 100'}});
safePlot('c_kr_vkospi', [{x:D.dates,y:D.vkospi,type:'scatter',mode:'lines',name:'VKOSPI',line:line('#A32D2D',2.4),fill:'tozeroy',fillcolor:'rgba(163,45,45,0.08)'}], 'VKOSPI 공포지수', {yaxis:{title:'VKOSPI'}});
safePlot('c_kr_credit', [
{x:D.dates,y:D.credit,type:'scatter',mode:'lines',name:'신용잔고(억)',line:line('#993C1D',2.2)},
{x:D.kr_dates,y:D.kospi_rel,type:'scatter',mode:'lines',name:'코스피(Base100)',yaxis:'y2',line:{color:'#185FA5',width:2,dash:'dot'}}], '신용잔고 vs 코스피 상대 추세', {yaxis:{title:'신용잔고(억)'},yaxis2:{title:'코스피(Base100)',overlaying:'y',side:'right'}});
safePlot('c_kr_forced', [{x:D.dates,y:D.forced,type:'bar',name:'반대매매(억)',marker:{color:(D.forced||[]).map(function(v){return v>=600?'#A32D2D':v>=400?'#D85A30':v>=200?'#BA7517':'#888';})}}], '일일 반대매매');
safePlot('c_kr_foreign', [{x:D.dates,y:D.foreign,type:'bar',name:'외국인(억)',marker:{color:(D.foreign||[]).map(function(v){return v<0?'#A32D2D':'#1D9E75';})}}], '외국인 일별 순매수/매도 (코스피+코스닥)');
safePlot('c_kr_semi', [
{x:D.semi_dates,y:D.semi_kospi,type:'scatter',mode:'lines',name:'코스피',line:{color:'#888',width:1.5,dash:'dot'}},
{x:D.semi_dates,y:D.semi_samsung,type:'scatter',mode:'lines',name:'삼성전자',line:line('#185FA5',2.6)},
{x:D.semi_dates,y:D.semi_hynix,type:'scatter',mode:'lines',name:'SK하이닉스',line:line('#534AB7',2.6)}], '반도체 누적 추세 (Base 100)', {yaxis:{title:'Base 100'}});
safePlot('c_kr_sector', [
{x:D.sector_dates,y:D.sec_반도체,type:'scatter',mode:'lines',name:'반도체',line:line('#185FA5',2.6)},
{x:D.sector_dates,y:D.sec_방산조선,type:'scatter',mode:'lines',name:'방산조선',line:line('#534AB7',2.3)},
{x:D.sector_dates,y:D.sec_바이오,type:'scatter',mode:'lines',name:'바이오',line:line('#1D9E75',2.1)},
{x:D.sector_dates,y:D.sec_2차전지,type:'scatter',mode:'lines',name:'2차전지',line:line('#BA7517',2.1)},
{x:D.sector_dates,y:D.sec_금융,type:'scatter',mode:'lines',name:'금융',line:line('#888',2.0)}], '업종 바구니 누적 추세 (Base 100)', {yaxis:{title:'Base 100'}});
var m7Colors={SP500:'#888',IXIC:'#185FA5',AAPL:'#555',MSFT:'#3b82f6',GOOGL:'#0ea5e9',AMZN:'#f59e0b',META:'#6366f1',NVDA:'#10b981',TSLA:'#ef4444',AVGO:'#8b5cf6',TSM:'#14b8a6'};
var m7Traces=Object.keys(D.m7||{}).map(function(k){return {x:D.m7_dates,y:D.m7[k],type:'scatter',mode:'lines',name:(k==='IXIC'?'나스닥':(k==='SP500'?'S&P500':k)),line:{color:m7Colors[k]||'#333',width:(k==='SP500'||k==='IXIC')?2.6:1.9}}});
safePlot('c_us_m7', m7Traces, '미국 빅테크/M7/브로드컴/TSMC + 나스닥 상대 추세 (Base 100)', {yaxis:{title:'Base 100'}});
safePlot('c_us_vix', [{x:D.dates,y:D.vix,type:'scatter',mode:'lines',name:'VIX',line:line('#A32D2D',2.4),fill:'tozeroy',fillcolor:'rgba(163,45,45,0.08)'}], 'VIX 공포지수', {yaxis:{title:'VIX'}});
safePlot('c_us_rate', [{x:D.dates,y:D.ust10y,type:'scatter',mode:'lines',name:'미국 10년물',line:line('#BA7517',2.4)}], '미국 10년물 국채금리', {yaxis:{title:'%'}});
safePlot('c_us_cor1m', [{x:D.dates,y:D.cor1m,type:'scatter',mode:'lines',name:'COR1M',line:line('#534AB7',2.4),fill:'tozeroy',fillcolor:'rgba(83,74,183,0.08)'}], 'CBOE 1개월 내재상관 (COR1M)', {yaxis:{title:'COR1M',range:[0,100]}});
safePlot('c_us_gl', [
{x:D.gl_dates,y:D.global_liquidity,type:'scatter',mode:'lines',name:'글로벌 유동성 proxy',line:line('#185FA5',2.8)},
{x:D.gl_dates,y:D.usdjpy,type:'scatter',mode:'lines',name:'달러-엔',yaxis:'y2',line:line('#9ca3af',2.1)}], '글로벌 유동성 Proxy vs 달러-엔', {yaxis:{title:'Proxy (Base 100)'},yaxis2:{title:'엔',overlaying:'y',side:'right'}});
</script></body></html>"""
    html = (html.replace('__LAST_DATE__', last_date)
        .replace('__KR_LABEL__', signals['label_kr'])
        .replace('__US_LABEL__', signals['label_us'])
        .replace('__KR_COLOR__', kr_color)
        .replace('__US_COLOR__', us_color)
        .replace('__KR_SCORE__', str(signals['score_kr']))
        .replace('__KR_MAX__', str(signals['max_kr']))
        .replace('__US_SCORE__', str(signals['score_us']))
        .replace('__US_MAX__', str(signals['max_us']))
        .replace('__REGIME_KR__', render_regime_block(regime_kr, '코스피 추세 전망'))
        .replace('__REGIME_US__', render_regime_block(regime_us, 'S&P 500 추세 전망'))
        .replace('__KR_CARDS__', render_signals(signals['kr']))
        .replace('__US_CARDS__', render_signals(signals['us']))
        .replace('__US_EXTRA_CHARTS__', ''.join([
            '<div class="chart-grid"><div class="chart"><div id="c_us_vix" style="height:300px;"></div></div><div class="chart"><div id="c_us_rate" style="height:300px;"></div></div></div>' if js_data.get('vix') and any(v is not None for v in js_data.get('vix', [])) and js_data.get('ust10y') and any(v is not None for v in js_data.get('ust10y', [])) else ('<div class="chart"><div id="c_us_vix" style="height:300px;"></div></div>' if js_data.get('vix') and any(v is not None for v in js_data.get('vix', [])) else ('<div class="chart"><div id="c_us_rate" style="height:300px;"></div></div>' if js_data.get('ust10y') and any(v is not None for v in js_data.get('ust10y', [])) else '')),
            '<div class="chart"><div id="c_us_cor1m" style="height:320px;"></div></div>' if js_data.get('cor1m') and any(v is not None for v in js_data.get('cor1m', [])) else '',
            '<div class="chart"><div id="c_us_gl" style="height:380px;"></div></div>' if ((js_data.get('global_liquidity') and any(v is not None for v in js_data.get('global_liquidity', []))) or (js_data.get('usdjpy') and any(v is not None for v in js_data.get('usdjpy', [])))) else ''
        ]))
        .replace('__JSON__', json.dumps(js_data, ensure_ascii=False)))
    fp = DOCS_DIR / 'index.html'
    fp.write_text(html, encoding='utf-8')
    print(f"[ok] dashboard written: {fp}")

def main():
    try:
        df = update_data()
    except Exception as e:
        print(f"[error] update_data fatal: {e}")
        import traceback; traceback.print_exc()
        df = load_history()

    extras = {}
    try:
        extras["m7_basket"] = fetch_m7_plus_basket()
    except Exception as e:
        print(f"[warn] m7 basket: {e}")
        extras["m7_basket"] = {}
    try:
        gl = fetch_global_liquidity_proxy()
        extras.update(gl)
    except Exception as e:
        print(f"[warn] global liquidity: {e}")
        extras["global_liquidity"] = pd.Series(dtype=float)
        extras["usdjpy"] = pd.Series(dtype=float)

    signals = compute_signals(df)
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
