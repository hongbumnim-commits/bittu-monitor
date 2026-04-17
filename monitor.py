"""
빚투 모니터 v5 - 추세 전망 + 에러 수정 + 차트 가독성 대폭 개선 (Base 100)

v4 에러 수정:
  - VKOSPI: FinanceDataReader로 안 됨 → 네이버 스크래핑
  - US 10Y: 야후 차트 경로 변경 → FRED API 사용

신규 반영: 
  - 네이버 크롤링 봇 차단 우회 (User-Agent 및 헤더 강화)
  - 차트 출력 기간 120일 -> 350일 확장
  - 반도체 양대장(삼성/하이닉스) 일간 변동률 -> 코스피 대비 Base 100 누적 추세선으로 변경
  - 업종 바구니 일간 변동률 -> 복리 누적 수익률(Base 100) 차트로 변경
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

# 네이버 스크래핑 차단 우회를 위한 공통 헤더
NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

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


def fetch_vkospi_naver():
    url = "https://finance.naver.com/sise/sise_index.naver?code=VKOSPI"
    r = requests.get(url, headers=NAVER_HEADERS, timeout=15)
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


def fetch_ust10y_fred():
    start = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    df = fdr.DataReader("FRED:DGS10", start)
    if df.empty:
        return pd.Series(dtype=float, name="ust10y")
    col = df.columns[0]
    s = df[col]
    s.index = pd.to_datetime(s.index).date
    return s.rename("ust10y").dropna()


def fetch_naver_deposit():
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    r = requests.get(url, headers=NAVER_HEADERS, timeout=20)
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
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    r = requests.get(url, headers=NAVER_HEADERS, timeout=15)
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
        print(f"  [warn] foreign flow: {e}")
    print(f"  foreign flow: {len(result)} days")
    return result


def fetch_sector_basket(tickers):
    closes = []
    # 데이터 조회 기간을 넉넉히 잡습니다 (누적 수익률 계산을 위해)
    for t in tickers:
        s = safe(f"ticker_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=400), default=pd.Series(dtype=float))
        if not s.empty:
            closes.append(s)
    if not closes:
        return pd.Series(dtype=float)
    df = pd.concat(closes, axis=1).ffill()
    
    # 일간 변동률 평균 후 복리 누적 계산 (Base 100)
    daily_ret = df.pct_change().mean(axis=1).fillna(0)
    cum_index = (1 + daily_ret).cumprod() * 100
    return cum_index


MAIN_COLS = [
    "date",
    "kospi", "kosdaq", "samsung", "hynix", "vkospi",
    "credit_balance_eok", "forced_sale_eok", "foreign_net_eok",
    "samsung_ret_pct", "hynix_ret_pct",
    "sp500", "nasdaq", "vix", "nvda", "ust10y",
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

    sectors = {}
    for name, tickers in SECTOR_BASKETS.items():
        s = safe(f"sector_{name}", lambda tt=tickers: fetch_sector_basket(tt), default=pd.Series(dtype=float))
        sectors[f"sec_{name}"] = s.rename(f"sec_{name}")

    deposit = safe("deposit", fetch_naver_deposit, default={"credit_balance_eok": None, "forced_sale_eok": None})
    foreign_flow = safe("foreign", fetch_naver_foreign_flow, default={})
    vkospi_today = safe("vkospi_naver", fetch_vkospi_naver, default=None)

    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix,
        "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y,
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

    if not df.empty and deposit.get("credit_balance_eok"):
        latest_date = df["date"].max()
        mask = df["date"] == latest_date
        df.loc[mask, "credit_balance_eok"] = deposit["credit_balance_eok"]
        df.loc[mask, "forced_sale_eok"] = deposit["forced_sale_eok"]
        if vkospi_today is not None:
            df.loc[mask, "vkospi"] = vkospi_today

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


def render_dashboard(df, signals, regime_kr, regime_us):
    # tail(350) 적용으로 시각화 기간 연장
    df_plot = df.sort_values("date").tail(350).copy() if not df.empty else pd.DataFrame()

    def col(c):
        if df_plot.empty or c not in df_plot.columns:
            return []
        return pd.to_numeric(df_plot[c], errors="coerce").ffill().fillna(0).round(2).tolist()

    def col_raw(c):
        if df_plot.empty or c not in df_plot.columns:
            return []
        return pd.to_numeric(df_plot[c], errors="coerce").fillna(0).round(2).tolist()

    dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else []

    def ma_series(c, window=200):
        if df.empty or c not in df.columns:
            return []
        full = pd.to_numeric(df[c], errors="coerce")
        ma_full = full.rolling(window).mean()
        return ma_full.tail(350).fillna(0).round(2).tolist()

    js_data = {
        "dates": dates,
        "kospi": col("kospi"), "kospi_ma200": ma_series("kospi", 200),
        "kosdaq": col("kosdaq"),
        "samsung": col("samsung"), "hynix": col("hynix"), # Base 100 계산을 위해 원본 주가 추가
        "vkospi": col_raw("vkospi"),
        "credit": col("credit_balance_eok"), "forced": col_raw("forced_sale_eok"),
        "foreign": col_raw("foreign_net_eok"),
        "sp500": col("sp500"), "sp500_ma200": ma_series("sp500", 200),
        "nasdaq": col("nasdaq"), "nasdaq_ma200": ma_series("nasdaq", 200),
        "vix": col("vix"), "nvda": col("nvda"), "ust10y": col("ust10y"),
        "sec_반도체": col_raw("sec_반도체"),
        "sec_방산조선": col_raw("sec_방산조선"),
        "sec_바이오": col_raw("sec_바이오"),
        "sec_2차전지": col_raw("sec_2차전지"),
        "sec_금융": col_raw("sec_금융"),
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

    kr_cards = render_signals(signals["kr"])
    us_cards = render_signals(signals["us"])
    regime_kr_html = render_regime_block(regime_kr, "코스피 추세 전망")
    regime_us_html = render_regime_block(regime_us, "S&P 500 추세 전망")

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>빚투 모니터 — 추세 전망 & 위험 신호</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 24px; color: #222; background: #fafafa; }}
  h1 {{ font-size: 26px; font-weight: 600; margin: 0 0 6px; }}
  .meta {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
  .tabs {{ display: flex; gap: 4px; margin-bottom: 20px; border-bottom: 2px solid #eee; }}
  .tab {{ padding: 12px 22px; cursor: pointer; font-weight: 500; font-size: 15px; color: #666;
          border-bottom: 2px solid transparent; margin-bottom: -2px; }}
  .tab.active {{ color: #222; border-bottom-color: #222; }}
  .pane {{ display: none; }}
  .pane.active {{ display: block; }}
  .overall {{ background: white; padding: 18px 22px; border-radius: 12px; margin-bottom: 20px;
              display: flex; justify-content: space-between; align-items: center; }}
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
  .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                   gap: 12px; margin-bottom: 20px; }}
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
    <div>
      <div class="overall-label">한국 위험도 점수</div>
      <div class="overall-value" style="color: {kr_color};">{signals['label_kr']}</div>
    </div>
    <div style="font-size: 14px; color: #888;">{signals['score_kr']} / {signals['max_kr']}점</div>
  </div>

  <div class="signal-grid">{kr_cards}</div>

  <div class="section-title">차트</div>
  <div class="chart-grid">
    <div class="chart"><div id="c_kr_idx" style="height:300px;"></div></div>
    <div class="chart"><div id="c_kr_vkospi" style="height:300px;"></div></div>
  </div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_foreign" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_semi" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_sector" style="height:320px;"></div></div>
</div>

<div id="pane-us" class="pane">
  {regime_us_html}

  <div class="section-title">단기 위험 신호 (일간)</div>
  <div class="overall" style="border-left: 6px solid {us_color};">
    <div>
      <div class="overall-label">미국 위험도 점수</div>
      <div class="overall-value" style="color: {us_color};">{signals['label_us']}</div>
    </div>
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
</div>

<div class="footer">
  데이터: FinanceDataReader, FRED(미국 10Y), 네이버 금융(신용잔고/VKOSPI/외국인) ·
  추세 전망은 이동평균·모멘텀·52주 범위 기반 통계 모델 (확정적 예측 아님) ·
  매일 평일 한국시간 17:30 GitHub Actions로 1회 실행 · 투자 권유 아님
</div>

<script>
const D = {json.dumps(js_data, ensure_ascii=False)};
const base = {{
  margin: {{t: 30, r: 45, b: 35, l: 50}}, font: {{family: 'system-ui'}},
  paper_bgcolor: 'white', plot_bgcolor: 'white',
  legend: {{orientation: 'h', y: -0.2}}
}};

function plot(id, traces, title, extra) {{
  if (!D.dates.length) {{
    document.getElementById(id).innerHTML = '<p style="text-align:center;color:#888;padding:40px;">데이터 대기</p>';
    return;
  }}
  Plotly.newPlot(id, traces, {{...base, title: {{text: title, font: {{size: 13}}}}, ...(extra || {{}})}});
}}

plot('c_kr_idx', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.kospi_ma200, type: 'scatter', mode: 'lines', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}},
  {{x: D.dates, y: D.kosdaq, type: 'scatter', mode: 'lines', name: '코스닥', yaxis: 'y2', line: {{color: '#534AB7', width: 2}}}}
], '코스피 + 200일선 & 코스닥', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '코스닥', overlaying: 'y', side: 'right'}}}});

plot('c_kr_vkospi', [
  {{x: D.dates, y: D.vkospi, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VKOSPI',
    line: {{color: '#A32D2D'}}, fillcolor: 'rgba(163,45,45,0.1)'}}
], 'VKOSPI 공포지수', {{
  shapes: [
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 20, y1: 20, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}},
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 30, y1: 30, line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}
  ]
}});

plot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
], '코스피 vs 신용잔고', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '잔고(억)', overlaying: 'y', side: 'right'}}}});

plot('c_kr_forced', [{{
  x: D.dates, y: D.forced, type: 'bar', name: '반대매매(억)',
  marker: {{color: D.forced.map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}
}}], '일일 반대매매');

plot('c_kr_foreign', [{{
  x: D.dates, y: D.foreign, type: 'bar', name: '외국인(억)',
  marker: {{color: D.foreign.map(v => v < 0 ? '#A32D2D' : '#1D9E75')}}
}}], '외국인 일별 순매수/매도 (코스피+코스닥)');

function normalize(arr) {{
  const first = arr.find(v => v > 0);
  return arr.map(v => v > 0 ? (v / first) * 100 : null);
}}

plot('c_kr_semi', [
  {{x: D.dates, y: normalize(D.kospi), type: 'scatter', mode: 'lines', name: '코스피', line: {{color: '#888', width: 2, dash: 'dot'}}}},
  {{x: D.dates, y: normalize(D.samsung), type: 'scatter', mode: 'lines', name: '삼성전자', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: normalize(D.hynix), type: 'scatter', mode: 'lines', name: 'SK하이닉스', line: {{color: '#534AB7', width: 2}}}}
], '코스피 vs 반도체 양대장 추세 (시작점=100)');

plot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', mode: 'lines', name: '반도체', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.sec_방산조선, type: 'scatter', mode: 'lines', name: '방산조선', line: {{color: '#534AB7'}}}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', mode: 'lines', name: '바이오', line: {{color: '#1D9E75'}}}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', mode: 'lines', name: '2차전지', line: {{color: '#BA7517'}}}},
  {{x: D.dates, y: D.sec_금융, type: 'scatter', mode: 'lines', name: '금융', line: {{color: '#888'}}}}
], '업종 바구니 누적 추세 (Base 100)');

plot('c_us_sp', [
  {{x: D.dates, y: D.sp500, type: 'scatter', mode: 'lines', name: 'S&P 500', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.sp500_ma200, type: 'scatter', mode: 'lines', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}}
], 'S&P 500 + 200일선');

plot('c_us_nasdaq', [
  {{x: D.dates, y: D.nasdaq, type: 'scatter', mode: 'lines', name: '나스닥', line: {{color: '#534AB7', width: 2}}}},
  {{x: D.dates, y: D.nasdaq_ma200, type: 'scatter', mode: 'lines', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}}
], '나스닥 + 200일선');

plot('c_us_vix', [
  {{x: D.dates, y: D.vix, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VIX',
    line: {{color: '#A32D2D'}}, fillcolor: 'rgba(163,45,45,0.1)'}}
], 'VIX 공포지수');

plot('c_us_nvda', [
  {{x: D.dates, y: D.nvda, type: 'scatter', mode: 'lines', name: 'NVDA', line: {{color: '#1D9E75', width: 2}}}}
], '엔비디아');

plot('c_us_rate', [
  {{x: D.dates, y: D.ust10y, type: 'scatter', mode: 'lines', name: '10Y', line: {{color: '#BA7517', width: 2}}}}
], '미국 10년물 국채금리 (%)');

document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', e => {{
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
  e.target.classList.add('active');
  document.getElementById('pane-' + e.target.dataset.tab).classList.add('active');
  setTimeout(() => window.dispatchEvent(new Event('resize')), 100);
}}));
</script>
</body>
</html>
"""
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")
    print(f"  dashboard written")


def main():
    try:
        df = update_data()
    except Exception as e:
        print(f"[error] update_data fatal: {e}")
        import traceback; traceback.print_exc()
        df = load_history()

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
        render_dashboard(df, signals, regime_kr, regime_us)
    except Exception as e:
        print(f"[error] render_dashboard: {e}")
        import traceback; traceback.print_exc()

    print("Done.")


if __name__ == "__main__":
    main()
