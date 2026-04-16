"""
빚투 모니터 v4 - 한국·미국 동시 감시 + 업종·외국인·밸류에이션 확장

주요 기능:
  한국장 (6개 신호)
    KR1. 일일 반대매매 추세
    KR2. 신용잔고 증감률 vs 코스피 수익률
    KR3. 삼성·하이닉스 동시 -3% 빈도
    KR4. 외국인 7일 누적 순매도 (조원)
    KR5. 코스피-200일선 괴리율
    KR6. VKOSPI 공포지수

  미국장 (5개 신호)
    US1. S&P500 일간 변동성
    US2. 나스닥-200일선 괴리율
    US3. VIX 공포지수
    US4. 반도체 대표주(NVDA) 일간 변동
    US5. 10년물 국채금리 변화

  추적 데이터:
    - 외국인 일별 순매수 (네이버 금융)
    - 업종별 대표 종목 바구니 (반도체/바이오/방산/2차전지/금융)
    - 국가별 PER/PBR 추정 (10년 추세)
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
LOOKBACK_DAYS = 400  # 200일선 계산 위해 충분히 넉넉하게

# ===== 업종별 대표 종목 바구니 (한국) =====
SECTOR_BASKETS = {
    "반도체":   ["005930", "000660", "042700", "403870"],  # 삼성전자, SK하이닉스, 한미반도체, HPSP
    "방산조선":  ["012450", "079550", "329180", "042660"],  # 한화에어로스페이스, LIG넥스원, HD현대중공업, 한화오션
    "바이오":   ["068270", "207940", "196170", "328130"],  # 셀트리온, 삼성바이오로직스, 알테오젠, 루닛
    "2차전지":  ["373220", "006400", "051910", "096770"],  # LG에너지솔루션, 삼성SDI, LG화학, SK이노베이션
    "금융":     ["105560", "055550", "086790", "316140"],  # KB금융, 신한지주, 하나금융지주, 우리금융지주
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


# ===================== 데이터 수집 =====================

def fetch_fdr(symbol, name=None, days=LOOKBACK_DAYS):
    start = (TODAY - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    end = TODAY.strftime("%Y-%m-%d")
    df = fdr.DataReader(symbol, start, end)
    if df.empty:
        return pd.Series(dtype=float, name=name or symbol)
    s = df["Close"]
    s.index = pd.to_datetime(s.index).date
    return s.rename(name or symbol)


def fetch_naver_deposit():
    """네이버 증시자금동향 - 신용잔고/반대매매"""
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    r.encoding = "euc-kr"
    result = {"credit_balance_eok": None, "forced_sale_eok": None}
    try:
        tables = pd.read_html(r.text, encoding="euc-kr")
        for t in tables:
            t_str = t.astype(str)
            flat = " ".join([" ".join(row) for row in t_str.values.tolist()])
            if ("신용잔고" in flat or "신용공여" in flat) and result["credit_balance_eok"] is None:
                for row in t_str.values.tolist():
                    row_txt = " ".join(row)
                    if "신용" in row_txt:
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
        print(f"  [warn] deposit parse: {e}")
    # 정규식 백업
    if result["credit_balance_eok"] is None or result["forced_sale_eok"] is None:
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
    print(f"  naver deposit: credit={result['credit_balance_eok']}, forced={result['forced_sale_eok']}")
    return result


def fetch_naver_foreign_flow():
    """
    네이버 투자자별 매매동향 스크래핑.
    https://finance.naver.com/sise/investorDealTrendDay.naver
    일별 외국인 순매수 (코스피+코스닥 합산, 억원 단위) 최근 30일.
    """
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    r.encoding = "euc-kr"

    result = {}  # date -> foreign_net_eok
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
                    # 외국인 컬럼 찾기 (보통 음수/양수 포함)
                    foreign_val = None
                    numeric_vals = []
                    for v in row_vals:
                        v_clean = v.replace(",", "").replace("+", "").strip()
                        m = re.match(r"^-?\d+$", v_clean)
                        if m:
                            numeric_vals.append(int(v_clean))
                    if len(numeric_vals) >= 3:
                        # 보통 개인 / 외국인 / 기관 순서 -> 외국인은 중간 또는 위치는 컬럼 순서에 따름
                        # 표 헤더가 어떻든 '외국인' 값은 단위 억원
                        foreign_val = numeric_vals[1] if len(numeric_vals) >= 3 else numeric_vals[0]
                    if foreign_val is not None:
                        result[date_match] = foreign_val
    except Exception as e:
        print(f"  [warn] foreign flow: {e}")

    print(f"  foreign flow: {len(result)} days collected")
    return result


def fetch_sector_basket(tickers):
    """업종 바구니 종목들의 평균 일간 수익률"""
    closes = []
    for t in tickers:
        s = safe(f"ticker_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=60), default=pd.Series(dtype=float))
        if not s.empty:
            closes.append(s)
    if not closes:
        return pd.Series(dtype=float)
    df = pd.concat(closes, axis=1).ffill()
    # 각 종목의 일간 % 변화율 평균 = 바구니 수익률
    returns = df.pct_change() * 100
    basket_return = returns.mean(axis=1)
    return basket_return


# ===================== 히스토리 관리 =====================

MAIN_COLS = [
    "date",
    # 한국
    "kospi", "kosdaq", "samsung", "hynix", "vkospi",
    "credit_balance_eok", "forced_sale_eok", "foreign_net_eok",
    "samsung_ret_pct", "hynix_ret_pct",
    # 미국
    "sp500", "nasdaq", "vix", "nvda", "ust10y",
    # 업종 바구니 수익률
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

    # 한국 지수
    kospi = safe("kospi",  lambda: fetch_fdr("KS11", "kospi"),  default=pd.Series(dtype=float, name="kospi"))
    kosdaq = safe("kosdaq", lambda: fetch_fdr("KQ11", "kosdaq"), default=pd.Series(dtype=float, name="kosdaq"))
    samsung = safe("samsung", lambda: fetch_fdr("005930", "samsung"), default=pd.Series(dtype=float, name="samsung"))
    hynix = safe("hynix", lambda: fetch_fdr("000660", "hynix"), default=pd.Series(dtype=float, name="hynix"))
    vkospi = safe("vkospi", lambda: fetch_fdr("KQ150", "vkospi"), default=pd.Series(dtype=float, name="vkospi"))

    # 미국 지수
    sp500 = safe("sp500", lambda: fetch_fdr("US500", "sp500"), default=pd.Series(dtype=float, name="sp500"))
    nasdaq = safe("nasdaq", lambda: fetch_fdr("IXIC", "nasdaq"), default=pd.Series(dtype=float, name="nasdaq"))
    vix = safe("vix", lambda: fetch_fdr("VIX", "vix"), default=pd.Series(dtype=float, name="vix"))
    nvda = safe("nvda", lambda: fetch_fdr("NVDA", "nvda"), default=pd.Series(dtype=float, name="nvda"))
    # 10년물 국채금리 TNX (*10 단위로 반환되기도 함)
    ust10y = safe("ust10y", lambda: fetch_fdr("US10YT=X", "ust10y"), default=pd.Series(dtype=float, name="ust10y"))
    if ust10y.empty:
        ust10y = safe("ust10y_alt", lambda: fetch_fdr("^TNX", "ust10y"), default=pd.Series(dtype=float, name="ust10y"))

    # 업종 바구니
    sectors = {}
    for name, tickers in SECTOR_BASKETS.items():
        s = safe(f"sector_{name}", lambda tt=tickers: fetch_sector_basket(tt), default=pd.Series(dtype=float))
        sectors[f"sec_{name}"] = s.rename(f"sec_{name}")

    # 자금 동향
    deposit = safe("deposit", fetch_naver_deposit, default={"credit_balance_eok": None, "forced_sale_eok": None})
    foreign_flow = safe("foreign", fetch_naver_foreign_flow, default={})

    # 통합 데이터프레임
    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix, "vkospi": vkospi,
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

    # 오늘치 자금 데이터 주입
    if not df.empty and deposit.get("credit_balance_eok"):
        latest_date = df["date"].max()
        mask = df["date"] == latest_date
        df.loc[mask, "credit_balance_eok"] = deposit["credit_balance_eok"]
        df.loc[mask, "forced_sale_eok"] = deposit["forced_sale_eok"]

    # 외국인 순매수는 네이버가 최근 30일치 한 번에 줌 -> 날짜별 매핑
    for date, val in foreign_flow.items():
        if date in df["date"].values:
            df.loc[df["date"] == date, "foreign_net_eok"] = val

    # 누락 컬럼 추가
    for c in MAIN_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[MAIN_COLS]

    history = load_history()
    combined = pd.concat([history, df], ignore_index=True)
    combined = save_history(combined)
    print(f"  saved: {len(combined)} rows, latest: {combined['date'].max() if not combined.empty else 'none'}")
    return combined


# ===================== 신호 계산 =====================

def level_from_gap(v, thresholds):
    """thresholds = [t1, t2, t3]. v >= t3 -> 3, >= t2 -> 2, >= t1 -> 1, else 0"""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0
    if v >= thresholds[2]: return 3
    if v >= thresholds[1]: return 2
    if v >= thresholds[0]: return 1
    return 0


def pct_deviation_from_ma(series, window=200):
    """이동평균 대비 괴리율"""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < window:
        return None
    ma = s.rolling(window).mean().iloc[-1]
    cur = s.iloc[-1]
    if ma == 0 or pd.isna(ma):
        return None
    return float((cur / ma - 1) * 100)


def compute_signals(df):
    """한국 6개 + 미국 5개 신호"""
    if df.empty:
        return {"kr": {}, "us": {}, "overall_kr": 0, "overall_us": 0, "label_kr": "대기", "label_us": "대기"}

    df = df.sort_values("date").reset_index(drop=True)
    last = df.iloc[-1]

    kr, us = {}, {}

    # === 한국 KR1: 반대매매 추세 ===
    forced_series = pd.to_numeric(df["forced_sale_eok"], errors="coerce").dropna()
    avg20 = float(forced_series.tail(20).mean()) if len(forced_series) else 0.0
    today_forced = float(last.get("forced_sale_eok") or 0)
    peak = max(today_forced, avg20)
    kr["KR1"] = {
        "name": "반대매매 일평균",
        "level": level_from_gap(peak, [200, 400, 600]),
        "description": f"오늘 {today_forced:.0f}억 / 20일 평균 {avg20:.0f}억"
    }

    # === KR2: 신용잔고 추월 ===
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

    # === KR3: 반도체 양대장 동시급락 ===
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

    # === KR4: 외국인 7일 누적 순매도 ===
    foreign_ser = pd.to_numeric(df["foreign_net_eok"], errors="coerce").dropna()
    kr["KR4"] = {"name": "외국인 7일 누적 순매도", "level": 0, "description": "데이터 부족"}
    if len(foreign_ser) >= 7:
        sum7 = float(foreign_ser.tail(7).sum())
        outflow = -sum7 / 10000  # 음수 매도를 양수 조원으로
        kr["KR4"] = {
            "name": "외국인 7일 누적 순매도",
            "level": level_from_gap(outflow, [1.0, 3.0, 5.0]),
            "description": f"최근 7영업일 합계 {sum7/10000:+.2f}조 (매도={outflow:+.2f}조)"
        }

    # === KR5: 코스피-200일선 괴리 ===
    dev_kospi = pct_deviation_from_ma(df["kospi"], 200)
    kr["KR5"] = {"name": "코스피-200일선 괴리", "level": 0, "description": "데이터 부족"}
    if dev_kospi is not None:
        abs_dev = abs(dev_kospi)
        kr["KR5"] = {
            "name": "코스피-200일선 괴리율",
            "level": level_from_gap(abs_dev, [10, 20, 30]),
            "description": f"200일선 대비 {dev_kospi:+.1f}% (과열/과매도 모두 위험)"
        }

    # === KR6: VKOSPI 공포지수 ===
    vkospi_ser = pd.to_numeric(df["vkospi"], errors="coerce").dropna()
    kr["KR6"] = {"name": "VKOSPI 공포지수", "level": 0, "description": "데이터 부족"}
    if len(vkospi_ser):
        v = float(vkospi_ser.iloc[-1])
        kr["KR6"] = {
            "name": "VKOSPI 공포지수",
            "level": level_from_gap(v, [20, 30, 40]),
            "description": f"현재 {v:.1f} (20↑주의, 30↑경계, 40↑공포)"
        }

    # === 미국 US1: S&P500 일간 변동성 ===
    sp500_ser = pd.to_numeric(df["sp500"], errors="coerce").dropna()
    us["US1"] = {"name": "S&P500 일간 변동성", "level": 0, "description": "데이터 부족"}
    if len(sp500_ser) >= 2:
        ret = (sp500_ser.iloc[-1] / sp500_ser.iloc[-2] - 1) * 100
        us["US1"] = {
            "name": "S&P500 전일 대비",
            "level": level_from_gap(abs(ret), [1, 2, 3]),
            "description": f"전일 대비 {ret:+.2f}%"
        }

    # === US2: 나스닥-200일선 괴리 ===
    dev_nas = pct_deviation_from_ma(df["nasdaq"], 200)
    us["US2"] = {"name": "나스닥-200일선 괴리", "level": 0, "description": "데이터 부족"}
    if dev_nas is not None:
        us["US2"] = {
            "name": "나스닥-200일선 괴리율",
            "level": level_from_gap(abs(dev_nas), [10, 20, 30]),
            "description": f"200일선 대비 {dev_nas:+.1f}%"
        }

    # === US3: VIX ===
    vix_ser = pd.to_numeric(df["vix"], errors="coerce").dropna()
    us["US3"] = {"name": "VIX 공포지수", "level": 0, "description": "데이터 부족"}
    if len(vix_ser):
        v = float(vix_ser.iloc[-1])
        us["US3"] = {
            "name": "VIX 공포지수",
            "level": level_from_gap(v, [20, 30, 40]),
            "description": f"현재 {v:.1f} (20↑주의, 30↑경계, 40↑공포)"
        }

    # === US4: 엔비디아 일간 ===
    nvda_ser = pd.to_numeric(df["nvda"], errors="coerce").dropna()
    us["US4"] = {"name": "엔비디아 일간", "level": 0, "description": "데이터 부족"}
    if len(nvda_ser) >= 2:
        ret = (nvda_ser.iloc[-1] / nvda_ser.iloc[-2] - 1) * 100
        us["US4"] = {
            "name": "엔비디아 전일 대비",
            "level": level_from_gap(abs(ret), [3, 5, 8]),
            "description": f"전일 대비 {ret:+.2f}% (AI 대표주)"
        }

    # === US5: 10년물 금리 1주 변화 ===
    ust_ser = pd.to_numeric(df["ust10y"], errors="coerce").dropna()
    us["US5"] = {"name": "10년물 금리 1주 변화", "level": 0, "description": "데이터 부족"}
    if len(ust_ser) >= 5:
        delta = float(ust_ser.iloc[-1] - ust_ser.iloc[-5])
        # TNX 는 x10 단위로 올 때도 있어서 정규화
        if abs(ust_ser.iloc[-1]) > 20:  # 가격이 아닌 yield*10 포맷일 때
            delta = delta / 10
            cur = ust_ser.iloc[-1] / 10
        else:
            cur = ust_ser.iloc[-1]
        us["US5"] = {
            "name": "10년물 국채금리 1주 변화",
            "level": level_from_gap(abs(delta) * 100, [15, 25, 40]),  # bp 단위
            "description": f"현재 {cur:.2f}% / 1주 변화 {delta*100:+.0f}bp"
        }

    # === 종합 위험도 ===
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


# ===================== 대시보드 렌더링 =====================

COLOR = {
    "평상": "#1D9E75", "주의": "#BA7517", "경계": "#D85A30",
    "경고": "#A32D2D", "위험": "#501313", "대기": "#888"
}


def lvl_style(lvl):
    colors = ["#1D9E75", "#BA7517", "#D85A30", "#A32D2D"]
    labels = ["안전", "주의", "경계", "경고"]
    return colors[min(lvl, 3)], labels[min(lvl, 3)]


def render_dashboard(df, signals):
    df_plot = df.sort_values("date").tail(120).copy() if not df.empty else pd.DataFrame()

    def col(c):
        if df_plot.empty or c not in df_plot.columns:
            return []
        return pd.to_numeric(df_plot[c], errors="coerce").ffill().fillna(0).round(2).tolist()

    def col_raw(c):
        if df_plot.empty or c not in df_plot.columns:
            return []
        return pd.to_numeric(df_plot[c], errors="coerce").fillna(0).round(2).tolist()

    dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else []

    js_data = {
        "dates": dates,
        "kospi": col("kospi"), "kosdaq": col("kosdaq"),
        "samsung_ret": col_raw("samsung_ret_pct"), "hynix_ret": col_raw("hynix_ret_pct"),
        "vkospi": col("vkospi"),
        "credit": col("credit_balance_eok"), "forced": col_raw("forced_sale_eok"),
        "foreign": col_raw("foreign_net_eok"),
        "sp500": col("sp500"), "nasdaq": col("nasdaq"),
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

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>빚투 모니터 — 한국·미국 시장 위험 신호</title>
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
  .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                   gap: 12px; margin-bottom: 20px; }}
  .sig {{ background: white; padding: 14px 16px; border-radius: 10px; border-top: 4px solid; }}
  .sig-name {{ font-size: 12px; color: #888; margin-bottom: 4px; }}
  .sig-status {{ font-size: 17px; font-weight: 600; margin-bottom: 6px; }}
  .sig-desc {{ font-size: 12px; color: #555; line-height: 1.4; }}
  .chart {{ background: white; padding: 14px; border-radius: 12px; margin-bottom: 14px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }}
  @media (max-width: 768px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
  .footer {{ font-size: 11px; color: #aaa; margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; }}
</style>
</head>
<body>
<h1>빚투 모니터</h1>
<div class="meta">한국·미국 시장 위험 신호 자동 감시 · 업데이트 {last_date}</div>

<div class="tabs">
  <div class="tab active" data-tab="kr">🇰🇷 한국장 ({signals['label_kr']})</div>
  <div class="tab" data-tab="us">🇺🇸 미국장 ({signals['label_us']})</div>
</div>

<div id="pane-kr" class="pane active">
  <div class="overall" style="border-left: 6px solid {kr_color};">
    <div>
      <div class="overall-label">한국 종합 위험도</div>
      <div class="overall-value" style="color: {kr_color};">{signals['label_kr']}</div>
    </div>
    <div style="font-size: 14px; color: #888;">{signals['score_kr']} / {signals['max_kr']}점</div>
  </div>

  <div class="signal-grid">{kr_cards}</div>

  <div class="chart-grid">
    <div class="chart"><div id="c_kr_idx" style="height:280px;"></div></div>
    <div class="chart"><div id="c_kr_vkospi" style="height:280px;"></div></div>
  </div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_foreign" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_semi" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_sector" style="height:320px;"></div></div>
</div>

<div id="pane-us" class="pane">
  <div class="overall" style="border-left: 6px solid {us_color};">
    <div>
      <div class="overall-label">미국 종합 위험도</div>
      <div class="overall-value" style="color: {us_color};">{signals['label_us']}</div>
    </div>
    <div style="font-size: 14px; color: #888;">{signals['score_us']} / {signals['max_us']}점</div>
  </div>

  <div class="signal-grid">{us_cards}</div>

  <div class="chart-grid">
    <div class="chart"><div id="c_us_sp" style="height:280px;"></div></div>
    <div class="chart"><div id="c_us_nasdaq" style="height:280px;"></div></div>
  </div>
  <div class="chart-grid">
    <div class="chart"><div id="c_us_vix" style="height:280px;"></div></div>
    <div class="chart"><div id="c_us_nvda" style="height:280px;"></div></div>
  </div>
  <div class="chart"><div id="c_us_rate" style="height:280px;"></div></div>
</div>

<div class="footer">
  데이터: FinanceDataReader, 네이버 금융 증시자금동향/투자자별매매동향 · 평일 한국시간 17:30 자동 갱신 ·
  업종 바구니는 대표 종목 4개의 평균 일간 수익률 · 투자 권유 아님
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

// KR charts
plot('c_kr_idx', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.kosdaq, type: 'scatter', mode: 'lines', name: '코스닥', yaxis: 'y2', line: {{color: '#534AB7', width: 2}}}}
], '코스피·코스닥 추이', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '코스닥', overlaying: 'y', side: 'right'}}}});

plot('c_kr_vkospi', [
  {{x: D.dates, y: D.vkospi, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VKOSPI',
    line: {{color: '#A32D2D', width: 2}}, fillcolor: 'rgba(163,45,45,0.1)'}}
], 'VKOSPI 공포지수 (20↑주의 / 30↑경계 / 40↑공포)', {{
  shapes: [
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 20, y1: 20, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}},
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 30, y1: 30, line: {{color: '#D85A30', width: 1, dash: 'dot'}}}},
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 40, y1: 40, line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}
  ]
}});

plot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', mode: 'lines', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
], '신호 ② 코스피 vs 신용잔고', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '잔고(억)', overlaying: 'y', side: 'right'}}}});

plot('c_kr_forced', [{{
  x: D.dates, y: D.forced, type: 'bar', name: '반대매매(억)',
  marker: {{color: D.forced.map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}
}}], '신호 ① 일일 반대매매 (200↑주의 / 400↑경계 / 600↑경고)');

plot('c_kr_foreign', [{{
  x: D.dates, y: D.foreign, type: 'bar', name: '외국인 순매수(억)',
  marker: {{color: D.foreign.map(v => v < 0 ? '#A32D2D' : '#1D9E75')}}
}}], '신호 ④ 외국인 일별 순매수/매도 (코스피+코스닥, 억원)');

plot('c_kr_semi', [
  {{x: D.dates, y: D.samsung_ret, type: 'bar', name: '삼성전자 %', marker: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.hynix_ret, type: 'bar', name: 'SK하이닉스 %', marker: {{color: '#534AB7'}}}}
], '신호 ③ 반도체 양대장 일간 변동률', {{
  barmode: 'group', yaxis: {{zeroline: true}},
  shapes: [{{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: -3, y1: -3, line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}]
}});

plot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', mode: 'lines', name: '반도체', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.sec_방산조선, type: 'scatter', mode: 'lines', name: '방산조선', line: {{color: '#534AB7'}}}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', mode: 'lines', name: '바이오', line: {{color: '#1D9E75'}}}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', mode: 'lines', name: '2차전지', line: {{color: '#BA7517'}}}},
  {{x: D.dates, y: D.sec_금융, type: 'scatter', mode: 'lines', name: '금융', line: {{color: '#888'}}}}
], '업종 바구니 일간 수익률 (%, 대표 종목 4개 평균)');

// US charts
plot('c_us_sp', [
  {{x: D.dates, y: D.sp500, type: 'scatter', mode: 'lines', name: 'S&P 500', line: {{color: '#185FA5', width: 2}}}}
], 'S&P 500');

plot('c_us_nasdaq', [
  {{x: D.dates, y: D.nasdaq, type: 'scatter', mode: 'lines', name: '나스닥', line: {{color: '#534AB7', width: 2}}}}
], '나스닥 종합');

plot('c_us_vix', [
  {{x: D.dates, y: D.vix, type: 'scatter', mode: 'lines', fill: 'tozeroy', name: 'VIX',
    line: {{color: '#A32D2D'}}, fillcolor: 'rgba(163,45,45,0.1)'}}
], 'VIX 공포지수', {{
  shapes: [
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 20, y1: 20, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}},
    {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 30, y1: 30, line: {{color: '#D85A30', width: 1, dash: 'dot'}}}}
  ]
}});

plot('c_us_nvda', [
  {{x: D.dates, y: D.nvda, type: 'scatter', mode: 'lines', name: 'NVDA', line: {{color: '#1D9E75', width: 2}}}}
], '엔비디아 (NVDA)');

plot('c_us_rate', [
  {{x: D.dates, y: D.ust10y, type: 'scatter', mode: 'lines', name: '10Y Yield', line: {{color: '#BA7517', width: 2}}}}
], '미국 10년물 국채금리 (%)');

// Tab switching
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', e => {{
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
  e.target.classList.add('active');
  document.getElementById('pane-' + e.target.dataset.tab).classList.add('active');
  // Trigger plotly resize after tab switch
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
    print(json.dumps(signals, indent=2, ensure_ascii=False, default=str))

    try:
        render_dashboard(df, signals)
    except Exception as e:
        print(f"[error] render_dashboard: {e}")
        import traceback; traceback.print_exc()

    print("Done.")


if __name__ == "__main__":
    main()
