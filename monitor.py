"""
빚투 모니터 v6 - 주말/휴일 0 처리 오류 완벽 해결 및 봇 차단 강력 우회
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


fetch_kofia_deposit_api():
    """공공데이터포털(금융위원회) API를 이용한 신용잔고/반대매매 수집"""
    result = {"credit_balance_eok": None, "forced_sale_eok": None}
    
    # 깃허브 Secrets에 저장해둔 API 키를 안전하게 불러옴
    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key:
        print("[error] API Key가 설정되지 않았습니다.")
        return result

    try:
        # 오픈 API 요청 주소 (JSON 형태로 요청)
        url = "http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockMarginTradingInfo"
        
        # 오늘 기준으로 최근 5일치 데이터를 요청하여 가장 최신 값을 찾음
        params = {
            "serviceKey": urllib.parse.unquote(api_key), # Encoding 키를 디코딩하여 사용
            "resultType": "json",
            "numOfRows": "5", 
            "pageNo": "1",
        }
        
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        
        # 응답 데이터 파싱
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        
        if items:
            # 가장 최신 날짜(첫 번째 리스트)의 데이터를 가져옴
            latest_data = items[0]
            
            # crdtBlncAmt: 신용거래 융자 잔고금액 (단위: 원) -> 억원 단위로 변환
            if "crdtBlncAmt" in latest_data:
                result["credit_balance_eok"] = int(float(latest_data["crdtBlncAmt"]) / 100000000)
            
            # nxtDdOpnprcFrcedRdptnAmt: 반대매매 금액 (단위: 원) -> 억원 단위로 변환
            # (API 명세서에 따라 필드명이 다를 수 있으므로 데이터 확인 필요)
            if "nxtDdOpnprcFrcedRdptnAmt" in latest_data:
                result["forced_sale_eok"] = int(float(latest_data["nxtDdOpnprcFrcedRdptnAmt"]) / 100000000)

        print(f"  API deposit: credit={result['credit_balance_eok']}, forced={result['forced_sale_eok']}")
        
    except Exception as e:
        print(f"  [error] API deposit fetch failed: {e}")
        
    return result



def fetch_sector_basket(tickers):
    closes = []
    for t in tickers:
        s = safe(f"ticker_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=400), default=pd.Series(dtype=float))
        if not s.empty:
            closes.append(s)
    if not closes:
        return pd.Series(dtype=float)
    df = pd.concat(closes, axis=1).ffill()
    daily_ret = df.pct_change().mean(axis=1).fillna(0)
    cum_index = (1 + daily_ret).cumprod() * 100
    return cum_index


MAIN_COLS = [
    "date", "kospi", "kosdaq", "samsung", "hynix", "vkospi",
    "credit_balance_eok", "forced_sale_eok", "foreign_net_eok",
    "samsung_ret_pct", "hynix_ret_pct",
    "sp500", "nasdaq", "vix", "nvda", "ust10y",
    "sec_반도체", "sec_방산조선", "sec_바이오", "sec_2차전지", "sec_금융",
]


def load_history():
    fp = DATA_DIR / "history.csv"
    if not fp.exists(): return pd.DataFrame(columns=MAIN_COLS)
    try:
        df = pd.read_csv(fp)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])
        for c in MAIN_COLS:
            if c not in df.columns: df[c] = None
        return df[MAIN_COLS]
    except Exception:
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
        sectors[f"sec_{name}"] = safe(f"sector_{name}", lambda tt=tickers: fetch_sector_basket(tt), default=pd.Series(dtype=float)).rename(f"sec_{name}")



    series_dict = {
        "kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix,
        "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y,
        **sectors
    }
    df = pd.DataFrame(series_dict).reset_index().rename(columns={"index": "date"})
    df["samsung_ret_pct"] = df["samsung"].pct_change() * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change() * 100
    df["credit_balance_eok"] = None
    df["forced_sale_eok"] = None
    df["foreign_net_eok"] = None
    df["vkospi"] = None

    if not df.empty and deposit.get("credit_balance_eok"):
        mask = df["date"] == df["date"].max()
        df.loc[mask, "credit_balance_eok"] = deposit["credit_balance_eok"]
        df.loc[mask, "forced_sale_eok"] = deposit["forced_sale_eok"]
        if vkospi_today: df.loc[mask, "vkospi"] = vkospi_today

    for date, val in foreign_flow.items():
        if date in df["date"].values:
            df.loc[df["date"] == date, "foreign_net_eok"] = val

    for c in MAIN_COLS:
        if c not in df.columns: df[c] = None
    
    history = load_history()
    combined = save_history(pd.concat([history, df[MAIN_COLS]], ignore_index=True))
    return combined


def level_from_gap(v, thresholds):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 0
    if v >= thresholds[2]: return 3
    if v >= thresholds[1]: return 2
    if v >= thresholds[0]: return 1
    return 0


def pct_deviation_from_ma(series, window=200):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < window or s.rolling(window).mean().iloc[-1] == 0: return None
    return float((s.iloc[-1] / s.rolling(window).mean().iloc[-1] - 1) * 100)


def compute_signals(df):
    if df.empty: return {"kr": {}, "us": {}, "score_kr": 0, "max_kr": 18, "score_us": 0, "max_us": 15, "label_kr": "대기", "label_us": "대기"}
    df = df.sort_values("date").reset_index(drop=True)
    kr, us = {}, {}

    forced_series = pd.to_numeric(df["forced_sale_eok"], errors="coerce").dropna()
    avg20 = float(forced_series.tail(20).mean()) if len(forced_series) else 0.0
    today_forced = float(df.iloc[-1].get("forced_sale_eok") or 0)
    kr["KR1"] = {"name": "반대매매 일평균", "level": level_from_gap(max(today_forced, avg20), [200, 400, 600]), "description": f"오늘 {today_forced:.0f}억 / 20일 평균 {avg20:.0f}억"}

    kospi_ser, credit_ser = pd.to_numeric(df["kospi"], errors="coerce").dropna(), pd.to_numeric(df["credit_balance_eok"], errors="coerce").dropna()
    kr["KR2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}
    if len(kospi_ser) >= 30 and len(credit_ser) >= 30:
        kospi_30d, credit_30d = (kospi_ser.iloc[-1]/kospi_ser.iloc[-30]-1)*100, (credit_ser.iloc[-1]/credit_ser.iloc[-30]-1)*100
        kr["KR2"] = {"name": "신용잔고 증가율 추월", "level": level_from_gap(credit_30d - kospi_30d, [0.5, 1.0, 2.0]), "description": f"코스피 {kospi_30d:+.1f}% vs 신용 {credit_30d:+.1f}%"}

    both_drop = int(((pd.to_numeric(df.tail(5)["samsung_ret_pct"], errors="coerce") <= -3) & (pd.to_numeric(df.tail(5)["hynix_ret_pct"], errors="coerce") <= -3)).sum())
    kr["KR3"] = {"name": "반도체 양대장 동시 -3%", "level": 3 if both_drop >= 2 else 2 if both_drop == 1 else 0, "description": f"최근 5일 동시 -3%: {both_drop}회"}

    foreign_ser = pd.to_numeric(df["foreign_net_eok"], errors="coerce").dropna()
    kr["KR4"] = {"name": "외국인 7일 순매도", "level": level_from_gap(-float(foreign_ser.tail(7).sum())/10000 if len(foreign_ser)>=7 else 0, [1.0, 3.0, 5.0]), "description": f"7일 합계 {float(foreign_ser.tail(7).sum())/10000 if len(foreign_ser)>=7 else 0:+.2f}조"}

    dev_kospi = pct_deviation_from_ma(df["kospi"], 200)
    kr["KR5"] = {"name": "코스피-200일선 괴리율", "level": level_from_gap(abs(dev_kospi) if dev_kospi else 0, [10, 20, 30]), "description": f"200일선 대비 {dev_kospi if dev_kospi else 0:+.1f}%"}

    v = float(pd.to_numeric(df["vkospi"], errors="coerce").dropna().iloc[-1]) if len(pd.to_numeric(df["vkospi"], errors="coerce").dropna()) else 0
    kr["KR6"] = {"name": "VKOSPI 공포지수", "level": level_from_gap(v, [20, 30, 40]), "description": f"현재 {v:.1f}"}

    kr_score = sum(s["level"] for s in kr.values())
    
    def label(score, maxi):
        pct = score / maxi if maxi else 0
        return "위험" if pct >= 0.7 else "경고" if pct >= 0.5 else "경계" if pct >= 0.3 else "주의" if pct >= 0.15 else "평상"

    return {"kr": kr, "us": {}, "score_kr": kr_score, "max_kr": len(kr) * 3, "score_us": 0, "max_us": 15, "label_kr": label(kr_score, len(kr)*3), "label_us": "대기"}


def compute_regime(index_series):
    s = pd.to_numeric(index_series, errors="coerce").dropna()
    res = {"short": {"label": "대기", "score": 0, "reasons": []}, "mid": {"label": "대기", "score": 0, "reasons": []}, "long": {"label": "대기", "score": 0, "reasons": []}}
    if len(s) < 200: return res
    cur, ma50, ma200 = s.iloc[-1], s.rolling(50).mean().iloc[-1], s.rolling(200).mean().iloc[-1]
    
    ret30d = (cur / s.iloc[-30] - 1) * 100
    res["short"]["score"] = 2 if ret30d > 5 else 1 if ret30d > 2 else -2 if ret30d < -5 else -1 if ret30d < -2 else 0
    
    res["mid"]["score"] = 2 if ma50 > ma200 * 1.02 else 1 if ma50 > ma200 else -2 if ma50 < ma200 * 0.98 else -1
    
    ma200_slope = (ma200 / s.rolling(200).mean().iloc[-31] - 1) * 100 if len(s) > 230 else 0
    res["long"]["score"] = 2 if ma200_slope > 3 else 1 if ma200_slope > 0 else -2 if ma200_slope < -3 else -1

    for k in res: res[k]["label"] = "강세" if res[k]["score"] >= 2 else "약한 강세" if res[k]["score"] >= 1 else "약세" if res[k]["score"] <= -2 else "약한 약세" if res[k]["score"] <= -1 else "중립"
    return res


def render_dashboard(df, signals, regime_kr, regime_us):
    # 핵심 필터: 코스피 데이터가 없는 날(주말/휴일)을 아예 삭제하여 차트 바닥 찍기 원천 차단
    df_plot = df.dropna(subset=["kospi"]).sort_values("date").tail(350).copy() if not df.empty else pd.DataFrame()

    def col(c):
        if df_plot.empty or c not in df_plot.columns: return []
        # 누락된 데이터를 강제로 0으로 만들지 않고 이전 값으로 부드럽게 이음(ffill)
        s = pd.to_numeric(df_plot[c], errors="coerce").ffill().bfill()
        return [None if pd.isna(x) else round(x, 2) for x in s]

    js_data = {
        "dates": [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else [],
        "kospi": col("kospi"), "kosdaq": col("kosdaq"),
        "samsung": col("samsung"), "hynix": col("hynix"),
        "vkospi": col("vkospi"), "credit": col("credit_balance_eok"), 
        "forced": col("forced_sale_eok"), "foreign": col("foreign_net_eok"),
        "sec_반도체": col("sec_반도체"), "sec_방산조선": col("sec_방산조선"),
        "sec_바이오": col("sec_바이오"), "sec_2차전지": col("sec_2차전지"), "sec_금융": col("sec_금융"),
    }

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><title>빚투 모니터</title><script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>body {{ font-family: system-ui; max-width: 1200px; margin: 0 auto; padding: 24px; background: #fafafa; }} .chart {{ background: white; padding: 14px; border-radius: 12px; margin-bottom: 14px; }}</style>
</head><body>
<h2>빚투 모니터 (한국장)</h2>
  <div class="chart"><div id="c_kr_idx" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_vkospi" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_foreign" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_semi" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_sector" style="height:320px;"></div></div>
<script>
const D = {json.dumps(js_data, ensure_ascii=False)};
const base = {{ margin: {{t: 30, r: 45, b: 35, l: 50}}, font: {{family: 'system-ui'}}, paper_bgcolor: 'white', plot_bgcolor: 'white' }};
function plot(id, traces, title) {{ Plotly.newPlot(id, traces, {{...base, title: {{text: title}}}}); }}

function normalize(arr) {{
  const valid = arr.filter(v => v !== null && v !== 0);
  const first = valid.length > 0 ? valid[0] : 1;
  return arr.map(v => (v !== null && v !== 0) ? (v / first) * 100 : null);
}}

plot('c_kr_idx', [
  {{x: D.dates, y: D.kospi, type: 'scatter', name: '코스피', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.kosdaq, type: 'scatter', name: '코스닥', yaxis: 'y2', line: {{color: '#534AB7'}}}}
], '코스피 & 코스닥');

plot('c_kr_vkospi', [{{x: D.dates, y: D.vkospi, type: 'scatter', fill: 'tozeroy', name: 'VKOSPI', line: {{color: '#A32D2D'}}}}], 'VKOSPI 공포지수');
plot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', name: '코스피', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', name: '신용잔고', yaxis: 'y2', line: {{color: '#993C1D'}}}}
], '코스피 vs 신용잔고(억)');

plot('c_kr_forced', [{{x: D.dates, y: D.forced, type: 'bar', name: '반대매매'}}], '일일 반대매매(억)');
plot('c_kr_foreign', [{{x: D.dates, y: D.foreign, type: 'bar', name: '외국인'}}], '외국인 순매수(억)');

plot('c_kr_semi', [
  {{x: D.dates, y: normalize(D.kospi), type: 'scatter', name: '코스피', line: {{color: '#888', dash: 'dot'}}}},
  {{x: D.dates, y: normalize(D.samsung), type: 'scatter', name: '삼성전자', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: normalize(D.hynix), type: 'scatter', name: 'SK하이닉스', line: {{color: '#534AB7'}}}}
], '반도체 누적 추세 (Base 100)');

plot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', name: '반도체'}},
  {{x: D.dates, y: D.sec_방산조선, type: 'scatter', name: '방산조선'}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', name: '바이오'}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', name: '2차전지'}}
], '업종 바구니 누적 추세 (Base 100)');
</script></body></html>"""
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")

def main():
    df = update_data()
    render_dashboard(df, compute_signals(df), compute_regime(df["kospi"]), {})

if __name__ == "__main__":
    main()
