"""
빚투 모니터 마스터 버전 - UI 원상복구 + 누적추세선 + yfinance/pykrx 적용
"""

import os
import json
import time
import datetime as dt
import re
import urllib.parse
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import FinanceDataReader as fdr

# 필수 라이브러리 (requirements.txt에 등록 필수)
try:
    from pykrx import stock
except ImportError:
    stock = None
    print("[error] pykrx is not installed.")

try:
    import yfinance as yf
except ImportError:
    yf = None
    print("[error] yfinance is not installed.")

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

def fetch_us_yf(symbol, name):
    """미국장은 차단 우회를 위해 yfinance를 사용합니다."""
    if yf is None: return pd.Series(dtype=float, name=name)
    try:
        start = (TODAY - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        df = yf.download(symbol, start=start, progress=False)
        if df.empty: return pd.Series(dtype=float, name=name)
        s = df["Close"]
        if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
        s.index = pd.to_datetime(s.index).date
        return s.rename(name).dropna()
    except Exception as e:
        print(f"[error] fetch_us_yf {name}: {e}")
        return pd.Series(dtype=float, name=name)

def fetch_vkospi_pykrx():
    if stock is None: return None
    try:
        start_date = (TODAY - dt.timedelta(days=10)).strftime("%Y%m%d")
        end_date = TODAY.strftime("%Y%m%d")
        df = stock.get_index_ohlcv(start_date, end_date, "2004")
        if not df.empty: return float(df['종가'].iloc[-1])
    except Exception: pass
    return None

def fetch_kofia_deposit_api():
    result = {"credit_balance_eok": None, "forced_sale_eok": None}
    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key: return result
    try:
        url = "http://apis.data.go.kr/1160100/service/GetKofiaStatsInfoService/getStkMktFundTrend"
        params = {"serviceKey": urllib.parse.unquote(api_key), "resultType": "json", "numOfRows": "5", "pageNo": "1"}
        response = requests.get(url, params=params, timeout=15)
        items = response.json().get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if items and "crdtLoanBal" in items[0]:
            val = float(items[0]["crdtLoanBal"])
            if val > 100000000000: result["credit_balance_eok"] = int(val / 100000000)
    except Exception: pass
    return result

def fetch_foreign_flow_pykrx():
    result = {}
    if stock is None: return result
    try:
        start_date = (TODAY - dt.timedelta(days=15)).strftime("%Y%m%d")
        end_date = TODAY.strftime("%Y%m%d")
        df_kpi = stock.get_market_trading_value_by_date(start_date, end_date, "KOSPI")
        df_kdq = stock.get_market_trading_value_by_date(start_date, end_date, "KOSDAQ")
        for date in df_kpi.index:
            v_kpi = df_kpi.loc[date, '외국인합계'] if '외국인합계' in df_kpi.columns else 0
            v_kdq = df_kdq.loc[date, '외국인합계'] if '외국인합계' in df_kdq.columns else 0
            result[date.date()] = int((v_kpi + v_kdq) / 100000000)
    except Exception: pass
    return result

def fetch_sector_basket(tickers):
    closes = [safe(f"t_{t}", lambda tt=t: fetch_fdr(tt, name=tt, days=400), pd.Series(dtype=float)) for t in tickers]
    df = pd.concat([c for c in closes if not c.empty], axis=1).ffill()
    if df.empty: return pd.Series(dtype=float)
    return (1 + df.pct_change().mean(axis=1).fillna(0)).cumprod() * 100

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
        return df.dropna(subset=["date"])[MAIN_COLS]
    except Exception: return pd.DataFrame(columns=MAIN_COLS)

def save_history(df):
    fp = DATA_DIR / "history.csv"
    if df.empty: return df
    df = df[df["date"] >= (TODAY - dt.timedelta(days=LOOKBACK_DAYS * 2))].sort_values("date").drop_duplicates("date", keep="last")
    df.to_csv(fp, index=False)
    return df

def update_data():
    print(f"[{TODAY}] Fetching all data...")
    kospi = safe("kospi",  lambda: fetch_fdr("KS11", "kospi"),  pd.Series(dtype=float, name="kospi"))
    kosdaq = safe("kosdaq", lambda: fetch_fdr("KQ11", "kosdaq"), pd.Series(dtype=float, name="kosdaq"))
    samsung = safe("samsung", lambda: fetch_fdr("005930", "samsung"), pd.Series(dtype=float, name="samsung"))
    hynix = safe("hynix", lambda: fetch_fdr("000660", "hynix"), pd.Series(dtype=float, name="hynix"))

    sp500 = safe("sp500", lambda: fetch_us_yf("^GSPC", "sp500"), pd.Series(dtype=float, name="sp500"))
    nasdaq = safe("nasdaq", lambda: fetch_us_yf("^IXIC", "nasdaq"), pd.Series(dtype=float, name="nasdaq"))
    vix = safe("vix", lambda: fetch_us_yf("^VIX", "vix"), pd.Series(dtype=float, name="vix"))
    nvda = safe("nvda", lambda: fetch_us_yf("NVDA", "nvda"), pd.Series(dtype=float, name="nvda"))
    ust10y = safe("ust10y", lambda: fetch_us_yf("^TNX", "ust10y"), pd.Series(dtype=float, name="ust10y"))

    sectors = {f"sec_{k}": safe(f"sec_{k}", lambda tt=v: fetch_sector_basket(tt), pd.Series(dtype=float)).rename(f"sec_{k}") for k, v in SECTOR_BASKETS.items()}

    deposit = safe("deposit", fetch_kofia_deposit_api, {"credit_balance_eok": None, "forced_sale_eok": None})
    foreign_flow = safe("foreign", fetch_foreign_flow_pykrx, {})
    vkospi_today = safe("vkospi_pykrx", fetch_vkospi_pykrx, None)

    df = pd.DataFrame({"kospi": kospi, "kosdaq": kosdaq, "samsung": samsung, "hynix": hynix, "sp500": sp500, "nasdaq": nasdaq, "vix": vix, "nvda": nvda, "ust10y": ust10y, **sectors}).reset_index().rename(columns={"index": "date"})
    
    df["samsung_ret_pct"], df["hynix_ret_pct"] = df["samsung"].pct_change() * 100, df["hynix"].pct_change() * 100
    df["credit_balance_eok"], df["forced_sale_eok"], df["foreign_net_eok"], df["vkospi"] = None, None, None, None

    if not df.empty and deposit.get("credit_balance_eok"):
        df.loc[df["date"] == df["date"].max(), ["credit_balance_eok", "forced_sale_eok"]] = deposit["credit_balance_eok"], deposit["forced_sale_eok"]
        if vkospi_today: df.loc[df["date"] == df["date"].max(), "vkospi"] = vkospi_today

    for d, v in foreign_flow.items(): df.loc[df["date"] == d, "foreign_net_eok"] = v
    for c in MAIN_COLS: 
        if c not in df.columns: df[c] = None

    return save_history(pd.concat([load_history(), df[MAIN_COLS]], ignore_index=True))

def level_from_gap(v, thresholds):
    if v is None or pd.isna(v): return 0
    return 3 if v >= thresholds[2] else 2 if v >= thresholds[1] else 1 if v >= thresholds[0] else 0

def pct_dev(s, w=200):
    s = pd.to_numeric(s, errors="coerce").dropna()
    return float((s.iloc[-1] / s.rolling(w).mean().iloc[-1] - 1) * 100) if len(s) >= w else None

def compute_signals(df):
    if df.empty: return {"kr": {}, "us": {}, "score_kr": 0, "max_kr": 18, "score_us": 0, "max_us": 15, "label_kr": "대기", "label_us": "대기"}
    df = df.sort_values("date").reset_index(drop=True)
    kr, us = {}, {}

    f_ser = pd.to_numeric(df["forced_sale_eok"], errors="coerce").dropna()
    avg20, today_f = float(f_ser.tail(20).mean()) if len(f_ser) else 0.0, float(df.iloc[-1].get("forced_sale_eok") or 0)
    kr["KR1"] = {"name": "반대매매 일평균", "level": level_from_gap(max(today_f, avg20), [200, 400, 600]), "description": f"오늘 {today_f:.0f}억 / 20일 평균 {avg20:.0f}억"}

    k_ser, c_ser = pd.to_numeric(df["kospi"], errors="coerce").dropna(), pd.to_numeric(df["credit_balance_eok"], errors="coerce").dropna()
    kr["KR2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}
    if len(k_ser) >= 30 and len(c_ser) >= 30:
        k30, c30 = (k_ser.iloc[-1]/k_ser.iloc[-30]-1)*100, (c_ser.iloc[-1]/c_ser.iloc[-30]-1)*100
        kr["KR2"] = {"name": "신용잔고 증가율이 코스피 추월", "level": level_from_gap(c30 - k30, [0.5, 1.0, 2.0]), "description": f"30일 코스피 {k30:+.1f}% vs 신용 {c30:+.1f}%"}

    b_drop = int(((pd.to_numeric(df.tail(5)["samsung_ret_pct"], errors="coerce") <= -3) & (pd.to_numeric(df.tail(5)["hynix_ret_pct"], errors="coerce") <= -3)).sum())
    kr["KR3"] = {"name": "반도체 양대장 동시 -3%", "level": 3 if b_drop >= 2 else 2 if b_drop == 1 else 0, "description": f"최근 5일 동시 -3%: {b_drop}회"}

    fn_ser = pd.to_numeric(df["foreign_net_eok"], errors="coerce").dropna()
    kr["KR4"] = {"name": "외국인 7일 누적 순매도", "level": level_from_gap(-float(fn_ser.tail(7).sum())/10000 if len(fn_ser)>=7 else 0, [1.0, 3.0, 5.0]), "description": f"7일 합계 {float(fn_ser.tail(7).sum())/10000 if len(fn_ser)>=7 else 0:+.2f}조"}

    dk = pct_dev(df["kospi"])
    kr["KR5"] = {"name": "코스피-200일선 괴리율", "level": level_from_gap(abs(dk) if dk else 0, [10, 20, 30]), "description": f"200일선 대비 {dk if dk else 0:+.1f}%"}

    vk = float(pd.to_numeric(df["vkospi"], errors="coerce").dropna().iloc[-1]) if len(pd.to_numeric(df["vkospi"], errors="coerce").dropna()) else 0
    kr["KR6"] = {"name": "VKOSPI 공포지수", "level": level_from_gap(vk, [20, 30, 40]), "description": f"현재 {vk:.1f}"}

    sp_ser = pd.to_numeric(df["sp500"], errors="coerce").dropna()
    us["US1"] = {"name": "S&P500 일간 변동성", "level": 0, "description": "데이터 부족"}
    if len(sp_ser) >= 2:
        r = (sp_ser.iloc[-1] / sp_ser.iloc[-2] - 1) * 100
        us["US1"] = {"name": "S&P500 전일 대비", "level": level_from_gap(abs(r), [1, 2, 3]), "description": f"전일 대비 {r:+.2f}%"}

    dn = pct_dev(df["nasdaq"])
    us["US2"] = {"name": "나스닥-200일선 괴리율", "level": level_from_gap(abs(dn) if dn else 0, [10, 20, 30]), "description": f"200일선 대비 {dn if dn else 0:+.1f}%"}

    vv = float(pd.to_numeric(df["vix"], errors="coerce").dropna().iloc[-1]) if len(pd.to_numeric(df["vix"], errors="coerce").dropna()) else 0
    us["US3"] = {"name": "VIX 공포지수", "level": level_from_gap(vv, [20, 30, 40]), "description": f"현재 {vv:.1f}"}

    nv_ser = pd.to_numeric(df["nvda"], errors="coerce").dropna()
    us["US4"] = {"name": "엔비디아 일간", "level": 0, "description": "데이터 부족"}
    if len(nv_ser) >= 2:
        r = (nv_ser.iloc[-1] / nv_ser.iloc[-2] - 1) * 100
        us["US4"] = {"name": "엔비디아 전일 대비", "level": level_from_gap(abs(r), [3, 5, 8]), "description": f"전일 대비 {r:+.2f}%"}

    u_ser = pd.to_numeric(df["ust10y"], errors="coerce").dropna()
    us["US5"] = {"name": "10년물 국채금리 변화", "level": 0, "description": "데이터 부족"}
    if len(u_ser) >= 5:
        d = float(u_ser.iloc[-1] - u_ser.iloc[-5])
        us["US5"] = {"name": "10년물 국채금리 1주 변화", "level": level_from_gap(abs(d) * 100, [15, 25, 40]), "description": f"현재 {float(u_ser.iloc[-1]):.2f}% / 1주 변화 {d*100:+.0f}bp"}

    sk, su = sum(s["level"] for s in kr.values()), sum(s["level"] for s in us.values())
    lbl = lambda s, m: "위험" if (s/m if m else 0)>=0.7 else "경고" if (s/m if m else 0)>=0.5 else "경계" if (s/m if m else 0)>=0.3 else "주의" if (s/m if m else 0)>=0.15 else "평상"
    return {"kr": kr, "us": us, "score_kr": sk, "max_kr": len(kr)*3, "score_us": su, "max_us": len(us)*3, "label_kr": lbl(sk, len(kr)*3), "label_us": lbl(su, len(us)*3)}

def compute_regime(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    r = {"short": {"label": "대기", "score": 0, "reasons": []}, "mid": {"label": "대기", "score": 0, "reasons": []}, "long": {"label": "대기", "score": 0, "reasons": []}}
    if len(s) < 200: return r
    c, m50, m200 = s.iloc[-1], s.rolling(50).mean().iloc[-1], s.rolling(200).mean().iloc[-1]
    
    r30 = (c / s.iloc[-30] - 1) * 100
    if r30 > 5: r["short"]["score"] += 2; r["short"]["reasons"].append(f"최근 30일 +{r30:.1f}%")
    elif r30 < -5: r["short"]["score"] -= 2; r["short"]["reasons"].append(f"최근 30일 {r30:.1f}%")

    if m50 > m200 * 1.02: r["mid"]["score"] += 2; r["mid"]["reasons"].append("50일선이 200일선 위")
    elif m50 < m200 * 0.98: r["mid"]["score"] -= 2; r["mid"]["reasons"].append("50일선이 200일선 아래")
    
    m200_slope = (m200 / s.rolling(200).mean().iloc[-31] - 1) * 100 if len(s) > 230 else 0
    if m200_slope > 3: r["long"]["score"] += 2; r["long"]["reasons"].append(f"200일선 상승 +{m200_slope:.1f}%")
    elif m200_slope < -3: r["long"]["score"] -= 2; r["long"]["reasons"].append(f"200일선 하락 {m200_slope:.1f}%")

    for k in r: r[k]["label"] = "강세" if r[k]["score"] >= 2 else "약한 강세" if r[k]["score"] >= 1 else "약세" if r[k]["score"] <= -2 else "약한 약세" if r[k]["score"] <= -1 else "중립"
    return r

COLOR = {"평상": "#1D9E75", "주의": "#BA7517", "경계": "#D85A30", "경고": "#A32D2D", "위험": "#501313", "대기": "#888"}
REGIME_COLOR = {"강세": "#1D9E75", "약한 강세": "#97C459", "중립": "#888780", "약한 약세": "#EF9F27", "약세": "#A32D2D", "대기": "#aaa"}
def lvl_style(lvl): return ["#1D9E75", "#BA7517", "#D85A30", "#A32D2D"][min(lvl, 3)], ["안전", "주의", "경계", "경고"][min(lvl, 3)]

def render_regime_block(regime, title):
    cards = []
    for k, lbl in [("short", "단기 (1~3개월)"), ("mid", "중기 (3~6개월)"), ("long", "장기 (6~12개월)")]:
        r = regime.get(k, {"label": "대기", "score": 0, "reasons": []})
        c = REGIME_COLOR.get(r["label"], "#888")
        html = "<br>".join(f"· {x}" for x in r["reasons"]) if r["reasons"] else "· 데이터 수집 중"
        cards.append(f'<div class="regime-card" style="border-top-color: {c};"><div class="regime-name">{lbl}</div><div class="regime-value" style="color: {c};">{r["label"]}</div><div class="regime-score">점수 {r["score"]:+d}</div><div class="regime-reasons">{html}</div></div>')
    return f'<div class="regime-block"><div class="regime-title">{title}</div><div class="regime-grid">{"".join(cards)}</div></div>'

def render_dashboard(df, signals, regime_kr, regime_us):
    df_plot = df.dropna(subset=["kospi"]).sort_values("date").tail(350).copy() if not df.empty else pd.DataFrame()
    def col(c): return [None if pd.isna(x) else round(x, 2) for x in pd.to_numeric(df_plot[c], errors="coerce").ffill().bfill()] if not df_plot.empty and c in df_plot.columns else []
    def ma_series(c, w=200): return [None if pd.isna(x) else round(x, 2) for x in pd.to_numeric(df[c], errors="coerce").ffill().bfill().rolling(w).mean().loc[df_plot.index]] if not df.empty and c in df.columns else []

    js_data = {
        "dates": [d.strftime("%Y-%m-%d") for d in df_plot["date"]] if not df_plot.empty else [],
        "kospi": col("kospi"), "kospi_ma200": ma_series("kospi", 200), "kosdaq": col("kosdaq"),
        "samsung": col("samsung"), "hynix": col("hynix"), "vkospi": col("vkospi"),
        "credit": col("credit_balance_eok"), "forced": col("forced_sale_eok"), "foreign": col("foreign_net_eok"),
        "sp500": col("sp500"), "sp500_ma200": ma_series("sp500", 200), "nasdaq": col("nasdaq"), "nasdaq_ma200": ma_series("nasdaq", 200),
        "vix": col("vix"), "nvda": col("nvda"), "ust10y": col("ust10y"),
        **{k: col(k) for k in [f"sec_{x}" for x in SECTOR_BASKETS.keys()]}
    }
    
    def render_signals(sigs):
        return "\n".join([f'<div class="sig" style="border-color: {lvl_style(s["level"])[0]}"><div class="sig-name">{k}. {s["name"]}</div><div class="sig-status" style="color: {lvl_style(s["level"])[0]}">{lvl_style(s["level"])[1]}</div><div class="sig-desc">{s["description"]}</div></div>' for k, s in sigs.items()])

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>빚투 모니터 — 추세 전망 & 위험 신호</title><script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; max-width: 1200px; margin: 0 auto; padding: 24px; color: #222; background: #fafafa; }}
  h1 {{ font-size: 26px; font-weight: 600; margin: 0 0 6px; }}
  .meta {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
  .tabs {{ display: flex; gap: 4px; margin-bottom: 20px; border-bottom: 2px solid #eee; }}
  .tab {{ padding: 12px 22px; cursor: pointer; font-weight: 500; font-size: 15px; color: #666; border-bottom: 2px solid transparent; margin-bottom: -2px; }}
  .tab.active {{ color: #222; border-bottom-color: #222; }}
  .pane {{ display: none; }} .pane.active {{ display: block; }}
  .overall {{ background: white; padding: 18px 22px; border-radius: 12px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; }}
  .overall-label {{ font-size: 13px; color: #888; }} .overall-value {{ font-size: 28px; font-weight: 600; }}
  .regime-block {{ background: white; padding: 18px 20px; border-radius: 12px; margin-bottom: 20px; }}
  .regime-title {{ font-size: 15px; font-weight: 600; margin-bottom: 14px; color: #333; }}
  .regime-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
  .regime-card {{ background: #fafafa; padding: 14px 16px; border-radius: 10px; border-top: 4px solid; }}
  .regime-name {{ font-size: 12px; color: #888; margin-bottom: 4px; }} .regime-value {{ font-size: 20px; font-weight: 600; margin-bottom: 2px; }}
  .regime-score {{ font-size: 11px; color: #aaa; margin-bottom: 8px; }} .regime-reasons {{ font-size: 12px; color: #555; line-height: 1.6; }}
  .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 20px; }}
  .sig {{ background: white; padding: 14px 16px; border-radius: 10px; border-top: 4px solid; }}
  .sig-name {{ font-size: 12px; color: #888; margin-bottom: 4px; }} .sig-status {{ font-size: 17px; font-weight: 600; margin-bottom: 6px; }}
  .sig-desc {{ font-size: 12px; color: #555; line-height: 1.4; }}
  .chart {{ background: white; padding: 14px; border-radius: 12px; margin-bottom: 14px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }}
  @media (max-width: 768px) {{ .regime-grid, .chart-grid {{ grid-template-columns: 1fr; }} }}
  .section-title {{ font-size: 16px; font-weight: 600; margin: 24px 0 10px; color: #444; }}
  .footer {{ font-size: 11px; color: #aaa; margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; }}
</style></head><body>
<h1>빚투 모니터</h1>
<div class="meta">시장 추세 전망 & 위험 신호 · 업데이트 {(df_plot["date"].iloc[-1].strftime("%Y년 %m월 %d일") if not df_plot.empty else "대기")}</div>
<div class="tabs">
  <div class="tab active" data-tab="kr">🇰🇷 한국장 ({signals['label_kr']})</div>
  <div class="tab" data-tab="us">🇺🇸 미국장 ({signals['label_us']})</div>
</div>

<div id="pane-kr" class="pane active">
  {render_regime_block(regime_kr, "코스피 추세 전망")}
  <div class="section-title">단기 위험 신호 (일간)</div>
  <div class="overall" style="border-left: 6px solid {COLOR.get(signals['label_kr'], '#888')};">
    <div><div class="overall-label">한국 위험도 점수</div><div class="overall-value" style="color: {COLOR.get(signals['label_kr'], '#888')};">{signals['label_kr']}</div></div>
    <div style="font-size: 14px; color: #888;">{signals['score_kr']} / {signals['max_kr']}점</div>
  </div>
  <div class="signal-grid">{render_signals(signals["kr"])}</div>
  <div class="section-title">차트</div>
  <div class="chart-grid"><div class="chart"><div id="c_kr_idx" style="height:300px;"></div></div><div class="chart"><div id="c_kr_vkospi" style="height:300px;"></div></div></div>
  <div class="chart"><div id="c_kr_credit" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_forced" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_foreign" style="height:300px;"></div></div>
  <div class="chart"><div id="c_kr_semi" style="height:280px;"></div></div>
  <div class="chart"><div id="c_kr_sector" style="height:320px;"></div></div>
</div>

<div id="pane-us" class="pane">
  {render_regime_block(regime_us, "S&P 500 추세 전망")}
  <div class="section-title">단기 위험 신호 (일간)</div>
  <div class="overall" style="border-left: 6px solid {COLOR.get(signals['label_us'], '#888')};">
    <div><div class="overall-label">미국 위험도 점수</div><div class="overall-value" style="color: {COLOR.get(signals['label_us'], '#888')};">{signals['label_us']}</div></div>
    <div style="font-size: 14px; color: #888;">{signals['score_us']} / {signals['max_us']}점</div>
  </div>
  <div class="signal-grid">{render_signals(signals["us"])}</div>
  <div class="section-title">차트</div>
  <div class="chart-grid"><div class="chart"><div id="c_us_sp" style="height:300px;"></div></div><div class="chart"><div id="c_us_nasdaq" style="height:300px;"></div></div></div>
  <div class="chart-grid"><div class="chart"><div id="c_us_vix" style="height:280px;"></div></div><div class="chart"><div id="c_us_nvda" style="height:280px;"></div></div></div>
  <div class="chart"><div id="c_us_rate" style="height:280px;"></div></div>
</div>
<div class="footer">데이터: FinanceDataReader, yfinance, pykrx, 공공데이터포털(API)</div>

<script>
const D = {json.dumps(js_data, ensure_ascii=False)};
const base = {{ margin: {{t: 30, r: 45, b: 35, l: 50}}, font: {{family: 'system-ui'}}, paper_bgcolor: 'white', plot_bgcolor: 'white', legend: {{orientation: 'h', y: -0.2}} }};
function plot(id, traces, title, extra) {{
  if (!D.dates.length) {{ document.getElementById(id).innerHTML = '<p style="text-align:center;color:#888;padding:40px;">데이터 대기</p>'; return; }}
  Plotly.newPlot(id, traces, {{...base, title: {{text: title, font: {{size: 13}}}}, ...(extra || {{}})}});
}}
function normalize(arr) {{
  const valid = arr.filter(v => v !== null && v !== 0 && !isNaN(v));
  const first = valid.length > 0 ? valid[0] : 1;
  return arr.map(v => (v !== null && v !== 0 && !isNaN(v)) ? (v / first) * 100 : null);
}}

plot('c_kr_idx', [
  {{x: D.dates, y: D.kospi, type: 'scatter', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.kospi_ma200, type: 'scatter', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}},
  {{x: D.dates, y: D.kosdaq, type: 'scatter', name: '코스닥', yaxis: 'y2', line: {{color: '#534AB7', width: 2}}}}
], '코스피 + 200일선 & 코스닥', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '코스닥', overlaying: 'y', side: 'right'}}}});

plot('c_kr_vkospi', [{{x: D.dates, y: D.vkospi, type: 'scatter', fill: 'tozeroy', name: 'VKOSPI', line: {{color: '#A32D2D'}}, fillcolor: 'rgba(163,45,45,0.1)'}}], 'VKOSPI 공포지수', {{
  shapes: [{{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 20, y1: 20, line: {{color: '#BA7517', width: 1, dash: 'dot'}}}}, {{type: 'line', xref: 'paper', x0: 0, x1: 1, y0: 30, y1: 30, line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}]
}});

plot('c_kr_credit', [
  {{x: D.dates, y: D.kospi, type: 'scatter', name: '코스피', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.credit, type: 'scatter', name: '신용잔고', yaxis: 'y2', line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
], '코스피 vs 신용잔고(억)', {{yaxis: {{title: '코스피'}}, yaxis2: {{title: '잔고(억)', overlaying: 'y', side: 'right'}}}});

plot('c_kr_forced', [{{x: D.dates, y: D.forced, type: 'bar', name: '반대매매', marker: {{color: D.forced.map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}}}], '일일 반대매매(억)');
plot('c_kr_foreign', [{{x: D.dates, y: D.foreign, type: 'bar', name: '외국인', marker: {{color: D.foreign.map(v => v < 0 ? '#A32D2D' : '#1D9E75')}}}}], '외국인 일별 순매수/매도 (코스피+코스닥)');

plot('c_kr_semi', [
  {{x: D.dates, y: normalize(D.kospi), type: 'scatter', name: '코스피', line: {{color: '#888', width: 2, dash: 'dot'}}}},
  {{x: D.dates, y: normalize(D.samsung), type: 'scatter', name: '삼성전자', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: normalize(D.hynix), type: 'scatter', name: 'SK하이닉스', line: {{color: '#534AB7', width: 2}}}}
], '코스피 vs 반도체 양대장 누적추세 (Base 100)');

plot('c_kr_sector', [
  {{x: D.dates, y: D.sec_반도체, type: 'scatter', name: '반도체', line: {{color: '#185FA5'}}}},
  {{x: D.dates, y: D.sec_방산조선, type: 'scatter', name: '방산조선', line: {{color: '#534AB7'}}}},
  {{x: D.dates, y: D.sec_바이오, type: 'scatter', name: '바이오', line: {{color: '#1D9E75'}}}},
  {{x: D.dates, y: D.sec_2차전지, type: 'scatter', name: '2차전지', line: {{color: '#BA7517'}}}},
  {{x: D.dates, y: D.sec_금융, type: 'scatter', name: '금융', line: {{color: '#888'}}}}
], '업종 바구니 누적추세 (Base 100)');

plot('c_us_sp', [
  {{x: D.dates, y: D.sp500, type: 'scatter', name: 'S&P 500', line: {{color: '#185FA5', width: 2}}}},
  {{x: D.dates, y: D.sp500_ma200, type: 'scatter', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}}
], 'S&P 500 + 200일선');

plot('c_us_nasdaq', [
  {{x: D.dates, y: D.nasdaq, type: 'scatter', name: '나스닥', line: {{color: '#534AB7', width: 2}}}},
  {{x: D.dates, y: D.nasdaq_ma200, type: 'scatter', name: '200일선', line: {{color: '#A32D2D', width: 1.5, dash: 'dash'}}}}
], '나스닥 + 200일선');

plot('c_us_vix', [{{x: D.dates, y: D.vix, type: 'scatter', fill: 'tozeroy', name: 'VIX', line: {{color: '#A32D2D'}}, fillcolor: 'rgba(163,45,45,0.1)'}}], 'VIX 공포지수');
plot('c_us_nvda', [{{x: D.dates, y: D.nvda, type: 'scatter', name: 'NVDA', line: {{color: '#1D9E75', width: 2}}}}], '엔비디아');
plot('c_us_rate', [{{x: D.dates, y: D.ust10y, type: 'scatter', name: '10Y', line: {{color: '#BA7517', width: 2}}}}], '미국 10년물 국채금리 (%)');

document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', e => {{
  document.querySelectorAll('.tab, .pane').forEach(x => x.classList.remove('active'));
  e.currentTarget.classList.add('active');
  document.getElementById('pane-' + e.currentTarget.dataset.tab).classList.add('active');
  setTimeout(() => window.dispatchEvent(new Event('resize')), 100);
}}));
</script></body></html>
"""
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")

def main():
    df = update_data() if not (df := load_history()).empty else update_data() # Ensure execution
    try:
        render_dashboard(df, compute_signals(df), compute_regime(df["kospi"]), compute_regime(df["sp500"]))
    except Exception as e:
        print(f"[error] render_dashboard: {e}")

if __name__ == "__main__":
    main()
