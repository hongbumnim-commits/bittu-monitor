"""
빚투(信用融資) 모니터 — 매일 자동으로 돌아가는 코스피 위험 신호 감시기

3가지 신호를 매일 체크합니다:
  ① 일일 반대매매 금액이 200억 → 400억 → 600억으로 상승 추세 진입 여부
  ② 신용잔고 증감률이 코스피 수익률을 0.5%p 이상 추월하는 달
  ③ 삼성전자·SK하이닉스 동시 -3% 일이 주에 2회 이상 발생

데이터 출처:
  - 코스피 / 종목 가격: pykrx (한국거래소)
  - 신용잔고 / 반대매매: 네이버 금융 (증시자금동향)
"""

import os
import csv
import json
import time
import datetime as dt
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock

ROOT = Path(__file__).parent
DATA_FILE = ROOT / "data" / "history.csv"
DASHBOARD_FILE = ROOT / "docs" / "index.html"

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
LOOKBACK_DAYS = 90  # 과거 90영업일 데이터 유지


def safe(fn, default=None, retries=3, sleep=2):
    """네트워크 오류 시 재시도하는 헬퍼"""
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"[warn] {fn.__name__} attempt {i+1} failed: {e}")
            time.sleep(sleep)
    return default


def fetch_kospi_recent():
    """최근 영업일들의 코스피 종가"""
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=180)).strftime("%Y%m%d")
    df = stock.get_index_ohlcv_by_date(start, end, "1001")  # 1001 = 코스피
    df.index = pd.to_datetime(df.index).date
    return df["종가"].rename("kospi")


def fetch_stock_recent(ticker, name):
    """개별 종목 종가"""
    end = TODAY.strftime("%Y%m%d")
    start = (TODAY - dt.timedelta(days=180)).strftime("%Y%m%d")
    df = stock.get_market_ohlcv_by_date(start, end, ticker)
    df.index = pd.to_datetime(df.index).date
    return df["종가"].rename(name)


def fetch_naver_money_flow():
    """
    네이버 금융 증시자금동향에서 신용잔고와 반대매매 데이터 스크래핑.
    https://finance.naver.com/sise/sise_deposit.naver

    반환: dict with keys: credit_balance(억), forced_sale(억), date
    """
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=10)
    r.encoding = "euc-kr"
    soup = BeautifulSoup(r.text, "html.parser")

    result = {"date": None, "credit_balance_eok": None, "forced_sale_eok": None}

    # 페이지 안에 표가 여러 개 있음. 텍스트로 매칭해서 안전하게 추출
    text = soup.get_text(separator=" ", strip=True)

    # 신용잔고 추출 (단위: 억원)
    import re
    m = re.search(r"신용잔고\s*([\d,]+)\s*억", text)
    if m:
        result["credit_balance_eok"] = int(m.group(1).replace(",", ""))

    # 반대매매 (위탁매매 미수금 대비 실제 반대매매)
    m = re.search(r"실제\s*반대매매\s*금액\s*([\d,]+)", text)
    if m:
        result["forced_sale_eok"] = int(m.group(1).replace(",", "")) / 100  # 표는 보통 백만원 단위

    # 날짜 추출
    m = re.search(r"(\d{4}\.\d{2}\.\d{2})", text)
    if m:
        result["date"] = m.group(1).replace(".", "-")

    return result


def load_history():
    """기존 CSV 로드 (없으면 빈 데이터프레임)"""
    if DATA_FILE.exists():
        df = pd.read_csv(DATA_FILE, parse_dates=["date"])
        df["date"] = df["date"].dt.date
        return df
    return pd.DataFrame(columns=[
        "date", "kospi", "samsung", "hynix",
        "credit_balance_eok", "forced_sale_eok",
        "samsung_ret_pct", "hynix_ret_pct"
    ])


def save_history(df):
    """CSV로 저장 (lookback 기간만 유지)"""
    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS * 2)
    df = df[df["date"] >= cutoff].sort_values("date").drop_duplicates("date", keep="last")
    df.to_csv(DATA_FILE, index=False)
    return df


def update_data():
    """모든 데이터 가져와서 history.csv 업데이트"""
    print(f"[{TODAY}] Fetching data...")

    kospi = safe(fetch_kospi_recent, default=pd.Series(dtype=float))
    samsung = safe(lambda: fetch_stock_recent("005930", "samsung"), default=pd.Series(dtype=float))
    hynix = safe(lambda: fetch_stock_recent("000660", "hynix"), default=pd.Series(dtype=float))
    flow = safe(fetch_naver_money_flow, default={"date": None, "credit_balance_eok": None, "forced_sale_eok": None})

    df = pd.DataFrame({"kospi": kospi, "samsung": samsung, "hynix": hynix})
    df["date"] = df.index
    df["samsung_ret_pct"] = df["samsung"].pct_change() * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change() * 100

    # 오늘 네이버에서 가져온 신용잔고/반대매매를 가장 최근 영업일에 붙임
    if not df.empty and flow["credit_balance_eok"]:
        latest_idx = df["date"].max()
        df.loc[df["date"] == latest_idx, "credit_balance_eok"] = flow["credit_balance_eok"]
        df.loc[df["date"] == latest_idx, "forced_sale_eok"] = flow["forced_sale_eok"]

    # 기존 데이터와 병합
    history = load_history()
    combined = pd.concat([history, df[["date", "kospi", "samsung", "hynix",
                                        "credit_balance_eok", "forced_sale_eok",
                                        "samsung_ret_pct", "hynix_ret_pct"]]])
    combined = save_history(combined)

    print(f"  rows: {len(combined)}, latest: {combined['date'].max()}")
    return combined


def compute_signals(df):
    """3가지 신호 계산. 각각 level (0=안전, 1=주의, 2=경계, 3=경고)"""
    df = df.sort_values("date").reset_index(drop=True)
    last = df.iloc[-1]
    signals = {}

    # ① 일일 반대매매 추세
    recent_forced = df["forced_sale_eok"].dropna().tail(20)
    avg20 = recent_forced.mean() if len(recent_forced) else 0
    today_forced = last.get("forced_sale_eok") or 0
    if today_forced >= 600:
        lvl = 3
    elif today_forced >= 400 or avg20 >= 400:
        lvl = 2
    elif today_forced >= 200 or avg20 >= 200:
        lvl = 1
    else:
        lvl = 0
    signals["signal1"] = {
        "name": "반대매매 일평균 상승 추세",
        "level": lvl,
        "today_value": round(today_forced, 1),
        "avg20_value": round(avg20, 1),
        "description": f"오늘 {today_forced:.0f}억 / 20일 평균 {avg20:.0f}억"
    }

    # ② 신용잔고 증감률 vs 코스피 (월간 비교 - 30일 변화율)
    if len(df) >= 30:
        kospi_30d = (df["kospi"].iloc[-1] / df["kospi"].iloc[-30] - 1) * 100
        credit_recent = df["credit_balance_eok"].dropna()
        if len(credit_recent) >= 30:
            credit_30d = (credit_recent.iloc[-1] / credit_recent.iloc[-30] - 1) * 100
            gap = credit_30d - kospi_30d
            if gap >= 2.0:
                lvl = 3
            elif gap >= 1.0:
                lvl = 2
            elif gap >= 0.5:
                lvl = 1
            else:
                lvl = 0
            signals["signal2"] = {
                "name": "신용잔고 증가율이 코스피 수익률 추월",
                "level": lvl,
                "kospi_30d": round(kospi_30d, 2),
                "credit_30d": round(credit_30d, 2),
                "gap": round(gap, 2),
                "description": f"30일 코스피 {kospi_30d:+.1f}% vs 신용잔고 {credit_30d:+.1f}% (+{gap:.1f}%p)"
            }
        else:
            signals["signal2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}
    else:
        signals["signal2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}

    # ③ 삼성전자·SK하이닉스 동시 -3% 일이 최근 5영업일 중 2회 이상
    last5 = df.tail(5)
    both_drop = ((last5["samsung_ret_pct"] <= -3) & (last5["hynix_ret_pct"] <= -3)).sum()
    if both_drop >= 2:
        lvl = 3
    elif both_drop == 1:
        lvl = 2
    elif (last5["samsung_ret_pct"] <= -3).any() or (last5["hynix_ret_pct"] <= -3).any():
        lvl = 1
    else:
        lvl = 0
    signals["signal3"] = {
        "name": "반도체 양대장 동시 -3% 빈도",
        "level": lvl,
        "both_drop_count": int(both_drop),
        "description": f"최근 5영업일 중 동시 -3% 발생: {both_drop}회"
    }

    # 종합 위험도 (3개 신호 합산)
    total = sum(s["level"] for s in signals.values())
    signals["overall"] = {
        "score": total,
        "max": 9,
        "label": ["평상", "관찰", "주의", "경계", "경고", "위험"][min(5, total // 2)]
    }
    return signals


def render_dashboard(df, signals):
    """Plotly로 HTML 대시보드 생성"""
    df = df.sort_values("date").reset_index(drop=True)
    df_plot = df.tail(60).copy()
    dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]]

    overall_color = {
        "평상": "#1D9E75", "관찰": "#1D9E75", "주의": "#BA7517",
        "경계": "#BA7517", "경고": "#A32D2D", "위험": "#A32D2D"
    }.get(signals["overall"]["label"], "#888")

    def lvl_color(lvl):
        return ["#1D9E75", "#BA7517", "#D85A30", "#A32D2D"][min(lvl, 3)]

    def lvl_label(lvl):
        return ["안전", "주의", "경계", "경고"][min(lvl, 3)]

    s1 = signals["signal1"]
    s2 = signals["signal2"]
    s3 = signals["signal3"]
    overall = signals["overall"]

    last_date = df_plot["date"].iloc[-1].strftime("%Y년 %m월 %d일")

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>빚투 모니터 — 코스피 위험 신호</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js@2.35.2/dist/plotly.min.js"></script>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
         max-width: 1100px; margin: 0 auto; padding: 24px; color: #222; background: #fafafa; }}
  h1 {{ font-size: 24px; font-weight: 600; margin: 0 0 6px; }}
  .meta {{ color: #888; font-size: 13px; margin-bottom: 24px; }}
  .overall {{ background: white; padding: 18px 22px; border-radius: 12px; margin-bottom: 24px;
              border-left: 6px solid {overall_color}; }}
  .overall-label {{ font-size: 13px; color: #888; }}
  .overall-value {{ font-size: 32px; font-weight: 600; color: {overall_color}; }}
  .signal-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                   gap: 14px; margin-bottom: 24px; }}
  .sig {{ background: white; padding: 16px; border-radius: 12px; border-top: 4px solid; }}
  .sig-name {{ font-size: 13px; color: #888; margin-bottom: 4px; }}
  .sig-status {{ font-size: 18px; font-weight: 600; margin-bottom: 6px; }}
  .sig-desc {{ font-size: 13px; color: #555; line-height: 1.5; }}
  .chart {{ background: white; padding: 16px; border-radius: 12px; margin-bottom: 16px; }}
  .footer {{ font-size: 11px; color: #aaa; margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; }}
  a {{ color: #185FA5; }}
</style>
</head>
<body>
<h1>빚투 모니터</h1>
<div class="meta">코스피 위험 신호 자동 감시 · 최근 업데이트 {last_date}</div>

<div class="overall">
  <div class="overall-label">종합 위험도 ({overall['score']}/{overall['max']})</div>
  <div class="overall-value">{overall['label']}</div>
</div>

<div class="signal-grid">
  <div class="sig" style="border-color: {lvl_color(s1['level'])}">
    <div class="sig-name">① {s1['name']}</div>
    <div class="sig-status" style="color: {lvl_color(s1['level'])}">{lvl_label(s1['level'])}</div>
    <div class="sig-desc">{s1['description']}</div>
  </div>
  <div class="sig" style="border-color: {lvl_color(s2['level'])}">
    <div class="sig-name">② {s2['name']}</div>
    <div class="sig-status" style="color: {lvl_color(s2['level'])}">{lvl_label(s2['level'])}</div>
    <div class="sig-desc">{s2['description']}</div>
  </div>
  <div class="sig" style="border-color: {lvl_color(s3['level'])}">
    <div class="sig-name">③ {s3['name']}</div>
    <div class="sig-status" style="color: {lvl_color(s3['level'])}">{lvl_label(s3['level'])}</div>
    <div class="sig-desc">{s3['description']}</div>
  </div>
</div>

<div class="chart"><div id="chart1" style="height: 320px;"></div></div>
<div class="chart"><div id="chart2" style="height: 320px;"></div></div>
<div class="chart"><div id="chart3" style="height: 320px;"></div></div>

<div class="footer">
  데이터: pykrx (한국거래소), 네이버 금융 증시자금동향 ·
  본 페이지는 매일 한국시간 17:30 GitHub Actions로 자동 갱신됩니다 ·
  투자 권유가 아니며, 모든 판단의 책임은 본인에게 있습니다.
</div>

<script>
const dates = {json.dumps(dates)};
const kospi = {json.dumps(df_plot['kospi'].fillna(0).round(2).tolist())};
const samsung = {json.dumps(df_plot['samsung'].fillna(0).round(0).tolist())};
const hynix = {json.dumps(df_plot['hynix'].fillna(0).round(0).tolist())};
const samsungRet = {json.dumps(df_plot['samsung_ret_pct'].fillna(0).round(2).tolist())};
const hynixRet = {json.dumps(df_plot['hynix_ret_pct'].fillna(0).round(2).tolist())};
const credit = {json.dumps(df_plot['credit_balance_eok'].fillna(method='ffill').fillna(0).round(0).tolist())};
const forced = {json.dumps(df_plot['forced_sale_eok'].fillna(0).round(1).tolist())};

const layoutBase = {{
  margin: {{t: 30, r: 50, b: 40, l: 50}},
  font: {{family: 'system-ui'}},
  showlegend: true,
  legend: {{orientation: 'h', y: -0.2}},
  paper_bgcolor: 'white',
  plot_bgcolor: 'white'
}};

Plotly.newPlot('chart1', [
  {{x: dates, y: forced, type: 'bar', name: '일일 반대매매(억)',
    marker: {{color: forced.map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}}},
], {{...layoutBase, title: {{text: '신호 ① 일일 반대매매 추이 (억원)', font: {{size: 14}}}}, yaxis: {{title: '억'}}}});

Plotly.newPlot('chart2', [
  {{x: dates, y: kospi, type: 'scatter', mode: 'lines', name: '코스피', yaxis: 'y', line: {{color: '#185FA5', width: 2}}}},
  {{x: dates, y: credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', line: {{color: '#993C1D', width: 2, dash: 'dash'}}}},
], {{...layoutBase, title: {{text: '신호 ② 코스피와 신용잔고 동조성', font: {{size: 14}}}},
     yaxis: {{title: '코스피', side: 'left'}}, yaxis2: {{title: '신용잔고(억)', overlaying: 'y', side: 'right'}}}});

Plotly.newPlot('chart3', [
  {{x: dates, y: samsungRet, type: 'bar', name: '삼성전자 일간%', marker: {{color: '#185FA5'}}}},
  {{x: dates, y: hynixRet, type: 'bar', name: 'SK하이닉스 일간%', marker: {{color: '#534AB7'}}}},
], {{...layoutBase, title: {{text: '신호 ③ 반도체 양대장 일간 변동률 (%)', font: {{size: 14}}}}, barmode: 'group',
     yaxis: {{title: '%', zeroline: true}}, shapes: [
       {{type: 'line', xref: 'paper', yref: 'y', x0: 0, x1: 1, y0: -3, y1: -3,
         line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}
     ]}});
</script>
</body>
</html>
"""
    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    print(f"  dashboard written: {DASHBOARD_FILE}")


def main():
    df = update_data()
    if df.empty:
        print("[error] no data fetched, skipping dashboard")
        return
    signals = compute_signals(df)
    print(json.dumps(signals, indent=2, ensure_ascii=False, default=str))
    render_dashboard(df, signals)
    print("Done.")


if __name__ == "__main__":
    main()
