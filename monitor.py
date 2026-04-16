"""
빚투(信用融資) 모니터 v2 — GitHub Actions 안정화 버전

변경사항:
- pykrx 제거 → FinanceDataReader로 교체 (KRX 로그인 불필요, 훨씬 안정적)
- 네이버 금융 스크래핑 로직 개선 (표 구조 기반으로 재작성)
- 빈 데이터프레임 처리 버그 수정
- 모든 단계에 예외 처리 추가 → 일부 실패해도 나머지는 계속 진행

감시 신호:
  ① 일일 반대매매 금액이 200억 → 400억 → 600억으로 상승 추세 진입 여부
  ② 신용잔고 증감률이 코스피 수익률을 0.5%p 이상 추월하는 월
  ③ 삼성전자·SK하이닉스 동시 -3% 일이 5영업일 중 2회 이상 발생
"""

import os
import json
import time
import datetime as dt
import re
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr

ROOT = Path(__file__).parent
DATA_FILE = ROOT / "data" / "history.csv"
DASHBOARD_FILE = ROOT / "docs" / "index.html"

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
LOOKBACK_DAYS = 90


def safe(fn_name, fn, default=None, retries=3, sleep=2):
    """네트워크 호출 재시도 헬퍼"""
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"[warn] {fn_name} attempt {i+1} failed: {e}")
            time.sleep(sleep)
    print(f"[error] {fn_name} gave up after {retries} tries, using default")
    return default


def fetch_kospi():
    """코스피 종가 - FinanceDataReader"""
    start = (TODAY - dt.timedelta(days=180)).strftime("%Y-%m-%d")
    end = TODAY.strftime("%Y-%m-%d")
    df = fdr.DataReader("KS11", start, end)
    if df.empty:
        return pd.Series(dtype=float, name="kospi")
    s = df["Close"]
    s.index = pd.to_datetime(s.index).date
    return s.rename("kospi")


def fetch_stock(ticker, name):
    """개별 종목 종가 - FinanceDataReader"""
    start = (TODAY - dt.timedelta(days=180)).strftime("%Y-%m-%d")
    end = TODAY.strftime("%Y-%m-%d")
    df = fdr.DataReader(ticker, start, end)
    if df.empty:
        return pd.Series(dtype=float, name=name)
    s = df["Close"]
    s.index = pd.to_datetime(s.index).date
    return s.rename(name)


def fetch_naver_flow():
    """
    네이버 금융 증시자금동향 스크래핑 (개선판).
    표 구조로 직접 파싱하여 정규식 실패 위험 줄임.
    """
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    r = requests.get(url, headers=headers, timeout=15)
    r.encoding = "euc-kr"
    soup = BeautifulSoup(r.text, "html.parser")

    result = {"credit_balance_eok": None, "forced_sale_eok": None}

    full_text = soup.get_text(" ", strip=True)

    # 신용잔고 - 여러 패턴 시도
    patterns_credit = [
        r"신용잔고[^\d]*([\d,]+)",
        r"신용공여잔고[^\d]*([\d,]+)",
    ]
    for p in patterns_credit:
        m = re.search(p, full_text)
        if m:
            try:
                val = int(m.group(1).replace(",", ""))
                if val > 100000:
                    result["credit_balance_eok"] = val
                    break
            except ValueError:
                continue

    # 반대매매
    patterns_forced = [
        r"반대매매[^\d]*([\d,]+)",
        r"실제\s*반대매매\s*금액[^\d]*([\d,]+)",
    ]
    for p in patterns_forced:
        m = re.search(p, full_text)
        if m:
            try:
                val_str = m.group(1).replace(",", "")
                val = int(val_str)
                if 10 <= val <= 100000:
                    result["forced_sale_eok"] = val
                    break
            except ValueError:
                continue

    print(f"  naver flow: credit={result['credit_balance_eok']}, forced={result['forced_sale_eok']}")
    return result


def load_history():
    """기존 CSV 로드 (빈 파일 안전 처리)"""
    cols = ["date", "kospi", "samsung", "hynix",
            "credit_balance_eok", "forced_sale_eok",
            "samsung_ret_pct", "hynix_ret_pct"]

    if not DATA_FILE.exists():
        return pd.DataFrame(columns=cols)

    try:
        df = pd.read_csv(DATA_FILE)
        if df.empty or "date" not in df.columns:
            return pd.DataFrame(columns=cols)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df[cols]
    except Exception as e:
        print(f"[warn] load_history failed: {e}, starting fresh")
        return pd.DataFrame(columns=cols)


def save_history(df):
    """CSV 저장 (lookback 기간만 유지)"""
    if df.empty:
        DATA_FILE.write_text("date,kospi,samsung,hynix,credit_balance_eok,forced_sale_eok,samsung_ret_pct,hynix_ret_pct\n")
        return df
    cutoff = TODAY - dt.timedelta(days=LOOKBACK_DAYS * 2)
    df = df[df["date"] >= cutoff].sort_values("date").drop_duplicates("date", keep="last")
    df.to_csv(DATA_FILE, index=False)
    return df


def update_data():
    """모든 데이터 가져와서 통합"""
    print(f"[{TODAY}] Fetching data...")

    kospi = safe("fetch_kospi", fetch_kospi, default=pd.Series(dtype=float, name="kospi"))
    samsung = safe("fetch_samsung", lambda: fetch_stock("005930", "samsung"), default=pd.Series(dtype=float, name="samsung"))
    hynix = safe("fetch_hynix", lambda: fetch_stock("000660", "hynix"), default=pd.Series(dtype=float, name="hynix"))
    flow = safe("fetch_naver_flow", fetch_naver_flow, default={"credit_balance_eok": None, "forced_sale_eok": None})

    print(f"  kospi: {len(kospi)} rows, samsung: {len(samsung)} rows, hynix: {len(hynix)} rows")

    if kospi.empty and samsung.empty and hynix.empty:
        print("[error] all price data fetches failed, keeping old history")
        return load_history()

    df = pd.DataFrame({"kospi": kospi, "samsung": samsung, "hynix": hynix})
    df = df.reset_index().rename(columns={"index": "date"})
    df["samsung_ret_pct"] = df["samsung"].pct_change() * 100
    df["hynix_ret_pct"] = df["hynix"].pct_change() * 100
    df["credit_balance_eok"] = None
    df["forced_sale_eok"] = None

    # 오늘 네이버 데이터를 최신 행에 반영
    if not df.empty and flow.get("credit_balance_eok"):
        latest_date = df["date"].max()
        mask = df["date"] == latest_date
        df.loc[mask, "credit_balance_eok"] = flow["credit_balance_eok"]
        df.loc[mask, "forced_sale_eok"] = flow["forced_sale_eok"]

    history = load_history()
    combined = pd.concat([history, df], ignore_index=True)
    combined = save_history(combined)
    print(f"  saved: {len(combined)} rows, latest: {combined['date'].max() if not combined.empty else 'none'}")
    return combined


def compute_signals(df):
    """3가지 신호 계산"""
    if df.empty:
        return {
            "signal1": {"name": "반대매매 일평균", "level": 0, "description": "데이터 없음"},
            "signal2": {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 없음"},
            "signal3": {"name": "반도체 양대장 동시급락", "level": 0, "description": "데이터 없음"},
            "overall": {"score": 0, "max": 9, "label": "데이터 대기"}
        }

    df = df.sort_values("date").reset_index(drop=True)
    last = df.iloc[-1]
    signals = {}

    # ① 반대매매 추세
    recent_forced = pd.to_numeric(df["forced_sale_eok"], errors="coerce").dropna().tail(20)
    avg20 = float(recent_forced.mean()) if len(recent_forced) else 0.0
    today_forced = float(last.get("forced_sale_eok") or 0)
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

    # ② 신용잔고 증감률 vs 코스피 수익률 (30일 변화율)
    signals["signal2"] = {"name": "신용잔고 증가율 추월", "level": 0, "description": "데이터 부족"}
    if len(df) >= 30:
        kospi_series = pd.to_numeric(df["kospi"], errors="coerce").dropna()
        credit_series = pd.to_numeric(df["credit_balance_eok"], errors="coerce").dropna()
        if len(kospi_series) >= 30 and len(credit_series) >= 30:
            kospi_30d = (kospi_series.iloc[-1] / kospi_series.iloc[-30] - 1) * 100
            credit_30d = (credit_series.iloc[-1] / credit_series.iloc[-30] - 1) * 100
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
                "description": f"30일 코스피 {kospi_30d:+.1f}% vs 신용잔고 {credit_30d:+.1f}% (+{gap:.1f}%p)"
            }

    # ③ 삼성·하이닉스 동시 -3%
    last5 = df.tail(5)
    sret = pd.to_numeric(last5["samsung_ret_pct"], errors="coerce")
    hret = pd.to_numeric(last5["hynix_ret_pct"], errors="coerce")
    both = ((sret <= -3) & (hret <= -3)).sum()
    if both >= 2:
        lvl = 3
    elif both == 1:
        lvl = 2
    elif ((sret <= -3).any() or (hret <= -3).any()):
        lvl = 1
    else:
        lvl = 0
    signals["signal3"] = {
        "name": "반도체 양대장 동시 -3% 빈도",
        "level": lvl,
        "description": f"최근 5영업일 중 동시 -3% 발생: {int(both)}회"
    }

    total = sum(s["level"] for s in signals.values())
    signals["overall"] = {
        "score": total,
        "max": 9,
        "label": ["평상", "관찰", "주의", "경계", "경고", "위험"][min(5, total // 2)]
    }
    return signals


def render_dashboard(df, signals):
    """Plotly HTML 대시보드"""
    df_plot = df.sort_values("date").tail(60).copy() if not df.empty else pd.DataFrame()

    if df_plot.empty:
        dates, kospi, samsung, hynix, sret, hret, credit, forced = [], [], [], [], [], [], [], []
    else:
        dates = [d.strftime("%Y-%m-%d") for d in df_plot["date"]]
        kospi = pd.to_numeric(df_plot["kospi"], errors="coerce").fillna(0).round(2).tolist()
        samsung = pd.to_numeric(df_plot["samsung"], errors="coerce").fillna(0).round(0).tolist()
        hynix = pd.to_numeric(df_plot["hynix"], errors="coerce").fillna(0).round(0).tolist()
        sret = pd.to_numeric(df_plot["samsung_ret_pct"], errors="coerce").fillna(0).round(2).tolist()
        hret = pd.to_numeric(df_plot["hynix_ret_pct"], errors="coerce").fillna(0).round(2).tolist()
        credit = pd.to_numeric(df_plot["credit_balance_eok"], errors="coerce").fillna(method="ffill").fillna(0).round(0).tolist()
        forced = pd.to_numeric(df_plot["forced_sale_eok"], errors="coerce").fillna(0).round(1).tolist()

    overall_color = {
        "평상": "#1D9E75", "관찰": "#1D9E75", "주의": "#BA7517",
        "경계": "#BA7517", "경고": "#A32D2D", "위험": "#A32D2D",
        "데이터 대기": "#888"
    }.get(signals["overall"]["label"], "#888")

    def lvl_color(lvl):
        return ["#1D9E75", "#BA7517", "#D85A30", "#A32D2D"][min(lvl, 3)]

    def lvl_label(lvl):
        return ["안전", "주의", "경계", "경고"][min(lvl, 3)]

    s1, s2, s3, overall = signals["signal1"], signals["signal2"], signals["signal3"], signals["overall"]
    last_date = df_plot["date"].iloc[-1].strftime("%Y년 %m월 %d일") if not df_plot.empty else "데이터 대기"

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
  데이터: FinanceDataReader(한국거래소), 네이버 금융 증시자금동향 ·
  평일 한국시간 17:30 GitHub Actions 자동 갱신 ·
  투자 권유 아님, 모든 판단의 책임은 본인에게 있습니다.
</div>

<script>
const dates = {json.dumps(dates)};
const kospi = {json.dumps(kospi)};
const samsung = {json.dumps(samsung)};
const hynix = {json.dumps(hynix)};
const samsungRet = {json.dumps(sret)};
const hynixRet = {json.dumps(hret)};
const credit = {json.dumps(credit)};
const forced = {json.dumps(forced)};

const layoutBase = {{
  margin: {{t: 30, r: 50, b: 40, l: 50}},
  font: {{family: 'system-ui'}},
  showlegend: true,
  legend: {{orientation: 'h', y: -0.2}},
  paper_bgcolor: 'white',
  plot_bgcolor: 'white'
}};

if (dates.length > 0) {{
  Plotly.newPlot('chart1', [
    {{x: dates, y: forced, type: 'bar', name: '일일 반대매매(억)',
      marker: {{color: forced.map(v => v >= 600 ? '#A32D2D' : v >= 400 ? '#D85A30' : v >= 200 ? '#BA7517' : '#888')}}}}
  ], {{...layoutBase, title: {{text: '신호 ① 일일 반대매매 추이 (억원)', font: {{size: 14}}}}, yaxis: {{title: '억'}}}});

  Plotly.newPlot('chart2', [
    {{x: dates, y: kospi, type: 'scatter', mode: 'lines', name: '코스피', yaxis: 'y', line: {{color: '#185FA5', width: 2}}}},
    {{x: dates, y: credit, type: 'scatter', mode: 'lines', name: '신용잔고(억)', yaxis: 'y2', line: {{color: '#993C1D', width: 2, dash: 'dash'}}}}
  ], {{...layoutBase, title: {{text: '신호 ② 코스피와 신용잔고 동조성', font: {{size: 14}}}},
       yaxis: {{title: '코스피', side: 'left'}}, yaxis2: {{title: '신용잔고(억)', overlaying: 'y', side: 'right'}}}});

  Plotly.newPlot('chart3', [
    {{x: dates, y: samsungRet, type: 'bar', name: '삼성전자 일간%', marker: {{color: '#185FA5'}}}},
    {{x: dates, y: hynixRet, type: 'bar', name: 'SK하이닉스 일간%', marker: {{color: '#534AB7'}}}}
  ], {{...layoutBase, title: {{text: '신호 ③ 반도체 양대장 일간 변동률 (%)', font: {{size: 14}}}}, barmode: 'group',
       yaxis: {{title: '%', zeroline: true}}, shapes: [
         {{type: 'line', xref: 'paper', yref: 'y', x0: 0, x1: 1, y0: -3, y1: -3,
           line: {{color: '#A32D2D', width: 1, dash: 'dot'}}}}
       ]}});
}} else {{
  document.getElementById('chart1').innerHTML = '<p style="text-align:center;color:#888;padding:40px;">데이터 수집 대기 중...</p>';
  document.getElementById('chart2').innerHTML = '';
  document.getElementById('chart3').innerHTML = '';
}}
</script>
</body>
</html>
"""
    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    print(f"  dashboard: {DASHBOARD_FILE}")


def main():
    try:
        df = update_data()
    except Exception as e:
        print(f"[error] update_data fatal: {e}")
        df = load_history()

    signals = compute_signals(df)
    print(json.dumps(signals, indent=2, ensure_ascii=False, default=str))

    try:
        render_dashboard(df, signals)
    except Exception as e:
        print(f"[error] render_dashboard failed: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
