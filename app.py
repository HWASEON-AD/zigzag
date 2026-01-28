import os
import time
import traceback
import pandas as pd
import smtplib
from datetime import datetime
from glob import glob

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait


# ===================== ENV 설정 =====================
URL = os.environ.get(
    "TARGET_URL",
    "https://zigzag.kr/search?keyword=%EC%9B%8C%EB%84%88%EB%A7%88%EC%9D%B8"
)

TARGET_UNIQUE = int(os.environ.get("TARGET_UNIQUE", "500"))
SCROLL_WAIT = int(os.environ.get("SCROLL_WAIT", "5"))
MAX_SCROLLS = int(os.environ.get("MAX_SCROLLS", "250"))
STAGNANT_LIMIT = int(os.environ.get("STAGNANT_LIMIT", "50"))

# ✅ 스냅샷 파일 누적 저장 개수(최근 N개만 유지)
KEEP_SNAPSHOT_FILES = int(os.environ.get("KEEP_SNAPSHOT_FILES", "48"))

# ✅ Render Persistent Disk 경로 권장: /var/data
#    (기본값을 /tmp로 두면 재시작/배포 때 prev 스냅샷이 날아가서 "변동 없음"이 계속 뜰 수 있음)
BASE_DIR = os.environ.get("DATA_DIR", "/var/data")
os.makedirs(BASE_DIR, exist_ok=True)

SNAPSHOT_PATH = os.path.join(BASE_DIR, "catalog_snapshot.xlsx")  # 비교 기준(항상 1개 덮어쓰기)
CHANGE_DIR = os.path.join(BASE_DIR, "price_changes")
os.makedirs(CHANGE_DIR, exist_ok=True)

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.worksmobile.com")
PORT = int(os.environ.get("SMTP_PORT", "465"))
USER = os.environ.get("SMTP_USER", "gt.min@hwaseon.com")
PASSWORD = os.environ.get("SMTP_PASSWORD", "")

# ✅ 변동/에러 알림: 전체
ALERT_TO_RAW = os.environ.get(
    "ALERT_TO",
    "wannamine@naver.com,gt.min@hwaseon.com,jhj970826@naver.com"
)
ALERT_TO = [x.strip() for x in ALERT_TO_RAW.split(",") if x.strip()]

# ✅ 스냅샷: 너한테만 (환경변수로 조절)
SNAPSHOT_TO_RAW = os.environ.get("SNAPSHOT_TO", USER)
SNAPSHOT_TO = [x.strip() for x in SNAPSHOT_TO_RAW.split(",") if x.strip()]
# ====================================================


# ---------- 사이트 셀렉터 ----------
LINK_SEL = "a.css-1pjr9xx.product-card-link"
NAME_SEL_1 = ".zds4_1kdomrc"
NAME_SEL_2 = ".zds4_1kdomra"
DISCOUNT_SEL = ".zds4_1jsf80i2"
PRICE_SEL = ".zds4_1jsf80i3"


def send_email(to_emails, subject: str, body_html: str, attachments=None):
    if attachments is None:
        attachments = []

    if not PASSWORD:
        raise RuntimeError("SMTP_PASSWORD가 비어있습니다. Render Environment Variables에 SMTP_PASSWORD를 설정하세요.")

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = USER
    msg["To"] = ", ".join(to_emails)
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    for path in attachments:
        if not path or not os.path.exists(path):
            continue
        part = MIMEBase("application", "octet-stream")
        with open(path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        filename = os.path.basename(path)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
        msg.attach(part)

    server = smtplib.SMTP_SSL(SMTP_SERVER, PORT)
    server.login(USER, PASSWORD)
    server.sendmail(USER, to_emails, msg.as_string())
    server.quit()


def safe_text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def find_first_text(card, selectors) -> str:
    for sel in selectors:
        try:
            el = card.find_element(By.CSS_SELECTOR, sel)
            txt = safe_text(el)
            if txt:
                return txt
        except Exception:
            pass
    return ""


def normalize_href(driver, href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("/"):
        parts = driver.current_url.split("/", 3)
        base = parts[0] + "//" + parts[2]
        return base + href
    return href


def wait_for_links(driver, timeout=20):
    WebDriverWait(driver, timeout).until(
        lambda d: len(d.find_elements(By.CSS_SELECTOR, LINK_SEL)) > 0
    )


def page_down(driver, n=1):
    for _ in range(n):
        driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight * 0.9));")
        time.sleep(SCROLL_WAIT)


def to_int_digits(s: str):
    """'12,900원' -> 12900 / '20%' -> 20 / '' -> None"""
    if s is None:
        return None
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    return int(digits) if digits else None


def scrape_ranked(driver, target_unique=TARGET_UNIQUE) -> pd.DataFrame:
    try:
        wait_for_links(driver, timeout=20)
    except TimeoutException:
        raise RuntimeError("LINK_SEL 요소가 0개입니다. (셀렉터/로딩/페이지 상태 확인 필요)")

    seen = set()
    items = []
    scrolls = 0
    stagnant = 0
    last_len = 0

    while len(items) < target_unique and scrolls < MAX_SCROLLS and stagnant < STAGNANT_LIMIT:
        links = driver.find_elements(By.CSS_SELECTOR, LINK_SEL)

        for a in links:
            if len(items) >= target_unique:
                break
            try:
                href = normalize_href(driver, a.get_attribute("href"))
                if not href or href in seen:
                    continue

                try:
                    card = a.find_element(By.XPATH, "./ancestor::*[1]")
                except Exception:
                    card = a

                name = find_first_text(card, [NAME_SEL_1, NAME_SEL_2])
                discount = find_first_text(card, [DISCOUNT_SEL])
                price = find_first_text(card, [PRICE_SEL])

                items.append({
                    "rank": len(items) + 1,
                    "href": href,
                    "product_name": name,
                    "discount": discount,
                    "price": price,
                    # 정규화(비교 안정화)
                    "discount_int": to_int_digits(discount),
                    "price_int": to_int_digits(price),
                })
                seen.add(href)

            except StaleElementReferenceException:
                continue

        if len(items) == last_len:
            stagnant += 1
        else:
            stagnant = 0
            last_len = len(items)

        if len(items) >= target_unique:
            break

        page_down(driver, n=1)
        scrolls += 1

    return pd.DataFrame(items)


def load_prev_snapshot(path=SNAPSHOT_PATH):
    if os.path.exists(path):
        try:
            return pd.read_excel(path)
        except Exception:
            return None
    return None


def save_snapshot_latest(df: pd.DataFrame, path=SNAPSHOT_PATH):
    df.to_excel(path, index=False)


def save_snapshot_copy_excel(df: pd.DataFrame, checked_at_str: str) -> str:
    ts = checked_at_str.replace("-", "").replace(":", "").replace(" ", "_")
    path = os.path.join(BASE_DIR, f"snapshot_{ts}.xlsx")
    df.to_excel(path, index=False)
    return path


def cleanup_old_snapshots(keep_n: int = KEEP_SNAPSHOT_FILES):
    try:
        files = sorted(glob(os.path.join(BASE_DIR, "snapshot_*.xlsx")))
        if keep_n <= 0 or len(files) <= keep_n:
            return
        to_delete = files[:len(files) - keep_n]
        for f in to_delete:
            try:
                os.remove(f)
            except Exception:
                pass
    except Exception:
        pass


def detect_changes(prev_df: pd.DataFrame, cur_df: pd.DataFrame):
    """
    비교 기준:
    - href 동일한 상품에 대해 discount_int / price_int가 바뀌면 변동으로 판단
    - 정규화 int가 None이면(raw 문자열로 fallback)
    """
    if prev_df is None or prev_df.empty or cur_df is None or cur_df.empty:
        return []

    # 이전 버전 스냅샷에 int 컬럼이 없을 수도 있으니 보정
    prev = prev_df.copy()
    cur = cur_df.copy()

    if "discount_int" not in prev.columns and "discount" in prev.columns:
        prev["discount_int"] = prev["discount"].apply(to_int_digits)
    if "price_int" not in prev.columns and "price" in prev.columns:
        prev["price_int"] = prev["price"].apply(to_int_digits)

    needed_cols_prev = [c for c in ["href", "rank", "discount", "price", "discount_int", "price_int"] if c in prev.columns]
    needed_cols_cur = [c for c in ["href", "rank", "discount", "price", "discount_int", "price_int"] if c in cur.columns]

    prev = prev[needed_cols_prev].copy()
    cur = cur[needed_cols_cur].copy()

    merged = prev.merge(cur, on="href", how="inner", suffixes=("_prev", "_cur"))

    changes = []
    for _, r in merged.iterrows():
        disc_prev_raw = (r.get("discount_prev") or "").strip()
        disc_cur_raw = (r.get("discount_cur") or "").strip()
        price_prev_raw = (r.get("price_prev") or "").strip()
        price_cur_raw = (r.get("price_cur") or "").strip()

        disc_prev_int = r.get("discount_int_prev", None)
        disc_cur_int = r.get("discount_int_cur", None)
        price_prev_int = r.get("price_int_prev", None)
        price_cur_int = r.get("price_int_cur", None)

        # int 값이 둘 다 있으면 int로 비교, 아니면 raw 문자열 비교
        disc_changed = (disc_prev_int != disc_cur_int) if (disc_prev_int is not None and disc_cur_int is not None) else (disc_prev_raw != disc_cur_raw)
        price_changed = (price_prev_int != price_cur_int) if (price_prev_int is not None and price_cur_int is not None) else (price_prev_raw != price_cur_raw)

        if disc_changed or price_changed:
            changes.append({
                "href": r["href"],
                "rank_prev": r.get("rank_prev", ""),
                "rank_cur": r.get("rank_cur", ""),
                "discount_prev": disc_prev_raw,
                "discount_cur": disc_cur_raw,
                "price_prev": price_prev_raw,
                "price_cur": price_cur_raw,
                "discount_prev_int": disc_prev_int,
                "discount_cur_int": disc_cur_int,
                "price_prev_int": price_prev_int,
                "price_cur_int": price_cur_int,
            })
    return changes


def build_issue_email_body(changes, checked_at: str) -> str:
    rows_html = ""
    for c in changes:
        rows_html += f"""
        <tr>
          <td>{c.get("rank_prev","")}</td>
          <td>{c.get("rank_cur","")}</td>
          <td>{c.get("discount_prev","")}</td>
          <td>{c.get("discount_cur","")}</td>
          <td>{c.get("price_prev","")}</td>
          <td>{c.get("price_cur","")}</td>
          <td><a href="{c["href"]}" target="_blank">open</a></td>
        </tr>
        """
    return f"""
    <p><b>가격/할인 변동 감지</b></p>
    <p>체크 시각: <b>{checked_at}</b></p>
    <p>첨부파일: 변동건만 엑셀로 첨부</p>

    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; font-size:13px;">
      <thead>
        <tr>
          <th>Prev Rank</th><th>Cur Rank</th>
          <th>Prev Discount</th><th>Cur Discount</th>
          <th>Prev Price</th><th>Cur Price</th>
          <th>Link</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    """


def save_changes_excel(changes, checked_at_str: str) -> str:
    ts = checked_at_str.replace("-", "").replace(":", "").replace(" ", "_")
    path = os.path.join(CHANGE_DIR, f"price_change_{ts}.xlsx")

    df = pd.DataFrame(changes)
    cols = [
        "href",
        "rank_prev", "rank_cur",
        "discount_prev", "discount_cur",
        "price_prev", "price_cur",
        "discount_prev_int", "discount_cur_int",
        "price_prev_int", "price_cur_int",
    ]
    df = df[[c for c in cols if c in df.columns]]
    df.to_excel(path, index=False)
    return path


def build_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1200,900")
    options.add_argument("--disable-blink-features=AutomationControlled")

    options.binary_location = "/usr/bin/chromium"
    service = Service("/usr/bin/chromedriver")

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def run_once():
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    driver = None

    try:
        # ---- prev 스냅샷 존재 여부 로그 (변동 안 오는 1순위 원인 확인용) ----
        print(f"[CONFIG] BASE_DIR={BASE_DIR}")
        print(f"[CONFIG] SNAPSHOT_PATH={SNAPSHOT_PATH} exists={os.path.exists(SNAPSHOT_PATH)}")

        driver = build_driver()
        driver.get(URL)
        time.sleep(2)
        driver.refresh()
        time.sleep(5)

        cur_df = scrape_ranked(driver, target_unique=TARGET_UNIQUE)

        prev_df = load_prev_snapshot(SNAPSHOT_PATH)
        print(f"[PREV] loaded={'YES' if (prev_df is not None and not prev_df.empty) else 'NO'} rows={0 if prev_df is None else len(prev_df)}")
        print(f"[CUR] rows={len(cur_df)}")

        changes = detect_changes(prev_df, cur_df)
        print(f"[DIFF] changes={len(changes)}")

        # 비교 기준용 최신 스냅샷 덮어쓰기 (다음 실행에서 prev로 사용)
        save_snapshot_latest(cur_df, SNAPSHOT_PATH)

        # 실행마다 스냅샷 파일 생성(메일 첨부용)
        snapshot_path = save_snapshot_copy_excel(cur_df, checked_at)

        # 오래된 스냅샷 정리
        cleanup_old_snapshots(KEEP_SNAPSHOT_FILES)

        # ✅ 스냅샷은 "너한테만" (SNAPSHOT_TO)
        subject = f"<스냅샷> {checked_at} (collected={len(cur_df)})"
        body = f"""
        <p><b>스냅샷 완료</b></p>
        <p>시간: <b>{checked_at}</b></p>
        <p>수집: <b>{len(cur_df)}</b>개 (목표 {TARGET_UNIQUE})</p>
        <p>첨부: <b>전체 스냅샷 엑셀</b></p>
        <p>SNAPSHOT_PATH 유지 여부: <b>{'YES' if os.path.exists(SNAPSHOT_PATH) else 'NO'}</b></p>
        """
        send_email(SNAPSHOT_TO, subject, body, attachments=[snapshot_path])
        print(f"snapshot mail sent | to={','.join(SNAPSHOT_TO)} | collected={len(cur_df)} | {checked_at} | attach={os.path.basename(snapshot_path)}")

        # ✅ 변동 있으면 변동 메일 + 엑셀 첨부는 "전체" (ALERT_TO)
        if changes:
            issue_subject = f"<가격변동 확인필요> {checked_at} ({len(changes)}건)"
            issue_body = build_issue_email_body(changes, checked_at)
            change_path = save_changes_excel(changes, checked_at)
            send_email(ALERT_TO, issue_subject, issue_body, attachments=[change_path])
            print(f"ISSUE mail sent | to={','.join(ALERT_TO)} | issue={len(changes)} | {checked_at} | attach={os.path.basename(change_path)}")
        else:
            print(f"no change | collected={len(cur_df)} | {checked_at}")

    except Exception as e:
        err = traceback.format_exc()
        print(err)

        # ✅ 에러도 "전체"에게 발송
        try:
            subject = f"<크롤러 에러> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            body = f"""
            <p><b>크롤러 실행 중 에러 발생</b></p>
            <pre style="white-space:pre-wrap; font-size:12px;">{err}</pre>
            """
            send_email(ALERT_TO, subject, body)
        except Exception as mail_e:
            print(f"error-mail failed: {mail_e}")

        raise e

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    run_once()
