import os
import time
import json
import traceback
import pandas as pd
import smtplib
from datetime import datetime
from glob import glob

import psycopg2
from psycopg2.extras import Json

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

# ✅ 크론잡이면 디스크가 휘발일 수 있으니 기본은 스냅샷 저장 OFF 권장
SAVE_SNAPSHOT_FILES = os.environ.get("SAVE_SNAPSHOT_FILES", "0") == "1"

# 스냅샷/변동 엑셀 임시 저장용(크론잡에서는 휘발 가능)
BASE_DIR = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(BASE_DIR, exist_ok=True)

CHANGE_DIR = os.path.join(BASE_DIR, "price_changes")
os.makedirs(CHANGE_DIR, exist_ok=True)

# ✅ Render PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STATE_KEY = os.environ.get("STATE_KEY", "zigzag:wannamine")

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

# ✅ 스냅샷: 너한테만
SNAPSHOT_TO_RAW = os.environ.get("SNAPSHOT_TO", USER)
SNAPSHOT_TO = [x.strip() for x in SNAPSHOT_TO_RAW.split(",") if x.strip()]
# ====================================================


# ---------- 사이트 셀렉터 ----------
LINK_SEL = "a.css-1pjr9xx.product-card-link"
NAME_SEL_1 = ".zds4_1kdomrc"
NAME_SEL_2 = ".zds4_1kdomra"
DISCOUNT_SEL = ".zds4_1jsf80i2"
PRICE_SEL = ".zds4_1jsf80i3"


# ===================== 메일 =====================
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


# ===================== 유틸 =====================
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


# ===================== Postgres (JSON 1줄 저장/로드) =====================
def pg_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL이 비어있습니다. Render Postgres Internal Database URL을 환경변수 DATABASE_URL로 넣어주세요.")
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


def pg_init():
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS crawler_state (
              key text PRIMARY KEY,
              value jsonb NOT NULL,
              updated_at timestamptz NOT NULL DEFAULT now()
            );
            """)
        conn.commit()


def pg_get_state(state_key: str) -> dict:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM crawler_state WHERE key=%s", (state_key,))
            row = cur.fetchone()
            if not row:
                return {}
            val = row[0]
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except Exception:
                    return {}
            if isinstance(val, dict):
                return val
            try:
                return dict(val)
            except Exception:
                return {}


def pg_set_state(state_key: str, state_value: dict):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO crawler_state (key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key)
            DO UPDATE SET value=EXCLUDED.value, updated_at=now()
            """, (state_key, Json(state_value)))
        conn.commit()


# ===================== 스냅샷/변동 엑셀(옵션) =====================
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


# ===================== 변동 감지 =====================
def df_to_state_map(cur_df: pd.DataFrame) -> dict:
    """
    JSON 1줄 저장 포맷:
      { href: {rank, product_name, discount_raw, price_raw, discount_int, price_int}, ... }
    """
    state = {}
    for _, r in cur_df.iterrows():
        href = r.get("href", "")
        if not href:
            continue
        state[href] = {
            "rank": int(r["rank"]) if pd.notna(r.get("rank")) else None,
            "product_name": (r.get("product_name") or ""),
            "discount_raw": (r.get("discount") or ""),
            "price_raw": (r.get("price") or ""),
            "discount_int": int(r["discount_int"]) if pd.notna(r.get("discount_int")) else None,
            "price_int": int(r["price_int"]) if pd.notna(r.get("price_int")) else None,
        }
    return state


def detect_changes(prev_map: dict, cur_map: dict):
    if not prev_map or not cur_map:
        return []

    changes = []
    for href, cur in cur_map.items():
        prev = prev_map.get(href)
        if not prev:
            continue

        prev_price = prev.get("price_int")
        cur_price = cur.get("price_int")
        prev_disc = prev.get("discount_int")
        cur_disc = cur.get("discount_int")

        price_changed = (prev_price is not None and cur_price is not None and prev_price != cur_price)
        disc_changed = (prev_disc is not None and cur_disc is not None and prev_disc != cur_disc)

        # int 없으면 raw로 fallback
        if not price_changed:
            pr = (prev.get("price_raw") or "").strip()
            cr = (cur.get("price_raw") or "").strip()
            if pr and cr and pr != cr:
                price_changed = True

        if not disc_changed:
            pd_ = (prev.get("discount_raw") or "").strip()
            cd_ = (cur.get("discount_raw") or "").strip()
            if pd_ and cd_ and pd_ != cd_:
                disc_changed = True

        if price_changed or disc_changed:
            changes.append({
                "href": href,
                "rank_prev": prev.get("rank", ""),
                "rank_cur": cur.get("rank", ""),
                "discount_prev": prev.get("discount_raw", ""),
                "discount_cur": cur.get("discount_raw", ""),
                "price_prev": prev.get("price_raw", ""),
                "price_cur": cur.get("price_raw", ""),
                "discount_prev_int": prev_disc,
                "discount_cur_int": cur_disc,
                "price_prev_int": prev_price,
                "price_cur_int": cur_price,
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


# ===================== 크롤링 =====================
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
        # 0) DB 준비 + prev 로드
        pg_init()
        prev_map = pg_get_state(STATE_KEY)
        print(f"[PG] loaded prev keys={len(prev_map)} state_key={STATE_KEY}")

        # 1) 크롤링
        driver = build_driver()
        driver.get(URL)
        time.sleep(2)
        driver.refresh()
        time.sleep(5)

        cur_df = scrape_ranked(driver, target_unique=TARGET_UNIQUE)
        print(f"[CUR] rows={len(cur_df)}")

        # 2) 이번 상태(cur_map) 만들고 비교
        cur_map = df_to_state_map(cur_df)
        changes = detect_changes(prev_map, cur_map)
        print(f"[DIFF] changes={len(changes)}")

        # 3) 상태 저장(마지막 스냅샷 1줄 JSON)
        pg_set_state(STATE_KEY, cur_map)
        print("[PG] saved current state")

        # 4) 스냅샷 메일 (옵션)
        if SAVE_SNAPSHOT_FILES:
            snapshot_path = save_snapshot_copy_excel(cur_df, checked_at)
            cleanup_old_snapshots(KEEP_SNAPSHOT_FILES)

            subject = f"<스냅샷> {checked_at} (collected={len(cur_df)})"
            body = f"""
            <p><b>스냅샷 완료</b></p>
            <p>시간: <b>{checked_at}</b></p>
            <p>수집: <b>{len(cur_df)}</b>개 (목표 {TARGET_UNIQUE})</p>
            <p>STATE_KEY: <b>{STATE_KEY}</b></p>
            <p>첨부: <b>전체 스냅샷 엑셀</b></p>
            """
            send_email(SNAPSHOT_TO, subject, body, attachments=[snapshot_path])
            print(f"snapshot mail sent | to={','.join(SNAPSHOT_TO)} | collected={len(cur_df)} | {checked_at}")

        # 5) 변동 메일
        if changes:
            issue_subject = f"<가격변동 확인필요> {checked_at} ({len(changes)}건)"
            issue_body = build_issue_email_body(changes, checked_at)
            change_path = save_changes_excel(changes, checked_at)
            send_email(ALERT_TO, issue_subject, issue_body, attachments=[change_path])
            print(f"ISSUE mail sent | to={','.join(ALERT_TO)} | issue={len(changes)} | {checked_at}")
        else:
            print(f"no change | collected={len(cur_df)} | {checked_at}")

    except Exception as e:
        err = traceback.format_exc()
        print(err)
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