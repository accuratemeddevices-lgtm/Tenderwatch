# ============================================================
# TENDERWATCH SCRAPER - PARALLEL WORKER ENGINE
# ============================================================

import os, asyncio, random, re, hashlib, time
import requests as req_lib
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import execute_values

# ── DATABASE CONFIGURATION ──────────────────────────────────────
NEON_URL = os.environ.get("NEON_DATABASE_URL")
WORKER_ID = os.environ.get("WORKER_ID", "local-worker")

if not NEON_URL:
    raise ValueError("CRITICAL: NEON_DATABASE_URL not found in environment variables!")

def get_conn():
    return psycopg2.connect(NEON_URL, connect_timeout=10)

def ensure_conn(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return conn
    except Exception:
        try: conn.close()
        except: pass
        return get_conn()

# ── PARALLEL QUEUE MANAGEMENT ──────────────────────────────────
def init_queue(conn, portals):
    """Creates the queue and resets it daily so workers can start fresh."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scraping_queue (
            portal_name TEXT PRIMARY KEY,
            status TEXT DEFAULT 'pending',
            worker_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for p in portals:
        # Insert portals, or reset them to pending if they haven't been touched in 12 hours
        cur.execute("""
            INSERT INTO scraping_queue (portal_name, status, updated_at)
            VALUES (%s, 'pending', CURRENT_TIMESTAMP)
            ON CONFLICT (portal_name) DO UPDATE SET
                status = CASE WHEN scraping_queue.updated_at < CURRENT_TIMESTAMP - INTERVAL '12 hours' THEN 'pending' ELSE scraping_queue.status END,
                updated_at = CASE WHEN scraping_queue.updated_at < CURRENT_TIMESTAMP - INTERVAL '12 hours' THEN CURRENT_TIMESTAMP ELSE scraping_queue.updated_at END;
        """, (p['name'],))
    conn.commit()
    cur.close()

def get_next_portal(conn):
    """Atomically fetches and locks the next pending portal for this specific worker."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE scraping_queue
        SET status = 'running', worker_id = %s, updated_at = CURRENT_TIMESTAMP
        WHERE portal_name = (
            SELECT portal_name FROM scraping_queue
            WHERE status = 'pending'
            ORDER BY portal_name
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING portal_name;
    """, (WORKER_ID,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row[0] if row else None

def mark_completed(conn, portal_name):
    conn = ensure_conn(conn) # Wakes up dead connections
    cur = conn.cursor()
    cur.execute("UPDATE scraping_queue SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE portal_name = %s", (portal_name,))
    conn.commit()
    cur.close()
    return conn
    
# ── CORE SCRAPING FUNCTIONS (From Your Code) ───────────────────
def get_existing_tenders(conn, tender_ids):
    if not tender_ids: return {}
    conn = ensure_conn(conn)
    cur = conn.cursor()
    cur.execute("SELECT tender_id, detail_scraped FROM tenders_data WHERE tender_id = ANY(%s)", (list(tender_ids),))
    result = {row[0]: bool(row[1]) for row in cur.fetchall()}
    cur.close()
    return result

def save_tenders(tenders, conn):
    if not tenders: return conn, 0, 0
    conn = ensure_conn(conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tenders_data;")
    before = cur.fetchone()[0]

    rows = []
    for t in tenders:
        tid = str(t.get("tender_id","")).strip()
        if not tid: tid = "noid:" + hashlib.md5((t.get("title","") + t.get("closing_date","")).encode()).hexdigest()

        published_at = None
        for fmt in ["%b %d, %Y %I:%M:%S %p", "%d-%b-%Y %I:%M %p", "%d-%b-%Y", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"]:
            try:
                published_at = datetime.strptime(str(t.get("published_date","")).strip(), fmt)
                break
            except: continue

        rows.append((
            str(t.get("source_portal",""))[:500], str(t.get("source_url",""))[:500],
            str(t.get("published_date",""))[:100], published_at, str(t.get("closing_date",""))[:100],
            str(t.get("opening_date",""))[:100], str(t.get("title",""))[:2000],
            str(t.get("organisation",""))[:1000], str(t.get("tender_value",""))[:200],
            str(t.get("detail_url",""))[:1000], tid[:200], str(t.get("product_category",""))[:200],
            str(t.get("work_description",""))[:5000], str(t.get("location",""))[:500],
            str(t.get("pincode",""))[:20], str(t.get("emd_amount",""))[:200],
            str(t.get("tender_fee",""))[:200], str(t.get("period_of_work",""))[:100],
            str(t.get("bid_validity",""))[:100], str(t.get("inviting_authority",""))[:500],
            str(t.get("inviting_address",""))[:1000], bool(t.get("detail_scraped", False)), datetime.now(),
        ))

    execute_values(cur, """
        INSERT INTO tenders_data (
            source_portal, source_url, published_date, published_at, closing_date, opening_date, title, organisation,
            tender_value, detail_url, tender_id, product_category, work_description, location, pincode, emd_amount, tender_fee,
            period_of_work, bid_validity, inviting_authority, inviting_address, detail_scraped, scraped_at
        ) VALUES %s
        ON CONFLICT (tender_id) DO UPDATE SET
            title              = EXCLUDED.title, closing_date       = EXCLUDED.closing_date,
            product_category   = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.product_category ELSE tenders_data.product_category END,
            work_description   = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.work_description ELSE tenders_data.work_description END,
            location           = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.location ELSE tenders_data.location END,
            pincode            = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.pincode ELSE tenders_data.pincode END,
            emd_amount         = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.emd_amount ELSE tenders_data.emd_amount END,
            tender_fee         = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.tender_fee ELSE tenders_data.tender_fee END,
            period_of_work     = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.period_of_work ELSE tenders_data.period_of_work END,
            bid_validity       = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.bid_validity ELSE tenders_data.bid_validity END,
            inviting_authority = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.inviting_authority ELSE tenders_data.inviting_authority END,
            inviting_address   = CASE WHEN EXCLUDED.detail_scraped THEN EXCLUDED.inviting_address ELSE tenders_data.inviting_address END,
            detail_scraped     = CASE WHEN EXCLUDED.detail_scraped THEN TRUE ELSE tenders_data.detail_scraped END,
            scraped_at         = EXCLUDED.scraped_at
    """, rows)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM tenders_data;")
    after = cur.fetchone()[0]
    cur.close()
    ins = after - before
    return conn, ins, len(rows) - ins


# (Include your exact parse_gepnic_page, parse_gepnic_detail, find_next_link, scrape_one_detail, mark_expired_tenders, and run_pincode_mapping functions here unchanged)
def parse_gepnic_page(html, pname, base_url):
    soup = BeautifulSoup(html, "html.parser")
    tenders = []
    tables = soup.find_all("table")
    if not tables: return []
    tbl = max(tables, key=lambda t: len(t.find_all("tr")))

    for row in tbl.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 5: continue
        sno = cols[0].get_text(strip=True).rstrip(".")
        if not sno.isdigit(): continue
        try:
            detail_url, title_cell = "", None
            for ci in range(len(cols)):
                lnk = cols[ci].find("a")
                if lnk and lnk.get("href"):
                    h = lnk["href"]
                    if "DirectLink" in h or "direct" in h.lower():
                        detail_url = base_url + h if h.startswith("/") else h
                        title_cell = cols[ci]
                        break
            if not title_cell:
                title_cell = cols[4] if len(cols) > 4 else cols[-1]
                lnk = title_cell.find("a")
                if lnk and lnk.get("href"):
                    h = lnk["href"]
                    detail_url = base_url + h if h.startswith("/") else h

            title = title_cell.get_text(separator=" ", strip=True) if title_cell else ""
            tid = (re.findall(r'\[([^\]]+)\]', title) or [""])[-1].strip()

            org = ""
            for ci in [5, 6, 7]:
                if len(cols) > ci:
                    txt = cols[ci].get_text(strip=True)
                    if txt and not txt.replace(",","").replace(".","").isdigit():
                        org = txt.replace("||"," > ")
                        break

            tenders.append({
                "source_portal":  pname, "source_url": base_url, "published_date": cols[1].get_text(strip=True),
                "closing_date": cols[2].get_text(strip=True), "opening_date": cols[3].get_text(strip=True),
                "title": title, "organisation": org, "detail_url": detail_url, "tender_id": tid,
            })
        except: continue
    return tenders

def parse_gepnic_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    r = {}
    def process_pair(key, val):
        key, val = key.strip().lower(), val.strip()
        if not val or val in ("NA","N/A","Nil","nil","","-","--"): return
        if len(val) > 2000: val = val[:2000]

        if "work description" in key: r.setdefault("work_description", val)
        elif ("location" in key and "pincode" not in key and "bid opening" not in key and "pre bid" not in key and "meeting" not in key): r.setdefault("location", val)
        elif "pincode" in key: r.setdefault("pincode", re.sub(r'[^\d]','',val)[:6])
        elif "emd amount" in key: r.setdefault("emd_amount", val.replace("₹","").replace(",","").strip())
        elif "tender fee in" in key: r.setdefault("tender_fee", re.sub(r'[^\d.]','',val).strip())
        elif "product category" in key: r.setdefault("product_category", val)
        elif "period of work" in key: r.setdefault("period_of_work", val)
        elif "bid validity" in key: r.setdefault("bid_validity", val)
        elif key == "name": r.setdefault("inviting_authority", val)
        elif key == "address": r.setdefault("inviting_address", val[:500])
        elif key == "tender id": r.setdefault("detail_tender_id", val)

    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) == 2: process_pair(cols[0].get_text(strip=True), cols[1].get_text(separator=" ", strip=True))
        elif len(cols) == 4:
            process_pair(cols[0].get_text(strip=True), cols[1].get_text(separator=" ", strip=True))
            process_pair(cols[2].get_text(strip=True), cols[3].get_text(separator=" ", strip=True))
        elif len(cols) == 6:
            process_pair(cols[0].get_text(strip=True), cols[1].get_text(separator=" ", strip=True))
            process_pair(cols[2].get_text(strip=True), cols[3].get_text(separator=" ", strip=True))
            process_pair(cols[4].get_text(strip=True), cols[5].get_text(separator=" ", strip=True))
    return r

def is_detail_page(html):
    t = html.lower()
    return ("basic details" in t or "work item details" in t or "emd fee details" in t or "tender details" in t)

async def find_next_link(page):
    for a in await page.query_selector_all("a"):
        try:
            if re.match(r'^next\s*[>›»]?\s*$', (await a.inner_text()).strip(), re.I): return a
        except: pass
    return None

async def scrape_one_detail(pg, detail_url, listing_url, listing_tender_id=""):
    try:
        try: await pg.goto(detail_url, timeout=15000, wait_until="domcontentloaded")
        except: await asyncio.sleep(2)
        try: await pg.wait_for_load_state("networkidle", timeout=10000)
        except: await asyncio.sleep(2)

        html = await pg.content()
        extra = {}
        if is_detail_page(html):
            extra = parse_gepnic_detail(html)
            if listing_tender_id and extra.get("detail_tender_id"):
                if extra["detail_tender_id"].strip().lower() not in listing_tender_id.strip().lower():
                    try:
                        await pg.goto(listing_url, timeout=15000, wait_until="domcontentloaded")
                        await asyncio.sleep(1)
                    except: pass
                    return {}
            extra.pop("detail_tender_id", None)

        back_clicked = False
        for sel in ["a:has-text('Back')", "input[value='Back']", "button:has-text('Back')"]:
            try:
                btn = await pg.query_selector(sel)
                if btn:
                    try:
                        async with pg.expect_navigation(timeout=10000): await btn.click()
                    except: await asyncio.sleep(2)
                    back_clicked = True
                    break
            except: continue
        if not back_clicked:
            try:
                await pg.goto(listing_url, timeout=15000, wait_until="domcontentloaded")
                await asyncio.sleep(1)
            except:
                await pg.go_back()
                await asyncio.sleep(1.5)
        return extra
    except Exception:
        try:
            await pg.goto(listing_url, timeout=15000, wait_until="domcontentloaded")
            await asyncio.sleep(2)
        except: pass
        return {}

def run_pincode_mapping(conn, batch_size=1000):
    print("  📍 Post-Processing: Pincode mapping...")
    # Add your existing logic here...
    return conn

def mark_expired_tenders(conn):
    print("  ⏰ Post-Processing: Marking expired tenders...")
    # Add your existing logic here...
    return 0

# ── WORKER EXECUTION LOOP ──────────────────────────────────────
async def scrape_portal(portal, pg, conn):
    """Scrapes a single portal."""
    print(f"\n{'='*60}\n📡 WORKER {WORKER_ID} SCRAPING: {portal['name']}\n{'='*60}")
    pname, base_url, listing_url = portal["name"], portal["base_url"], portal["listing_url"]
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await pg.goto(listing_url, timeout=60000, wait_until="domcontentloaded")
            break
        except Exception as e:
            if attempt == max_retries - 1: 
                print(f"  ❌ Failed to load listing page: {e}")
                return
            await asyncio.sleep(5)

    for org_idx in range(250):
        try:
            # Check if page is still alive before continuing
            if pg.is_closed():
                print("  ❌ Browser page was unexpectedly closed. Aborting this portal.")
                return

            try: await pg.wait_for_selector("text='Organisation Name'", timeout=15000)
            except:
                await pg.goto(listing_url, timeout=30000, wait_until="domcontentloaded")
                await pg.wait_for_selector("text='Organisation Name'", timeout=15000)

            link_handle = await pg.evaluate_handle(f'''(idx) => {{
                const headerRow = Array.from(document.querySelectorAll("tr")).find(r => r.innerText.includes("Organisation Name"));
                if (!headerRow) return null;
                const dataRows = Array.from(headerRow.closest("table").querySelectorAll("tr")).filter(r => {{
                    const tds = r.querySelectorAll("td");
                    return tds.length >= 3 && tds[0].innerText.trim().match(/^\\d+\\.?$/);
                }});
                return idx < dataRows.length ? dataRows[idx].querySelectorAll("td")[dataRows[idx].querySelectorAll("td").length - 1].querySelector("a") : null;
            }}''', org_idx)

            try:
                async with pg.expect_navigation(timeout=30000): await link_handle.click()
            except: continue

            await asyncio.sleep(2)
            while True:
                if pg.is_closed(): return # Safety check
                
                html = await pg.content()
                tenders = parse_gepnic_page(html, pname, base_url)
                if not tenders: break

                existing_status = get_existing_tenders(conn, [t.get("tender_id","") for t in tenders])
                needs_detail = [i for i, t in enumerate(tenders) if t.get("tender_id","") not in existing_status or not existing_status[t.get("tender_id","")]]

                for idx, row_i in enumerate(needs_detail):
                    if pg.is_closed(): return # Safety check
                    t = tenders[row_i]
                    extra = await scrape_one_detail(pg, t.get("detail_url",""), pg.url, t.get("tender_id",""))
                    if extra is None: break
                    if extra:
                        t.update(extra)
                        t["detail_scraped"] = True
                    else: t["detail_scraped"] = False
                    await asyncio.sleep(random.uniform(0.5, 1.0))

                conn, ins, skp = save_tenders(tenders, conn)

                nxt = await find_next_link(pg)
                if not nxt: break
                try:
                    async with pg.expect_navigation(timeout=20000): await nxt.click()
                except: break

            back_clicked = False
            for sel in ["a:has-text('Back')", "input[value='Back']"]:
                try:
                    btn = await pg.query_selector(sel)
                    if btn:
                        async with pg.expect_navigation(timeout=10000): await btn.click()
                        back_clicked = True
                        break
                except: pass
            if not back_clicked: await pg.goto(listing_url, timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f"  ⚠️ Recoverable error on org {org_idx}: {e}")
            continue

async def worker_loop():
    PORTALS = [
        {"name": "CPPP (eprocure.gov.in)", "base_url": "https://eprocure.gov.in", "listing_url": "https://eprocure.gov.in/eprocure/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "eTenders", "base_url": "https://etenders.gov.in", "listing_url": "https://etenders.gov.in/eprocure/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "NTPC", "base_url": "https://eprocurentpc.nic.in", "listing_url": "https://eprocurentpc.nic.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "BHEL", "base_url": "https://eprocurebhel.co.in", "listing_url": "https://eprocurebhel.co.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "Coal India", "base_url": "https://coalindiatenders.nic.in", "listing_url": "https://coalindiatenders.nic.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "Daman & Diu", "base_url": "https://ddtenders.gov.in", "listing_url": "https://ddtenders.gov.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"},
        {"name": "Dadra & NH", "base_url": "https://dnhtenders.gov.in", "listing_url": "https://dnhtenders.gov.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"}
    ]

    conn = get_conn()
    init_queue(conn, PORTALS)

    while True:
        # 1. Fetch the next available portal from DB
        portal_name = get_next_portal(conn)
        
        # 2. If nothing is pending, this worker is done
        if not portal_name:
            print(f"🏁 WORKER {WORKER_ID}: No pending portals left in queue. Shutting down.")
            break
            
        # 3. Find the config
        portal_config = next((p for p in PORTALS if p["name"] == portal_name), None)
        if portal_config:
            
            # 🔥 THE FIX: Launch a FRESH browser for EVERY portal to prevent Memory/RAM crashes
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage", "--disable-gpu"])
                    ctx = await browser.new_context(viewport={"width":1280,"height":900})
                    pg = await ctx.new_page()
                    
                    # Scrape the portal
                    await scrape_portal(portal_config, pg, conn)
                    
                    await browser.close()
                
                # Only mark completed if the browser didn't completely blow up
                conn = mark_completed(conn, portal_name)
                print(f"✅ WORKER {WORKER_ID}: Finished {portal_name}")
                
            except Exception as e:
                print(f"❌ WORKER {WORKER_ID}: Fatal Browser Crash on {portal_name}: {e}")
                # We skip mark_completed so it stays 'running' or we can manual reset it later.
                # The worker survives and will pick up the next portal in the queue.

    # POST-PROCESSING: Only the last worker to finish should run this
    conn = ensure_conn(conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraping_queue WHERE status != 'completed'")
    if cur.fetchone()[0] == 0:
        cur.execute("UPDATE scraping_queue SET status = 'post_processing' WHERE portal_name = %s", (PORTALS[0]['name'],))
        if cur.rowcount > 0: # Ensure we acquired the lock to process
            print("🚀 ALL WORKERS DONE. Starting Post-Processing...")
            mark_expired_tenders(conn)
            run_pincode_mapping(conn, batch_size=1000)
    cur.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(worker_loop())
