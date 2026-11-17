# -*- coding: utf-8 -*-
"""
Egg Dashboard Banna Plant - เซิร์ฟเวอร์เก็บข้อมูลกลาง
=====================================================
- ใช้ Python 3.8+ เท่านั้น ไม่ต้องติดตั้งอะไรเพิ่ม
- เก็บข้อมูลลงไฟล์ฐานข้อมูล  data/egg_dashboard.db  (SQLite)
- สำรองฐานข้อมูลอัตโนมัติวันละ 1 ครั้งที่  data/backups/
- เปิดหน้าเว็บ Dashboard ให้ด้วย  →  http://<ชื่อเครื่อง หรือ IP>:8080/

API (ตรงกับโหมด "REST API กลาง" ในหน้า 5 ของ Dashboard)
  GET  /api/day/<YYYY-MM-DD>      -> {"payload": {...}, "modified": "..."}  หรือ 404
  PUT  /api/day/<YYYY-MM-DD>      <- {"payload": {...}}
  GET  /api/config/SETTINGS
  PUT  /api/config/SETTINGS
  GET  /api/days?from=&to=        -> รายการวันที่มีข้อมูล (+ข้อมูล ถ้า &full=1)
  GET  /api/backup                -> ดาวน์โหลดข้อมูลทั้งหมด (.json)
  GET  /api/health

วิธีรัน:  python server.py            (พอร์ต 8080)
          python server.py --port 9000
"""
import argparse
import json
import os
import re
import shutil
import socket
import sqlite3
import threading
import time
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
DB_PATH = os.path.join(DATA_DIR, "egg_dashboard.db")
PAGE_CANDIDATES = ["index.html", "index.html.html", "Dashboard-Production.html"]


def default_page():
    for f in PAGE_CANDIDATES:
        if os.path.exists(os.path.join(BASE_DIR, f)):
            return f
    return PAGE_CANDIDATES[0]
KEEP_BACKUPS = 60                      # เก็บไฟล์สำรองย้อนหลังกี่วัน
MAX_BODY = 20 * 1024 * 1024            # 20 MB ต่อครั้ง

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
KEY_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
_db_lock = threading.Lock()


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
def db():
    con = sqlite3.connect(DB_PATH, timeout=15)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS days(
                         date TEXT PRIMARY KEY, payload TEXT NOT NULL,
                         modified TEXT NOT NULL, modified_by TEXT)""")
        con.execute("""CREATE TABLE IF NOT EXISTS config(
                         key TEXT PRIMARY KEY, payload TEXT NOT NULL, modified TEXT NOT NULL)""")
        # เก็บประวัติทุกครั้งที่มีการบันทึก (ย้อนดู/กู้คืนได้ถ้าข้อมูลถูกแก้ผิด)
        con.execute("""CREATE TABLE IF NOT EXISTS day_history(
                         id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL,
                         payload TEXT NOT NULL, saved_at TEXT NOT NULL, saved_by TEXT)""")
        con.execute("CREATE INDEX IF NOT EXISTS ix_hist_date ON day_history(date)")


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def merge_day(local, remote):
    """รวมข้อมูล 2 ชุดแบบเดียวกับ mergeDay() ในหน้าเว็บ — กันข้อมูลหายเมื่อกรอกพร้อมกัน"""
    if not remote:
        return local
    if not local:
        return remote
    dead = set((local.get("_deleted") or []) + (remote.get("_deleted") or []))
    seen, prod = set(), []
    for p in (remote.get("production") or []) + (local.get("production") or []):
        pid = p.get("_id")
        if pid and pid in dead:
            continue
        k = pid or "|".join(str(p.get(x, "")) for x in ("station", "code", "customer", "time", "qty"))
        if k not in seen:
            seen.add(k)
            prod.append(p)
    prod.sort(key=lambda p: str(p.get("time") or ""))
    lt, rt = local.get("_ts") or 0, remote.get("_ts") or 0
    newer, older = (local, remote) if lt >= rt else (remote, local)
    orders = json.loads(json.dumps(older.get("orders") or {}))
    for code, row in (newer.get("orders") or {}).items():
        if code not in orders:
            orders[code] = row
        else:
            orders[code].setdefault("byCustomer", {}).update(row.get("byCustomer") or {})
    confirmed = dict(older.get("confirmed") or {})
    confirmed.update(newer.get("confirmed") or {})
    return {
        "orders": orders, "production": prod, "confirmed": confirmed,
        "orderImportedAt": newer.get("orderImportedAt") or older.get("orderImportedAt"),
        "prodImportedAt": newer.get("prodImportedAt") or older.get("prodImportedAt"),
        "_deleted": sorted(dead), "_ts": max(lt, rt),
    }


def get_day(date):
    with db() as con:
        r = con.execute("SELECT payload, modified FROM days WHERE date=?", (date,)).fetchone()
    return (json.loads(r[0]), r[1]) if r else (None, None)


def put_day(date, payload, who):
    with _db_lock, db() as con:
        r = con.execute("SELECT payload FROM days WHERE date=?", (date,)).fetchone()
        existing = json.loads(r[0]) if r else None
        merged = merge_day(payload, existing) if existing else payload
        txt = json.dumps(merged, ensure_ascii=False)
        ts = now_iso()
        con.execute("INSERT INTO days(date,payload,modified,modified_by) VALUES(?,?,?,?) "
                    "ON CONFLICT(date) DO UPDATE SET payload=excluded.payload, "
                    "modified=excluded.modified, modified_by=excluded.modified_by",
                    (date, txt, ts, who))
        con.execute("INSERT INTO day_history(date,payload,saved_at,saved_by) VALUES(?,?,?,?)",
                    (date, txt, ts, who))
        # เก็บประวัติแต่ละวันไว้ไม่เกิน 200 เวอร์ชัน
        con.execute("DELETE FROM day_history WHERE date=? AND id NOT IN "
                    "(SELECT id FROM day_history WHERE date=? ORDER BY id DESC LIMIT 200)", (date, date))
    return merged, ts


def get_config(key):
    with db() as con:
        r = con.execute("SELECT payload, modified FROM config WHERE key=?", (key,)).fetchone()
    return (json.loads(r[0]), r[1]) if r else (None, None)


def put_config(key, payload):
    ts = now_iso()
    with _db_lock, db() as con:
        con.execute("INSERT INTO config(key,payload,modified) VALUES(?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload, modified=excluded.modified",
                    (key, json.dumps(payload, ensure_ascii=False), ts))
    return ts


def list_days(d_from, d_to, full):
    sql, args = "SELECT date, modified" + (", payload" if full else "") + " FROM days WHERE 1=1", []
    if d_from:
        sql += " AND date>=?"; args.append(d_from)
    if d_to:
        sql += " AND date<=?"; args.append(d_to)
    sql += " ORDER BY date"
    with db() as con:
        rows = con.execute(sql, args).fetchall()
    out = []
    for r in rows:
        item = {"date": r[0], "modified": r[1]}
        if full:
            item["payload"] = json.loads(r[2])
        out.append(item)
    return out


# --------------------------------------------------------------------------
# Daily backup
# --------------------------------------------------------------------------
def backup_once():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    target = os.path.join(BACKUP_DIR, "egg_dashboard_%s.db" % datetime.now().strftime("%Y-%m-%d"))
    if os.path.exists(target):
        return
    src = sqlite3.connect(DB_PATH)
    dst = sqlite3.connect(target)
    with dst:
        src.backup(dst)
    src.close(); dst.close()
    files = sorted(f for f in os.listdir(BACKUP_DIR) if f.endswith(".db"))
    for f in files[:-KEEP_BACKUPS]:
        try:
            os.remove(os.path.join(BACKUP_DIR, f))
        except OSError:
            pass
    print("[backup] " + target)


def backup_loop():
    while True:
        try:
            backup_once()
        except Exception as e:
            print("[backup] error:", e)
        time.sleep(3600)


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=BASE_DIR, **kw)

    def log_message(self, fmt, *args):
        if "/api/" in (self.path or "") and self.command == "GET":
            return                                # ไม่ log การ poll ถี่ ๆ
        print("%s  %s  %s" % (datetime.now().strftime("%H:%M:%S"), self.address_string(), fmt % args))

    # ---- helpers ----
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", self.headers.get("Origin") or "*")
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code, obj, extra_headers=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0 or n > MAX_BODY:
            raise ValueError("body size")
        return json.loads(self.rfile.read(n).decode("utf-8"))

    def _route(self):
        u = urlparse(self.path)
        parts = [unquote(p) for p in u.path.split("/") if p]
        return u, parts

    # ---- methods ----
    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()

    def do_GET(self):
        u, parts = self._route()
        if not parts:                                   # "/" -> หน้า Dashboard
            self.send_response(302)
            self.send_header("Location", "/" + default_page())
            self.end_headers(); return
        if parts[0] != "api":
            return super().do_GET()
        try:
            if parts[1:] == ["health"]:
                return self._json(200, {"ok": True, "app": "egg-dashboard-banna", "time": now_iso()})
            if len(parts) == 3 and parts[1] == "day":
                if not DATE_RE.match(parts[2]):
                    return self._json(404, {"error": "not found"})
                payload, mod = get_day(parts[2])
                if payload is None:
                    return self._json(404, {"error": "not found"})
                return self._json(200, {"payload": payload, "modified": mod})
            if len(parts) == 3 and parts[1] == "config" and KEY_RE.match(parts[2]):
                payload, mod = get_config(parts[2])
                if payload is None:
                    return self._json(404, {"error": "not found"})
                return self._json(200, {"payload": payload, "modified": mod})
            if parts[1:] == ["days"]:
                q = parse_qs(u.query)
                return self._json(200, {"days": list_days(q.get("from", [None])[0], q.get("to", [None])[0],
                                                          q.get("full", ["0"])[0] == "1")})
            if parts[1:] == ["backup"]:
                settings, _ = get_config("SETTINGS")
                dump = {"_app": "egg-dashboard-banna", "_version": 1, "exportedAt": now_iso(),
                        "settings": settings or {},
                        "days": {d["date"]: d["payload"] for d in list_days(None, None, True)}}
                fn = "Backup_Server_EggDashboard_%s.json" % datetime.now().strftime("%Y-%m-%d")
                return self._json(200, dump, {"Content-Disposition": 'attachment; filename="%s"' % fn})
            return self._json(404, {"error": "unknown endpoint"})
        except Exception as e:
            return self._json(500, {"error": str(e)})

    def do_PUT(self):
        u, parts = self._route()
        try:
            body = self._read_json()
            payload = body.get("payload", body) if isinstance(body, dict) else None
            if not isinstance(payload, dict):
                return self._json(400, {"error": "payload must be an object"})
            if len(parts) == 3 and parts[0] == "api" and parts[1] == "day" and DATE_RE.match(parts[2]):
                merged, ts = put_day(parts[2], payload, self.client_address[0])
                return self._json(200, {"ok": True, "modified": ts})
            if len(parts) == 3 and parts[0] == "api" and parts[1] == "config" and KEY_RE.match(parts[2]):
                ts = put_config(parts[2], payload)
                return self._json(200, {"ok": True, "modified": ts})
            return self._json(404, {"error": "unknown endpoint"})
        except ValueError as e:
            return self._json(400, {"error": "bad request: %s" % e})
        except Exception as e:
            return self._json(500, {"error": str(e)})


# --------------------------------------------------------------------------
def lan_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ips.add(s.getsockname()[0]); s.close()
    except OSError:
        pass
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if not ip.startswith("127."):
                ips.add(ip)
    except OSError:
        pass
    return sorted(ips)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="0.0.0.0")
    a = ap.parse_args()
    init_db()
    threading.Thread(target=backup_loop, daemon=True).start()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print("=" * 60)
    print(" Egg Dashboard Banna Plant — เซิร์ฟเวอร์เก็บข้อมูลกลาง")
    print(" ฐานข้อมูล : " + DB_PATH)
    print(" เปิดบนเครื่องนี้ : http://localhost:%d/" % a.port)
    for ip in lan_ips():
        print(" เครื่องอื่นเปิด  : http://%s:%d/" % (ip, a.port))
    print(" หน้า Dashboard : " + default_page())
    print(" จอ TV         : http://<IP ด้านบน>:%d/%s?mode=display" % (a.port, default_page()))
    print(" ปิดเซิร์ฟเวอร์: กด Ctrl+C")
    print("=" * 60)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nปิดเซิร์ฟเวอร์แล้ว")


if __name__ == "__main__":
    main()
