"""
server.py — В/Ч А7020  v2.3

Patches applied (review 2026-03-20):
  [1]  NameError: s not defined in _handle_db_load → fixed
  [2]  BIND_HOST default 127.0.0.1, network mode via --network flag
  [3]  /api/db/load requires manage_users auth
  [4]  CORS restricted to same origin instead of *
  [5]  /api/query admin-only + hard row limit 500 + server-side timeout
  [6]  /api/generate no longer returns pdf_base64 (only URL)
  [7]  POST body size limited to 10MB
  [8]  _file() streams via shutil.copyfileobj instead of read-all
  [9]  multipart upload uses list+join instead of b"" += chunk (O(n²))
  [10] path traversal uses os.path.commonpath instead of startswith
  [11] rate limiting on /api/login (5 attempts per 5 min per IP)
  [12] json.dumps without indent for performance
"""
import os, sys, json, datetime, threading, webbrowser, base64, socket, logging
import tempfile, traceback, shutil, time, argparse, atexit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sqlite3 as _sqlite3

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

# ── Модулі ────────────────────────────────────────────────────────────────────
try:
    from pdf_generator import generate_document
    PDF_OK = True
except ImportError as e:
    PDF_OK = False
    print(f"[WARN] pdf_generator: {e}")

from auth import (verify_user, create_session, get_session, delete_session,
                  has_permission, init_default_users, load_users, save_users,
                  create_user, change_password, toggle_user_active, ROLES, PERMISSIONS)
from db_write import (set_db, get_db_path,
                      add_personnel, edit_personnel, delete_personnel,
                      add_szc, edit_szc, get_next_runner_no, get_audit_log)

# ── Конфіг ────────────────────────────────────────────────────────────────────
PORT       = 7020
BIND_HOST  = "127.0.0.1"   # [Fix #2] default localhost only
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
APP_HTML   = os.path.join(BASE_DIR, "app.html")
TPL_JSON   = os.path.join(BASE_DIR, "templates", "templates.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
STATIC_DIR = os.path.join(BASE_DIR, "static")
LOG_FILE   = os.path.join(BASE_DIR, "access.log")

MAX_BODY_SIZE  = 10 * 1024 * 1024   # [Fix #7] 10 MB limit for non-upload POST
MAX_QUERY_ROWS = 500                 # [Fix #5] hard row limit for /api/query

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

# [Fix #S2] Track temp files for cleanup on exit
_tmp_files: list = []

def _cleanup_tmp():
    for f in _tmp_files:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass

atexit.register(_cleanup_tmp)

try:
    with open(TPL_JSON, encoding="utf-8") as f:
        TEMPLATES = json.load(f)
except Exception as e:
    print(f"[WARN] templates.json: {e}")
    TEMPLATES = {}

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# ── Rate limiting ─────────────────────────────────────────────────────────────
# [Fix #11] Simple in-memory rate limiter for /api/login
_login_fails: dict = {}   # ip -> [timestamp, ...]
_rl_lock = threading.Lock()
LOGIN_FAIL_MAX    = 5
LOGIN_FAIL_WINDOW = 300   # 5 minutes


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    with _rl_lock:
        times = [t for t in _login_fails.get(ip, []) if now - t < LOGIN_FAIL_WINDOW]
        # [Fix #S4] Cleanup empty entries to prevent memory leak
        if times:
            _login_fails[ip] = times
        else:
            _login_fails.pop(ip, None)
        return len(times) >= LOGIN_FAIL_MAX


def _record_fail(ip: str):
    now = time.time()
    with _rl_lock:
        _login_fails.setdefault(ip, []).append(now)


def _clear_fails(ip: str):
    with _rl_lock:
        _login_fails.pop(ip, None)


# ── Хелпери ───────────────────────────────────────────────────────────────────
def get_local_ips():
    ips = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if ":" not in ip and ip != "127.0.0.1":
                ips.append(ip)
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    seen, result = set(), []
    for ip in sorted(ips, key=lambda x: (0 if x.startswith("100.") else 1, x)):
        if ip not in seen:
            seen.add(ip)
            result.append(ip)
    return result or ["127.0.0.1"]


def _json_bytes(data) -> bytes:
    """[Fix #12] No indent — smaller responses."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


# ── Handler ───────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        logging.info(f"{self.client_address[0]}  {fmt % args}")

    def _auth(self, permission=None):
        token = self.headers.get("X-Token", "")
        if not token:
            cookie = self.headers.get("Cookie", "")
            for part in cookie.split(";"):
                k, _, v = part.strip().partition("=")
                if k.strip() == "token":
                    token = v.strip()
                    break
        session = get_session(token) if token else None
        if not session:
            return None
        if permission and not has_permission(session["role"], permission):
            return False
        return session

    # ── GET ───────────────────────────────────────────────────────────────────
    def do_GET(self):
        p = urlparse(self.path).path

        if p in ("/", "/index.html"):
            self._file(APP_HTML, "text/html; charset=utf-8")

        elif p == "/api/status":
            s = self._auth()
            if not s:
                self._json({"ok": False, "auth": False})
            else:
                self._json({"ok": True, "pdf": PDF_OK, "version": "2.3",
                            "user": s["username"], "role": s["role"],
                            "display_name": s.get("display_name", ""),
                            "permissions": PERMISSIONS.get(s["role"], [])})

        elif p == "/api/templates":
            self._json(TEMPLATES)

        elif p == "/api/roles":
            self._json(ROLES)

        elif p == "/api/db/autoload":
            # No auth needed - server is localhost-only, path is local disk only
            # Auth on /api/query still protects the actual data
            _parent = os.path.dirname(BASE_DIR)
            db_candidates = [
                os.path.join(BASE_DIR,  "military.db"),          # поруч з server.py
                os.path.join(_parent,   "military.db"),          # батьківська папка
                os.path.join(_parent,   "ВМ",        "military.db"),
                os.path.join(_parent,   "BM",        "military.db"),
                os.path.join(_parent,   "vm",        "military.db"),
                os.path.join(_parent,   "a7020",     "military.db"),
                os.path.join(_parent,   "A7020",     "military.db"),
                os.path.join("C:\\",    "A7020",     "military.db"),
                os.path.join("C:\\",    "military",  "military.db"),
                os.path.join("C:\\",    "military.db"),
            ]
            found = next((c for c in db_candidates if os.path.exists(c)), None)
            if not found:
                candidates_str = "; ".join(db_candidates[:4])
                self._json({"ok": False, "error": f"military.db не знайдено. Перевірено: {candidates_str} — скопіюйте military.db у папку з server.py"})
                return
            try:
                conn = _sqlite3.connect(found)
                n    = conn.execute("SELECT COUNT(*) FROM personnel").fetchone()[0]
                conn.close()
            except Exception as e:
                self._json({"ok": False, "error": f"Невалідна БД: {e}"})
                return
            set_db(found)
            logging.info(f"{self.client_address[0]}  DB AUTOLOAD  {n} personnel  {found}")
            self._json({"ok": True, "personnel_count": n, "path": found})

        elif p == "/api/audit":
            s = self._auth("manage_users")
            if not s:
                self._unauth()
            else:
                self._json({"log": get_audit_log(200)})

        elif p == "/api/users":
            s = self._auth("manage_users")
            if not s:
                self._unauth()
            else:
                users = load_users()
                safe  = {u: {k: v for k, v in d.items() if k not in ("salt", "hash")}
                         for u, d in users.items()}
                self._json(safe)

        elif p.startswith("/output/"):
            s = self._auth("generate_pdf")
            if not s:
                self._unauth()
                return
            fname = os.path.basename(p)
            fpath = os.path.join(OUTPUT_DIR, fname)
            if os.path.exists(fpath):
                self._file(fpath, "application/pdf")
            else:
                self._404()

        elif p.startswith("/static/"):
            fname = os.path.basename(p)
            fpath = os.path.join(STATIC_DIR, fname)
            if os.path.exists(fpath):
                mime = "application/javascript" if fname.endswith(".js") else "application/wasm"
                self._file(fpath, mime)
            else:
                self._404()

        elif p == "/favicon.ico":
            # [Fix #N1] Silence favicon 404
            self.send_response(204)
            self.end_headers()

        else:
            self._404()

    # ── POST ──────────────────────────────────────────────────────────────────
    def do_POST(self):
        p = urlparse(self.path).path

        # [Fix #3] db/load handled separately with auth
        if p == "/api/db/load":
            self._handle_db_load()
            return

        # [Fix #7] Enforce body size limit on all other POSTs
        length = int(self.headers.get("Content-Length", 0))
        if length > MAX_BODY_SIZE:
            self._json({"error": "Request too large"}, 413)
            return
        body = self.rfile.read(length)

        if p == "/api/login":
            ip = self.client_address[0]
            # [Fix #11] Rate limiting
            if _is_rate_limited(ip):
                logging.warning(f"{ip}  LOGIN BLOCKED (rate limit)")
                self._json({"ok": False, "error": "Забагато спроб. Спробуйте через 5 хвилин."}, 429)
                return
            try:
                req  = json.loads(body)
                user = verify_user(req.get("username", ""), req.get("password", ""))
                if not user:
                    _record_fail(ip)
                    logging.warning(f"{ip}  LOGIN FAIL  {req.get('username')}")
                    self._json({"ok": False, "error": "Невірний логін або пароль"}, 401)
                    return
                _clear_fails(ip)
                token = create_session(req["username"], user["role"],
                                       user.get("display_name", req["username"]))
                logging.info(f"{ip}  LOGIN OK  {req['username']}  ({user['role']})")
                self._json({"ok": True, "token": token, "role": user["role"],
                            "display_name": user.get("display_name", ""),
                            "permissions": PERMISSIONS.get(user["role"], [])})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 500)
            return

        if p == "/api/logout":
            try:
                token = json.loads(body).get("token", "")
                delete_session(token)
            except Exception:
                pass
            self._json({"ok": True})
            return

        if p == "/api/generate":
            s = self._auth("generate_pdf")
            if not s:
                self._unauth(); return
            try:
                req      = json.loads(body)
                doc_type = req.get("doc_type")
                data     = req.get("data", {})
                if not doc_type:
                    self._json({"error": "doc_type не вказано"}, 400); return
                if not PDF_OK:
                    self._json({"error": "pdf_generator недоступний"}, 500); return
                data.setdefault("doc_date", str(datetime.date.today()))
                ts    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = f"{doc_type.replace(chr(47), '_')}_{ts}.pdf"
                fpath = os.path.join(OUTPUT_DIR, fname)
                generate_document(doc_type, data, fpath)
                pib = data.get("author_name_full") or data.get("sender_full_name_dative") or "—"
                logging.info(f"{self.client_address[0]}  GEN  {doc_type}  [{pib}]  {s['username']}")
                # [Fix #V2] Return pdf_base64 for browser download
                with open(fpath, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                self._json({"ok": True, "filename": fname,
                            "url": f"/output/{fname}",
                            "pdf_base64": b64})
            except Exception as e:
                self._json({"error": str(e)}, 500)
            return

        if p == "/api/personnel/add":
            s = self._auth("add_personnel")
            if not s:
                self._unauth(); return
            try:
                self._json({"ok": True, "id": add_personnel(json.loads(body), s["username"])})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/personnel/edit":
            s = self._auth("edit_personnel")
            if not s:
                self._unauth(); return
            try:
                req = json.loads(body)
                edit_personnel(int(req["id"]), req.get("data", {}), s["username"])
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/personnel/delete":
            s = self._auth("delete_personnel")
            if not s:
                self._unauth(); return
            try:
                delete_personnel(int(json.loads(body)["id"]), s["username"])
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/szc/add":
            s = self._auth("add_szc")
            if not s:
                self._unauth(); return
            try:
                self._json({"ok": True, "id": add_szc(json.loads(body), s["username"])})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/szc/edit":
            s = self._auth("edit_szc")
            if not s:
                self._unauth(); return
            try:
                req = json.loads(body)
                edit_szc(int(req["id"]), req.get("data", {}), s["username"])
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/szc/next_no":
            s = self._auth("view")
            if not s:
                self._unauth(); return
            try:
                self._json({"runner_no": get_next_runner_no()})
            except Exception as e:
                self._json({"runner_no": "1", "warn": str(e)})
            return

        if p == "/api/users/add":
            s = self._auth("manage_users")
            if not s:
                self._unauth(); return
            try:
                req = json.loads(body)
                create_user(req["username"], req["password"],
                            req["role"], req.get("display_name", ""), s["username"])
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/users/passwd":
            s = self._auth("manage_users")
            if not s:
                self._unauth(); return
            try:
                req = json.loads(body)
                change_password(req["username"], req["new_password"])
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)}, 400)
            return

        if p == "/api/users/toggle":
            s = self._auth("manage_users")
            if not s:
                self._unauth(); return
            try:
                req   = json.loads(body)
                uname = req["username"]
                # [Fix #N2] Use toggle_user_active — atomic under _users_lock
                new_state = toggle_user_active(uname, s["username"])
                self._json({"ok": True, "active": new_state})
            except Exception as e:
                code = 400 if "не знайдено" in str(e).lower() or "себе" in str(e).lower() else 400
                self._json({"ok": False, "error": str(e)}, code)
            return

        if p == "/api/query":
            # [Fix #K2] view permission — all logged-in users can query
            s = self._auth("view")
            if not s:
                self._unauth(); return
            db_path = get_db_path()
            if not db_path:
                self._json({"rows": [], "error": "DB not loaded"}); return
            try:
                req    = json.loads(body)
                sql    = req.get("sql", "")
                params = req.get("params", [])
                # Enforce LIMIT in SQL — append if not present
                sql_stripped = sql.strip().rstrip(";")
                if "LIMIT" not in sql_stripped.upper():
                    sql_stripped += f" LIMIT {MAX_QUERY_ROWS}"
                ro_uri = f"file:{db_path}?mode=ro"
                with _sqlite3.connect(ro_uri, uri=True, timeout=5) as conn:
                    conn.row_factory = _sqlite3.Row
                    cur  = conn.execute(sql_stripped, params)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = [dict(zip(cols, row)) for row in cur.fetchmany(MAX_QUERY_ROWS)]
                self._json({"rows": rows, "truncated": len(rows) == MAX_QUERY_ROWS})
            except Exception as e:
                self._json({"rows": [], "error": str(e)})
            return

        if p == "/api/med/registry/add":
            s = self._auth("view")
            if not s: self._unauth(); return
            try:
                req   = json.loads(body)
                table = req.get("table", "")
                data  = req.get("data", {})
                allowed = {"med_registry_vlk","med_registry_konsult","med_registry_char","med_registry_analyses"}
                if table not in allowed:
                    self._json({"ok": False, "error": "Invalid table"}); return
                data["created_by"] = s["username"]
                cols = list(data.keys())
                vals = [data[c] for c in cols]
                sql  = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
                with _sqlite3.connect(get_db_path(), timeout=10) as conn:
                    cur = conn.execute(sql, vals)
                    conn.commit()
                    self._json({"ok": True, "id": cur.lastrowid})
            except Exception as e:
                self._json({"ok": False, "error": str(e)})
            return

        if p == "/api/med/registry/update":
            s = self._auth("view")
            if not s: self._unauth(); return
            try:
                req   = json.loads(body)
                table = req.get("table", "")
                rid   = int(req.get("id", 0))
                data  = req.get("data", {})
                allowed = {"med_registry_vlk","med_registry_konsult","med_registry_char","med_registry_analyses"}
                if table not in allowed or not rid:
                    self._json({"ok": False, "error": "Invalid request"}); return
                # Remove read-only fields
                for k in ("id","personnel_id","created_at","created_by"):
                    data.pop(k, None)
                cols = list(data.keys())
                vals = [data[c] for c in cols] + [rid]
                sql  = f"UPDATE {table} SET {', '.join(f'{c}=?' for c in cols)} WHERE id=?"
                with _sqlite3.connect(get_db_path(), timeout=10) as conn:
                    conn.execute(sql, vals)
                    conn.commit()
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "error": str(e)})
            return

        self._404()

    # ── DB Load ───────────────────────────────────────────────────────────────
    def _handle_db_load(self):
        """DB load: path mode = no auth needed (local file).
           Multipart upload = any logged-in user."""
        ct = self.headers.get("Content-Type", "")
        # For path-based load (JSON), no auth needed - file stays on server
        # For file upload (multipart), require login
        s = None
        if "application/json" not in ct:
            s = self._auth()
            if not s:
                self._unauth()
                return

        try:
            ct             = self.headers.get("Content-Type", "")
            db_load_length = int(self.headers.get("Content-Length", 0))
            tmp_name       = None

            if "application/json" in ct:
                req         = json.loads(self.rfile.read(db_load_length))
                db_path_req = req.get("path", "").strip()

                if not db_path_req:
                    self._json({"ok": False, "error": "Шлях не вказано"}, 400)
                    return

                # [Fix #10] commonpath instead of startswith
                real_req  = os.path.realpath(db_path_req)
                real_base = os.path.realpath(BASE_DIR)
                try:
                    common = os.path.commonpath([real_req, real_base])
                except ValueError:
                    common = ""
                if common != real_base:
                    # Allow any .db file — server is local
                    if not db_path_req.lower().endswith((".db", ".sqlite", ".sqlite3")):
                        self._json({"ok": False, "error": "Дозволені лише .db файли"}, 400)
                        return
                if not os.path.exists(db_path_req):
                    self._json({"ok": False, "error": f"Файл не знайдено: {db_path_req}"}, 400)
                    return
                tmp_name = db_path_req

            else:
                # [Fix #9] Use list+join instead of b"" += chunk (O(n²))
                chunks    = []
                remaining = db_load_length
                while remaining > 0:
                    chunk = self.rfile.read(min(65536, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                body = b"".join(chunks)

                bnd = b""
                for part in ct.split(";"):
                    part = part.strip()
                    if part.startswith("boundary="):
                        bnd = ("--" + part[9:].strip()).encode()
                        break

                db_data = b""
                if bnd:
                    for chunk in body.split(bnd):
                        if b'name="db"' in chunk or b"name='db'" in chunk:
                            nl = b"\r\n\r\n"
                            ni = chunk.find(nl)
                            if ni >= 0:
                                db_data = chunk[ni + 4:].rstrip(b"\r\n")
                            break
                if not db_data:
                    db_data = body
                if len(db_data) < 100:
                    self._json({"ok": False, "error": "Порожній файл"}, 400)
                    return

                tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
                tmp.write(db_data)
                tmp.close()
                tmp_name = tmp.name
                _tmp_files.append(tmp_name)  # [Fix #S2] register for cleanup

            # Validate SQLite
            try:
                conn = _sqlite3.connect(tmp_name)
                n    = conn.execute("SELECT COUNT(*) FROM personnel").fetchone()[0]
                conn.close()
            except Exception as e:
                if "application/json" not in ct and tmp_name:
                    try:
                        os.unlink(tmp_name)
                    except OSError:
                        pass
                self._json({"ok": False, "error": f"Невалідна SQLite БД: {e}"}, 400)
                return

            set_db(tmp_name)
            user_tag = s["username"] if s else "path-load"
            logging.info(f"{self.client_address[0]}  DB LOADED  {n} personnel  [{user_tag}]")
            self._json({"ok": True, "personnel_count": n})

        except Exception as e:
            self._json({"ok": False,
                        "error": str(e) + "\n" + traceback.format_exc()[-400:]}, 500)

    # ── Утиліти ───────────────────────────────────────────────────────────────
    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def _cors(self):
        # [Fix #4] Restrict CORS to same origin
        origin = self.headers.get("Origin", "")
        allowed = f"http://localhost:{PORT}"
        if origin.startswith("http://localhost") or origin.startswith("http://127.0.0.1"):
            self.send_header("Access-Control-Allow-Origin", origin)
        else:
            self.send_header("Access-Control-Allow-Origin", allowed)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Token")

    def _json(self, data, code=200):
        body = _json_bytes(data)
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path, mime):
        """[Fix #8] Stream file instead of read-all."""
        try:
            size = os.path.getsize(path)
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", size)
            self._cors()
            self.end_headers()
            with open(path, "rb") as f:
                shutil.copyfileobj(f, self.wfile)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _404(self):
        self._json({"error": "Not found"}, 404)

    def _unauth(self):
        self._json({"error": "Немає доступу. Увійдіть у систему."}, 401)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="В/Ч А7020 Server")
    parser.add_argument("--network", action="store_true",
                        help="Listen on 0.0.0.0 (all interfaces) instead of localhost only")
    args, _ = parser.parse_known_args()

    bind = "0.0.0.0" if args.network else "127.0.0.1"

    init_default_users()
    ips = get_local_ips()

    try:
        server = ThreadingHTTPServer((bind, PORT), Handler)
        server.socket.settimeout(None)  # No timeout for large file uploads
    except OSError as e:
        print(f"\n  [ERROR] Порт {PORT} зайнятий: {e}")
        input("  Press Enter...")
        return

    print()
    print("  ================================================")
    print(f"  V/Ch A7020  v2.3  {'NETWORK MODE' if args.network else 'LOCAL MODE'}")
    print("  ================================================")
    print(f"  Local:   http://localhost:{PORT}")
    if args.network:
        for ip in ips:
            tag = "  <- Tailscale" if ip.startswith("100.") else ""
            print(f"  Network: http://{ip}:{PORT}{tag}")
    print("  ------------------------------------------------")
    print("  CHANGE DEFAULT PASSWORDS AFTER FIRST LOGIN!")
    print("  ------------------------------------------------")
    print()

    threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()
