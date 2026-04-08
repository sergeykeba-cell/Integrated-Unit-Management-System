"""
auth.py — Авторизація для В/Ч А7020
PBKDF2-HMAC-SHA256, 600k ітерацій (NIST SP 800-132)

Patches applied:
  [A1] verify_user: full read-modify-write under _users_lock (TOCTOU fix)
  [A2] create_session: display_name passed as param, no disk I/O
  [A3] Atomic save_users via write-then-rename
  [A4] hmac.compare_digest timing-safe comparison
  [A5] Dummy hash for non-existent users (timing parity)
  [A6] Session cleanup job (expired sessions GC every hour)
"""
import json, hashlib, hmac, secrets, os, datetime, tempfile, threading, time

USERS_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")
_users_lock = threading.Lock()

ROLES = {
    "admin":    {"label": "Адмін",    "color": "#f05050"},
    "operator": {"label": "Оператор", "color": "#f5a623"},
    "medic":    {"label": "Медик",    "color": "#3ec97a"},
    "reader":   {"label": "Читач",    "color": "#7a8490"},
}

PERMISSIONS = {
    "admin":    ["view", "add_personnel", "edit_personnel", "delete_personnel",
                 "add_szc", "edit_szc", "delete_szc", "add_medical",
                 "edit_medical", "generate_pdf", "manage_users"],
    "operator": ["view", "add_personnel", "edit_personnel",
                 "add_szc", "edit_szc", "generate_pdf"],
    "medic":    ["view", "add_medical", "edit_medical", "generate_pdf"],
    "reader":   ["view", "generate_pdf"],
}


def _hash(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"),
        salt.encode("utf-8"), 600_000
    ).hex()


def load_users() -> dict:
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_users(users: dict):
    """[A3] Atomic write via tmp + os.replace."""
    dir_ = os.path.dirname(USERS_FILE)
    fd, tmp_path = tempfile.mkstemp(dir=dir_, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, USERS_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def create_user(username: str, password: str, role: str,
                display_name: str = "", created_by: str = "system") -> dict:
    with _users_lock:
        users = load_users()
        if username in users:
            raise ValueError(f"Користувач '{username}' вже існує")
        if role not in ROLES:
            raise ValueError(f"Невідома роль: {role}")
        salt = secrets.token_hex(32)
        users[username] = {
            "display_name": display_name or username,
            "role":         role,
            "salt":         salt,
            "hash":         _hash(password, salt),
            "created_by":   created_by,
            "created_at":   datetime.datetime.now().isoformat(),
            "last_login":   None,
            "active":       True,
        }
        save_users(users)
        return dict(users[username])


def verify_user(username: str, password: str):
    """[A1] Full read-modify-write under lock. [A4] compare_digest. [A5] dummy hash."""
    with _users_lock:
        users = load_users()
        u = users.get(username)
        if not u or not u.get("active"):
            # [A5] Timing parity for non-existent users
            _hash(password, "0" * 64)
            return None
        expected = u["hash"]
        actual   = _hash(password, u["salt"])
        # [A4] Constant-time comparison
        if not hmac.compare_digest(actual, expected):
            return None
        # [A1] update last_login inside the same lock scope
        u["last_login"] = datetime.datetime.now().isoformat()
        save_users(users)
        return dict(u)


def change_password(username: str, new_password: str):
    with _users_lock:
        users = load_users()
        if username not in users:
            raise ValueError("Користувача не знайдено")
        salt = secrets.token_hex(32)
        users[username]["salt"] = salt
        users[username]["hash"] = _hash(new_password, salt)
        save_users(users)


def has_permission(role: str, permission: str) -> bool:
    return permission in PERMISSIONS.get(role, [])


def init_default_users():
    """Creates default users only if users.json does not exist."""
    if os.path.exists(USERS_FILE):
        return
    print("  [AUTH] Створення початкових користувачів...")
    create_user("admin",    "admin1234",  "admin",    "Адміністратор")
    create_user("operator", "oper1234",   "operator", "Оператор")
    create_user("medic",    "medic1234",  "medic",    "Медична служба")
    create_user("reader",   "reader1234", "reader",   "Читач")
    print("  [AUTH] users.json створено. ЗМІНІТЬ ПАРОЛІ!")


def toggle_user_active(username: str, requesting_user: str) -> bool:
    """[Fix #N2] Toggle user active state under _users_lock. Returns new active state."""
    with _users_lock:
        users = load_users()
        if username not in users:
            raise ValueError(f"Користувача '{username}' не знайдено")
        if username == requesting_user:
            raise ValueError("Не можна деактивувати себе")
        users[username]["active"] = not users[username].get("active", True)
        new_state = users[username]["active"]
        save_users(users)
    return new_state


# ── Сесії (in-memory) ─────────────────────────────────────────────────────────
_sessions: dict = {}   # token -> {username, role, display_name, expires}
_lock     = threading.Lock()
SESSION_TTL = 8 * 3600  # 8 hours


def create_session(username: str, role: str, display_name: str = "") -> str:
    """[A2] display_name passed as param — no disk I/O here."""
    token   = secrets.token_hex(32)
    expires = time.time() + SESSION_TTL
    with _lock:
        # Remove old sessions for this user
        for t, s in list(_sessions.items()):
            if s["username"] == username:
                del _sessions[t]
        _sessions[token] = {
            "username":     username,
            "role":         role,
            "display_name": display_name or username,
            "expires":      expires,
            "issued_at":    datetime.datetime.now().isoformat(),
        }
    return token


def get_session(token: str):
    """Returns a copy to prevent mutation of shared state."""
    with _lock:
        s = _sessions.get(token)
        if not s:
            return None
        if time.time() > s["expires"]:
            del _sessions[token]
            return None
        return dict(s)


def delete_session(token: str):
    with _lock:
        _sessions.pop(token, None)


def _session_cleanup():
    """[A6] Periodic cleanup of expired sessions."""
    while True:
        time.sleep(3600)
        now = time.time()
        with _lock:
            expired = [t for t, s in _sessions.items() if now > s["expires"]]
            for t in expired:
                del _sessions[t]
        if expired:
            print(f"  [AUTH] Cleaned {len(expired)} expired sessions")


# Start background cleanup thread
threading.Thread(target=_session_cleanup, daemon=True, name="session-gc").start()
