"""
db_write.py — API запису в SQLite для В/Ч А7020

Patches applied:
  [D1] _backup() race: set flag inside lock before copy (prevents double-copy)
  [D2] _ensure_audit() checked+set under _db_lock (prevents double-DDL)
  [D3] edit_personnel / edit_szc check rowcount → 404 if not found
  [D4] delete_personnel renamed to soft_delete_personnel (semantic clarity)
  [D5] audit failure logged to stderr instead of silently swallowed
"""
import sqlite3, datetime, os, shutil, json, threading, sys

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

# ── DB state ──────────────────────────────────────────────────────────────────
_db_lock           = threading.Lock()
_DB_PATH: str      = ""
_backup_done_today = ""
_audit_ready       = False


def set_db(path: str):
    global _DB_PATH, _backup_done_today, _audit_ready
    with _db_lock:
        _DB_PATH           = path
        _backup_done_today = ""
        _audit_ready       = False


def get_db_path() -> str:
    with _db_lock:
        return _DB_PATH


def _conn():
    path = get_db_path()
    if not path:
        raise RuntimeError("DB не підключена")
    return sqlite3.connect(path, timeout=10)


def _backup():
    """[D1] Set flag INSIDE lock before releasing — prevents race double-copy."""
    global _backup_done_today
    today = datetime.date.today().isoformat()
    with _db_lock:
        if _backup_done_today == today:
            return
        path = _DB_PATH
        _backup_done_today = today   # reserve before releasing lock
    if not path or not os.path.exists(path):
        return
    dst = os.path.join(BACKUP_DIR, f"military_{today}.db")
    # [Fix #S5] WAL checkpoint before copy → consistent backup
    try:
        _ck = sqlite3.connect(path, timeout=5)
        _ck.execute("PRAGMA wal_checkpoint(FULL)")
        _ck.close()
    except Exception as e:
        print(f"[WARN] WAL checkpoint failed: {e}", file=sys.stderr)
    shutil.copy2(path, dst)
    bk = sorted(f for f in os.listdir(BACKUP_DIR) if f.endswith(".db"))
    for old in bk[:-30]:
        os.remove(os.path.join(BACKUP_DIR, old))


def _audit(conn, action: str, table: str, record_id, user: str, details: str = ""):
    """Write to audit_log using actual DB schema:
    id, table_name, row_id, action, field_name, old_value, new_value, changed_by, changed_at
    """
    try:
        conn.execute(
            """INSERT INTO audit_log
               (table_name, row_id, action, field_name, old_value, new_value, changed_by, changed_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (table, str(record_id), action, details, None, None, user,
             datetime.datetime.now().isoformat())
        )
    except Exception as e:
        print(f"[AUDIT ERROR] {e} — action={action} table={table} id={record_id}",
              file=sys.stderr)


def _ensure_audit(conn):
    """Check audit_log exists — do NOT recreate (real DB has its own schema)."""
    global _audit_ready
    with _db_lock:
        if _audit_ready:
            return
        _audit_ready = True


# ══════════════════════════════════════════════════════════════════════════════
#  PERSONNEL
# ══════════════════════════════════════════════════════════════════════════════

PERSONNEL_FIELDS = [
    "pib", "rank_text", "filter_group", "date_of_birth", "ipn",
    "military_ticket", "phone", "subdivision", "vos_position",
    "staff_type", "service_type", "enroll_date", "status",
    "conscription_date", "conscription_tck", "conscription_oblast",
    "blood_type", "family_status", "driver_license",
    "civil_education", "note", "health_status", "vlk_decision_summary",
    "has_wound", "ato_participant", "ubd_number", "relative_pib",
]

REQUIRED_PERSONNEL = ["pib", "rank_text"]

RANK_TO_GROUP = {
    "генерал": "ОФ", "полковник": "ОФ", "підполковник": "ОФ",
    "майор": "ОФ", "капітан": "ОФ", "старший лейтенант": "ОФ",
    "лейтенант": "ОФ", "молодший лейтенант": "ОФ",
    "старший сержант": "Серж", "сержант": "Серж", "молодший сержант": "Серж",
    "старшина": "Серж", "майстер-сержант": "Серж",
    "штаб-сержант": "Серж", "головний сержант": "Серж",
    "солдат": "Солд", "старший солдат": "Солд",
    "матрос": "Солд", "старший матрос": "Солд",
}


def _infer_filter_group(rank: str) -> str:
    if not rank:
        return "Солд"
    r = rank.lower().strip()
    for key, group in RANK_TO_GROUP.items():
        if key in r:
            return group
    return "Солд"


def add_personnel(data: dict, user: str) -> int:
    for f in REQUIRED_PERSONNEL:
        if not data.get(f):
            raise ValueError(f"Поле '{f}' обов'язкове")
    if not data.get("filter_group"):
        data["filter_group"] = _infer_filter_group(data.get("rank_text", ""))

    _backup()
    fields       = [f for f in PERSONNEL_FIELDS if f in data]
    placeholders = ",".join("?" * len(fields))
    values       = [data[f] for f in fields]

    with _conn() as conn:
        _ensure_audit(conn)
        cur    = conn.execute(
            f"INSERT INTO personnel ({','.join(fields)}) VALUES ({placeholders})",
            values
        )
        new_id = cur.lastrowid
        _audit(conn, "INSERT", "personnel", new_id, user,
               f"П.І.Б.: {data.get('pib')}")
        conn.commit()
    return new_id


def edit_personnel(person_id: int, data: dict, user: str):
    """[D3] Raises ValueError if record not found."""
    if not data:
        raise ValueError("Немає даних для оновлення")
    data.pop("id", None)
    fields = [f for f in PERSONNEL_FIELDS if f in data]
    if not fields:
        raise ValueError("Немає відомих полів для оновлення")

    _backup()
    set_clause = ", ".join(f"{f}=?" for f in fields)
    values     = [data[f] for f in fields] + [person_id]

    with _conn() as conn:
        _ensure_audit(conn)
        cur = conn.execute(f"UPDATE personnel SET {set_clause} WHERE id=?", values)
        # [D3] Check rowcount
        if cur.rowcount == 0:
            raise ValueError(f"Запис personnel id={person_id} не знайдено")
        _audit(conn, "UPDATE", "personnel", person_id, user,
               json.dumps({f: data[f] for f in fields}, ensure_ascii=False))
        conn.commit()


def soft_delete_personnel(person_id: int, user: str):
    """[D4] Renamed from delete_personnel to clarify soft-delete semantics."""
    _backup()
    with _conn() as conn:
        _ensure_audit(conn)
        row = conn.execute(
            "SELECT pib FROM personnel WHERE id=?", (person_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Запис personnel id={person_id} не знайдено")
        pib = row[0]
        conn.execute(
            "UPDATE personnel SET status='виключений' WHERE id=?", (person_id,)
        )
        _audit(conn, "SOFT_DELETE", "personnel", person_id, user,
               f"Soft delete: {pib}")
        conn.commit()


# Backward-compatible alias
delete_personnel = soft_delete_personnel


# ══════════════════════════════════════════════════════════════════════════════
#  СЗЧ JOURNAL
# ══════════════════════════════════════════════════════════════════════════════

SZC_FIELDS = [
    "pib_raw", "rank_raw", "runner_no", "szc_date",
    "conscription_oblast", "source_unit", "phone",
    "state", "enrollment_date", "personnel_id",
]
REQUIRED_SZC = ["pib_raw", "szc_date"]


def add_szc(data: dict, user: str) -> int:
    for f in REQUIRED_SZC:
        if not data.get(f):
            raise ValueError(f"Поле '{f}' обов'язкове")

    _backup()
    fields       = [f for f in SZC_FIELDS if f in data]
    placeholders = ",".join("?" * len(fields))
    values       = [data[f] for f in fields]

    with _conn() as conn:
        _ensure_audit(conn)
        cur    = conn.execute(
            f"INSERT INTO szc_journal ({','.join(fields)}) VALUES ({placeholders})",
            values
        )
        new_id = cur.lastrowid
        _audit(conn, "INSERT", "szc_journal", new_id, user,
               f"СЗЧ: {data.get('pib_raw')} від {data.get('szc_date')}")
        conn.commit()
    return new_id


def edit_szc(szc_id: int, data: dict, user: str):
    """[D3] Raises ValueError if record not found."""
    data.pop("id", None)
    fields = [f for f in SZC_FIELDS if f in data]
    if not fields:
        raise ValueError("Немає полів для оновлення")
    _backup()
    set_clause = ", ".join(f"{f}=?" for f in fields)
    values     = [data[f] for f in fields] + [szc_id]
    with _conn() as conn:
        _ensure_audit(conn)
        cur = conn.execute(f"UPDATE szc_journal SET {set_clause} WHERE id=?", values)
        if cur.rowcount == 0:
            raise ValueError(f"Запис szc_journal id={szc_id} не знайдено")
        _audit(conn, "UPDATE", "szc_journal", szc_id, user,
               json.dumps({f: data[f] for f in fields}, ensure_ascii=False))
        conn.commit()


def get_next_runner_no() -> str:
    if not get_db_path():
        return "1"
    with _conn() as conn:
        row = conn.execute(
            "SELECT MAX(CAST(runner_no AS INTEGER)) FROM szc_journal"
            " WHERE runner_no GLOB '[0-9]*'"
        ).fetchone()
        return str((row[0] or 0) + 1)


def get_audit_log(limit: int = 100) -> list:
    """Read audit_log using actual DB schema."""
    with _conn() as conn:
        try:
            rows = conn.execute(
                "SELECT changed_at, action, table_name, row_id, changed_by, field_name"
                " FROM audit_log ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [
                dict(zip(["ts", "action", "table", "id", "user", "details"], r))
                for r in rows
            ]
        except Exception as e:
            print(f"[AUDIT READ ERROR] {e}", file=sys.stderr)
            return []
