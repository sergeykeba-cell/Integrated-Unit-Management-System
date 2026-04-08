"""
profile_manager.py
Управління профілем користувача (SQLite, локально)
"""

import sqlite3
import os
import json
from typing import Optional, Dict

DB_PATH = os.path.join(os.path.dirname(__file__), "profiles", "user_profiles.db")


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                name    TEXT NOT NULL UNIQUE,
                data    TEXT NOT NULL,
                is_active INTEGER DEFAULT 0
            )
        """)
        conn.commit()


def save_profile(profile_name: str, profile_data: Dict) -> bool:
    try:
        with _get_conn() as conn:
            conn.execute("""
                INSERT INTO profiles (name, data) VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET data = excluded.data
            """, (profile_name, json.dumps(profile_data, ensure_ascii=False)))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERR] save_profile: {e}")
        return False


def load_profile(profile_name: str) -> Optional[Dict]:
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT data FROM profiles WHERE name = ?", (profile_name,)
            ).fetchone()
            if row:
                return json.loads(row["data"])
    except Exception as e:
        print(f"[ERR] load_profile: {e}")
    return None


def list_profiles() -> list:
    try:
        with _get_conn() as conn:
            rows = conn.execute("SELECT name FROM profiles ORDER BY name").fetchall()
            return [r["name"] for r in rows]
    except Exception:
        return []


def delete_profile(profile_name: str) -> bool:
    try:
        with _get_conn() as conn:
            conn.execute("DELETE FROM profiles WHERE name = ?", (profile_name,))
            conn.commit()
        return True
    except Exception:
        return False


def get_active_profile() -> Optional[Dict]:
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT data FROM profiles WHERE is_active = 1 LIMIT 1"
            ).fetchone()
            if row:
                return json.loads(row["data"])
    except Exception:
        pass
    return None


def set_active_profile(profile_name: str) -> bool:
    try:
        with _get_conn() as conn:
            conn.execute("UPDATE profiles SET is_active = 0")
            conn.execute(
                "UPDATE profiles SET is_active = 1 WHERE name = ?", (profile_name,)
            )
            conn.commit()
        return True
    except Exception:
        return False


# Структура профілю користувача
DEFAULT_PROFILE = {
    "rank": "",           # Військове звання (напр. "старший лейтенант")
    "position": "",       # Посада
    "name_full": "",      # ПІБ повністю (Іваненко Іван Іванович)
    "name_short": "",     # ПІБ скорочено (І.І. Іваненко)
    "unit": "",           # Номер частини (А1234)
    "location": "",       # Місце дислокації (м. Дніпро)
}


init_db()
