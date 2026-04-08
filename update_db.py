#!/usr/bin/env python3
"""
update_db.py — Оновлення military.db з Excel-файлу  В/Ч А7020
Використання:
  python update_db.py --excel FILE.xlsx --db military.db
  python update_db.py --excel FILE.xlsx --db military.db --dry-run
  python update_db.py --excel FILE.xlsx --db military.db --yes
"""
import sqlite3, re, sys, shutil, datetime, argparse
import pandas as pd
from pathlib import Path

# ── Нормалізатори ─────────────────────────────────────────────────────────────

def s(v):
    if v is None: return None
    try:
        if pd.isna(v): return None
    except Exception:
        pass
    r = str(v).strip()
    return None if r in ('', 'nan', 'NaN', 'None') else r

def norm_pib(v):
    r = s(v)
    if not r: return None
    return re.sub(r' +', ' ', r)

def parse_date(v):
    r = s(v)
    if not r: return None
    # Handle Excel serial number (days since 1899-12-30)
    try:
        n = int(float(r))
        if 10000 < n < 60000:  # plausible date range 1927-2064
            dt = datetime.date(1899, 12, 30) + datetime.timedelta(days=n)
            if 1940 <= dt.year <= 2030:
                return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y'):
        try:
            return datetime.datetime.strptime(r[:10], fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    try:
        ts = pd.Timestamp(v)
        if ts.year == 1970 and ts.month == 1 and ts.day == 1:
            return None  # epoch = no date
        return ts.strftime('%Y-%m-%d')
    except Exception:
        return None

def i(v):
    r = s(v)
    if not r: return None
    try: return int(float(r))
    except Exception: return None

def bool_col(v):
    r = s(v)
    if not r: return 0
    return 1 if r.lower() in ('так', 'yes', 'true', '1', '+') else 0

def norm_filter(v):
    r = s(v)
    if not r: return None
    vl = r.lower()
    if vl in ('оф', 'офіцер', 'of'): return 'ОФ'
    if vl in ('серж', 'сержант'):     return 'Серж'
    if vl in ('солд', 'солдат'):      return 'Солд'
    return r

def norm_service(v):
    r = s(v)
    if not r: return None
    vl = r.lower()
    if 'мобіл' in vl:    return 'мобілізація'
    if 'контракт' in vl: return 'контракт'
    return r

def norm_family(v):
    r = s(v)
    if not r: return None
    vl = r.lower()
    if any(x in vl for x in ('одружен', 'заміжн')):
        return 'не одружений' if ('не' in vl or 'без' in vl) else 'одружений'
    if 'розлуч' in vl: return 'розлучений'
    if 'цивіл' in vl or 'громад' in vl: return 'цивільний шлюб'
    if 'вдов' in vl: return 'вдівець'
    return r

def norm_blood(v):
    r = s(v)
    if not r: return None
    valid = {'1+','1-','2+','2-','3+','3-','4+','4-'}
    clean = re.sub(r'[^1-4\+\-]', '', r)
    if clean in valid: return clean
    m = re.match(r'^([1-4])', r)
    return m.group(1) if m else r[:5]

# ── Автовизначення колонок ────────────────────────────────────────────────────

PIB_VARIANTS = [
    '\u041f.\u0406.\u0411.',   # П.І.Б.
    '\u041f\u0406\u0411',      # ПІБ
    'PIB', 'pib',
    '\u041f\u0406\u0411 \u043e\u0441\u043e\u0431\u0438',   # ПІБ особи
]
IPN_VARIANTS = [
    '\u0406\u041f\u041d',      # ІПН
    'IPN',
    '\u0420\u041d\u041e\u041a\u041f\u041f',  # РНОКПП
    'Unnamed: 33',
]

def find_col(df, variants):
    cols = list(df.columns)
    cols_lower = {c.strip().lower(): c for c in cols}
    for v in variants:
        if v in cols: return v
        if v.lower() in cols_lower: return cols_lower[v.lower()]
    return None

def get_col(row, variants):
    """Get value from row trying multiple column name variants."""
    for v in variants:
        val = s(row.get(v))
        if val:
            return val
    return None

# ── Вивід ─────────────────────────────────────────────────────────────────────

def clr(t, c): return f'\033[{c}m{t}\033[0m'
def green(t):  return clr(t, '32')
def yellow(t): return clr(t, '33')
def red(t):    return clr(t, '31')
def bold(t):   return clr(t, '1')
def dim(t):    return clr(t, '2')
def cyan(t):   return clr(t, '36')

# ── Завантаження Excel ────────────────────────────────────────────────────────

SHEET_TARGETS = {
    '\u041f\u0415\u0420\u0421\u041e\u041d\u0410\u041b': None,  # ПЕРСОНАЛ
    '\u0421\u0417\u0427': None,                                  # СЗЧ
    'TRANZIT': None,
}

def load_excel(path):
    print('  Завантаження Excel...')
    try:
        _test = open(str(path), 'rb'); _test.close()
    except Exception as e:
        print(red(f'[!] Не вдається відкрити файл: {path}\n    {e}')); sys.exit(1)
    with pd.ExcelFile(str(path)) as xf:
        sheets = xf.sheet_names
        print(f'  Аркуші: {dim(", ".join(sheets))}')
        sheet_map = {sn.upper(): sn for sn in sheets}
        targets = dict(SHEET_TARGETS)
        for canon in list(targets.keys()):
            if canon in sheet_map:
                targets[canon] = sheet_map[canon]
            else:
                for var in (canon, canon.capitalize(), canon.lower()):
                    if var in sheet_map:
                        targets[canon] = sheet_map[var]
                        break
        missing = [k for k, v in targets.items() if v is None]
        if missing:
            print(yellow(f'  ⚠ Аркуші не знайдено: {", ".join(missing)}'))
        xl = {}
        for canon, actual in targets.items():
            if actual:
                xl[canon] = xf.parse(actual, dtype=str)
    return xl

# ── Аналіз нових записів ─────────────────────────────────────────────────────

def analyze_personnel(df, conn):
    pib_col = find_col(df, PIB_VARIANTS)
    ipn_col = find_col(df, IPN_VARIANTS)

    if not pib_col:
        print(yellow('    ⚠ Колонка ПІБ не знайдена!'))
        print(f'    Колонки: {list(df.columns[:12])}')
        return [], [], []

    print(f"    Колонка ПІБ: '{pib_col}'" + (f", ІПН: '{ipn_col}'" if ipn_col else ''))

    # Завантажуємо існуючі з БД (нормалізовані)
    existing_pibs = set()
    existing_ipns = {}  # ipn -> pib (для конфліктів)
    for row in conn.execute('SELECT pib, ipn FROM personnel WHERE pib IS NOT NULL'):
        pib_n = norm_pib(row[0])
        if pib_n:
            existing_pibs.add(pib_n)
        if row[1]:
            existing_ipns[row[1].strip()] = pib_n

    new_rows, dup_pib, dup_ipn = [], [], []
    seen_pib = set()
    seen_ipn = set()

    for _, row in df.iterrows():
        pib = norm_pib(row.get(pib_col))
        if not pib:
            continue
        ipn = s(row.get(ipn_col)) if ipn_col else None
        if ipn:
            ipn = ipn.strip()

        pib_match = pib in existing_pibs or pib in seen_pib
        ipn_match = bool(ipn and (ipn in existing_ipns or ipn in seen_ipn))

        if pib_match:
            dup_pib.append(pib)
        elif ipn_match and ipn in existing_ipns:
            # ІПН в БД але ПІБ інший — різні люди, додаємо
            new_rows.append(row)
            seen_pib.add(pib)
        elif ipn_match and ipn in seen_ipn:
            # дублікат всередині Excel по ІПН
            dup_ipn.append((pib, ipn))
        else:
            new_rows.append(row)
            seen_pib.add(pib)
            if ipn:
                seen_ipn.add(ipn)

    return new_rows, dup_pib, dup_ipn


def analyze_szc(df, conn):
    existing = set()
    for r in conn.execute('SELECT pib_raw, szc_date FROM szc_journal WHERE pib_raw IS NOT NULL'):
        existing.add((r[0], str(r[1]) if r[1] else None))
    new_rows, dups = [], []
    pib_col = find_col(df, PIB_VARIANTS) or '\u041f.\u0406.\u0411.'
    for _, row in df.iterrows():
        pib = s(row.get(pib_col))
        if not pib:
            continue
        dt = parse_date(row.get('\u0414\u0430\u0442\u0430 \u0437\u0434\u0456\u0439\u0441\u043d\u0435\u043d\u043d\u044f \u0421\u0417\u0427'))  # Дата здійснення СЗЧ
        key = (pib, dt)
        if key in existing:
            dups.append(pib)
        else:
            new_rows.append(row)
    return new_rows, dups


def analyze_transit(df, conn):
    existing = set(
        r[0] for r in conn.execute('SELECT pib_raw FROM transit_journal WHERE pib_raw IS NOT NULL')
    )
    new_rows, dups = [], []
    pib_col = find_col(df, PIB_VARIANTS) or '\u041f.\u0406.\u0411.'
    for _, row in df.iterrows():
        pib = s(row.get(pib_col))
        if not pib:
            continue
        if pib in existing:
            dups.append(pib)
        else:
            new_rows.append(row)
    return new_rows, dups

# ── Вставка ───────────────────────────────────────────────────────────────────

def insert_personnel(rows, conn):
    if not rows:
        return 0
    pib_col = find_col(rows[0].to_frame().T, PIB_VARIANTS) or '\u041f.\u0406.\u0411.'
    ipn_col = find_col(rows[0].to_frame().T, IPN_VARIANTS)

    data = []
    for row in rows:
        pib = norm_pib(row.get(pib_col))
        if not pib:
            continue
        ipn = s(row.get(ipn_col)) if ipn_col else (s(row.get('Unnamed: 33')))
        data.append((
            pib,
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),          # Дата народження
            ipn,
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u043d\u043e\u043c\u0435\u0440 \u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u043e\u0433\u043e \u043a\u0432\u0438\u0442\u043a\u0430')),  # серія номер військового квитка
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0435 \u0437\u0432\u0430\u043d\u043d\u044f')),              # військове звання
            norm_filter(row.get('\u0424\u0406\u041b\u042c\u0422\u0420') or row.get('\u041e\u0424')),                                # ФІЛЬТР або ОФ
            s(row.get('\u0412\u041e\u0421 (\u043a\u043e\u0434)')),     # ВОС (код)
            s(row.get('\u0413\u0440\u0443\u043f\u0430 \u0412\u041e\u0421')),  # Група ВОС
            s(row.get('\u0421\u0422\u0410\u0422\u0423\u0421')),        # СТАТУС
            s(row.get('\u0420\u041e\u0422\u0410')),                    # РОТА
            s(row.get('\u0421\u041a\u041b\u0410\u0414')),              # СКЛАД
            s(row.get('\u0410\u041b\u0424\u0410\u0412\u0406\u0422')),  # АЛФАВІТ
            s(row.get('\u043c\u0456\u0441\u0446\u0435 \u043a\u0443\u0434\u0438 \u0432\u0438\u0431\u0443\u0432')),
            s(row.get('\u041f\u0440\u0438\u0447\u0438\u043d\u0430 \u043f\u0435\u0440\u0435\u043c\u0456\u0449\u0435\u043d\u043d\u044f')),
            s(row.get('\u041d\u0430\u043a\u0430\u0437 \u041e\u0421')),
            s(row.get('\u043d\u0430\u043a\u0430\u0437 \u0421/\u0427')),
            s(row.get('\u043c\u0456\u0441\u044f\u0446\u044c \u0440\u0435\u0430\u043b\u0456\u0437\u0430\u0446\u0456\u0457')),
            i(row.get('\u0440\u0456\u043a \u0440\u0435\u0430\u043b\u0456\u0437\u0430\u0446\u0456\u0457')),
            norm_service(row.get('\u0432\u0438\u0434 \u0441\u043b\u0443\u0436\u0431\u0438')),
            parse_date(row.get('\u0434\u0430\u0442\u0430 \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
            s(row.get('\u042f\u043a\u0438\u043c \u0422\u0426\u041a \u043f\u0440\u0438\u0437\u0432\u0430\u043d\u043e')),
            s(row.get('\u043e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043f\u043e\u0456\u043c\u0435\u043d\u043d\u043e\u0433\u043e \u0441\u043f\u0438\u0441\u043a\u0443')),
            s(row.get('\u041d\u043e\u043c\u0435\u0440 \u0441\u043f\u0438\u0441\u043a\u0443')),
            s(row.get('\u041c\u0456\u0441\u0442\u043e \u0432 \u043f\u043e\u0456\u043c\u0435\u043d\u043d\u043e\u043c\u0443 \u0441\u043f\u0438\u0441\u043a\u0443')),
            s(row.get('\u0441\u043b\u0443\u0436\u0438\u0432 \u0434\u043e \u0432\u0456\u0439\u043d\u0438?')),
            s(row.get('\u043a\u0430\u043b\u0435\u043d\u0434\u0430\u0440\u043d\u0430 \u0432\u0438\u0441\u043b\u0443\u0433\u0430 \u0440\u043e\u043a\u0456\u0432')),
            parse_date(row.get('\u041f\u0440\u0438\u0441\u044f\u0433\u0430')),
            parse_date(row.get('\u0434\u0430\u0442\u0430 \u0437\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043d\u043d\u044f \u0434\u043e \u043e/\u0441 \u0432/\u0447')),
            s(row.get('\u0434\u0430\u0442\u0430 \u0442\u0430 \u043d\u043e\u043c\u0435\u0440 \u043d\u0430\u043a\u0430\u0437\u0443')),
            s(row.get('\u043f\u0440\u0438\u0439\u0448\u043e\u0432 \u0437')),
            s(row.get('\u041d\u0430\u043a\u0430\u0437 (\u043f\u0440\u0438\u0445\u043e\u0434)')),
            s(row.get('\u0446\u0438\u0432\u0456\u043b\u044c\u043d\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u0432\u0447\u0435\u043d\u0430 \u0441\u0442\u0443\u043f\u0456\u043d\u044c')),
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            norm_family(row.get('\u0441\u0456\u043c\u0435\u0439\u043d\u0438\u0439 \u0441\u0442\u0430\u0442\u0443\u0441')),
            s(row.get('\u043c\u0456\u0441\u0446\u0435 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),
            s(row.get('\u0444\u0430\u043a\u0442\u0438\u0447\u043d\u0435 \u043c\u0456\u0441\u0446\u0435 \u043f\u0440\u043e\u0436\u0438\u0432\u0430\u043d\u043d\u044f')),
            s(row.get('\u043d\u043e\u043c\u0435\u0440 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0443')),
            norm_blood(row.get('\u0433\u0440\u0443\u043f\u0430 \u043a\u0440\u043e\u0432\u0456')),
            s(row.get('\u0432\u043e\u0434\u0456\u0439\u0441\u044c\u043a\u0435 \u043f\u043e\u0441\u0432\u0456\u0434\u0447\u0435\u043d\u043d\u044f')),
            bool_col(row.get('\u0423\u0447\u0430\u0441\u043d\u0438\u043a \u0410\u0422\u041e/\u041e\u041e\u0421 (40)')),
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u0442\u0430 \u043d\u043e\u043c\u0435\u0440 \u0423\u0411\u0414')),
            s(row.get('\u043f\u0435\u0440\u0456\u043e\u0434\u0438 \u0411\u0414 (\u0410\u0422\u041e/\u041e\u041e\u0421)')),
            s(row.get("\u0421\u0442\u0430\u043d \u0437\u0434\u043e\u0440\u043e\u0432'\u044f")),
            s(row.get('\u0420\u0406\u0428\u0415\u041d\u041d\u042f \u0412\u041b\u041a')),
            bool_col(row.get('\u043c\u0430\u0454 \u043f\u043e\u0440\u0430\u043d\u0435\u043d\u043d\u044f?')),
            bool_col(row.get('\u043e\u0442\u043a\u0430\u0437\u043d\u0438\u043a?')),
            s(row.get('\u041f\u0440\u0438\u043c\u0456\u0442\u043a\u0430')),
            s(row.get('\u0445\u043e\u0432\u0430\u043d\u043a\u0430')),
            s(row.get('\u0441\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u0438\u0439 \u0434\u043e')),
            s(row.get('\u0441\u0442\u0430\u0442\u0443\u0441 \u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442\u0456\u0432')),
            s(row.get('\u043a\u0443\u0440\u0441 \u043f\u0456\u0434\u0433\u043e\u0442\u043e\u0432\u043a\u0438 ')),
            s(row.get('\u0432\u0456\u0434\u0431\u0456\u0440 \u043f\u043e \u0444\u0430\u043a\u0442\u0443 \u0432 \u0456\u043d\u0448\u0456 \u0447\u0430\u0441\u0442\u0438\u043d\u0438 (\u0432\\ч, \u0434\u0430\u0442\u0430)')),
            s(row.get('\u0410\u0442\u0435\u0441\u0442\u0430\u0442\u0438:')),
            s(row.get('\u0413\u0414\u041e_2023')),
            s(row.get('\u041c\u0424\u0414_2023')),
            s(row.get('\u0432/\u0437\u0432 \u0440\u043e\u0434-\u0432\u0456\u0434')),
            s(row.get('\u041f\u0406\u0411 \u0440\u043e\u0434-\u0432\u0456\u0434 \n(41)')),
            s(row.get('\u043f\u043e\u0441\u0430\u0434\u0430')),
        ))

    conn.executemany("""
        INSERT INTO personnel (
            pib, date_of_birth, ipn, military_ticket,
            rank_text, filter_group, vos_code, vos_position,
            status, subdivision, staff_type, alphabet_letter,
            location_note, movement_reason, os_order, sch_order, move_month, move_year,
            service_type, conscription_date, conscription_tck, conscription_oblast,
            conscription_list_date, conscription_list_no, conscription_city,
            served_before_war, calendar_seniority, oath_date, enroll_date, enroll_order,
            incoming_from, incoming_order,
            civil_education, academic_degree, military_education,
            family_status, birth_place, residence, phone, blood_type, driver_license,
            ato_participant, ubd_number, ato_periods,
            health_status, vlk_decision_summary, has_wound,
            is_refuser, note, hidden_note,
            planned_to, doc_status, training_course, selection_note,
            attestat_status, course_gdo_2023, course_mfd_2023,
            relative_rank_rod, relative_pib, position_rod
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, data)
    conn.commit()
    return len(data)


def insert_szc(rows, conn, pib_to_id):
    if not rows: return 0
    pib_col = find_col(rows[0].to_frame().T, PIB_VARIANTS) or '\u041f.\u0406.\u0411.'
    data = []
    for row in rows:
        pib = s(row.get(pib_col))
        if not pib: continue
        pid = pib_to_id.get(norm_pib(pib))
        data.append((
            pid, pib,
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0435 \u0437\u0432\u0430\u043d\u043d\u044f')),
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u043d\u043e\u043c\u0435\u0440 \u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u043e\u0433\u043e \u043a\u0432\u0438\u0442\u043a\u0430')),
            s(row.get('\u0406\u041f\u041d')),
            s(row.get('\u0412\u041e\u0421 (\u043a\u043e\u0434)')),
            s(row.get('\u0412\u041e\u0421 (\u043f\u043e\u0441\u0430\u0434\u0430)')),
            None,  # runner_no
            s(row.get('\u0425\u0442\u043e \u043f\u0440\u0438\u0439\u043c\u0430\u0432')),
            s(row.get('\u041e\u0431\u043b\u0430\u0441\u0442\u044c  \u0432\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u043d\u044f \u0444\u0430\u043a\u0442\u0443 \u0421\u0417\u0427 (\u0434\u043b\u044f \u0412\u0421\u041f)')),
            s(row.get('\u041f\u0440\u0438\u0439\u0448\u043e\u0432 \u0437')),
            s(row.get('\u0412 \u044f\u043a\u0443 \u0447\u0430\u0441\u0442\u0438\u043d\u0443 \u0431\u0430\u0436\u0430\u0454 \u0432/c')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u0437\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043d\u043d\u044f \u0443 \u0412/\u0427 \u0410\u0437020')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u0437\u0434\u0456\u0439\u0441\u043d\u0435\u043d\u043d\u044f \u0421\u0417\u0427')),
            norm_service(row.get('\u0412\u0438\u0434 \u0441\u043b\u0443\u0436\u0431\u0438')),
            s(row.get('\u041f\u0456\u0434\u0441\u0442\u0430\u0432\u0438 \u0434\u043b\u044f \u0437\u0432\u0456\u043b\u044c\u043d\u0435\u043d\u043d\u044f')),
            s(row.get('\u0427\u0438\u043c \u0437\u0430\u0439\u043c\u0430\u0432\u0441\u044f \u0443 \u0446\u0438\u0432\u0456\u043b\u044c\u043d\u043e\u043c\u0443 \u0436\u0438\u0442\u0442\u0456')),
            s(row.get('\u0420\u043e\u0437\u043c\u0456\u0449\u0435\u043d\u044f \u043e/\u0441')),
            s(row.get('\u041f\u0440\u0438\u043c\u0456\u0442\u043a\u0430')),
            s(row.get('\u0441\u043b\u0443\u0436\u0438\u0432 \u0434\u043e \u0432\u0456\u0439\u043d\u0438?')),
            s(row.get('\u0446\u0438\u0432\u0456\u043b\u044c\u043d\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u043f\u0435\u0440\u0456\u043e\u0434\u0438 \u0441\u043b\u0443\u0436\u0431\u0438')),
            norm_family(row.get('\u0421\u0456\u043c\u0435\u0439\u043d\u0438\u0439 \u0441\u0442\u0430\u0442\u0443\u0441')),
            s(row.get('\u041c\u0456\u0441\u0446\u0435 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),
            s(row.get('\u0424\u0430\u043a\u0442\u0438\u0447\u043d\u0435 \u043c\u0456\u0441\u0446\u0435 \u043f\u0440\u043e\u0436\u0438\u0432\u0430\u043d\u043d\u044f')),
            s(row.get('\u041d\u043e\u043c\u0435\u0440 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0443')),
            s(row.get('\u0432\u043e\u0434\u0456\u0439\u0441\u044c\u043a\u0435 \u043f\u043e\u0441\u0432\u0456\u0434\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u0423\u0447\u0430\u0441\u043d\u0438\u043a \u0410\u0422\u041e/\u041e\u041e\u0421 (40)')),
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u0442\u0430 \u043d\u043e\u043c\u0435\u0440 \u0423\u0411\u0414')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
            s(row.get('\u042f\u043a\u0438\u043c \u0422\u0426\u041a \u043f\u0440\u0438\u0437\u0432\u0430\u043d\u043e\n(\u0432 \u043e\u0440\u0443\u0434\u043d\u043e\u043c\u0443 \u0432\u0456\u0434\u043c\u0456\u043d\u043a\u0443)')),
            s(row.get('\u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
        ))
    conn.executemany("""
        INSERT INTO szc_journal (
            personnel_id, pib_raw, date_of_birth_raw, rank_raw,
            military_ticket_raw, ipn_raw, vos_code_raw, vos_position_raw,
            runner_no, who_accepted, oblast_szc, source_unit, target_unit,
            enrollment_date, szc_date, service_type, release_grounds,
            civilian_occupation, accommodation, state,
            served_before_war, civil_education, military_education,
            service_periods, family_status, birth_place, residence, phone,
            driver_license, ato_participant, ubd_number,
            conscription_date, conscription_tck, conscription_oblast
        ) VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?,?, ?,?,?, ?,?,?)
    """, data)
    conn.commit()
    return len(data)


def insert_transit(rows, conn, pib_to_id):
    if not rows: return 0
    pib_col = find_col(rows[0].to_frame().T, PIB_VARIANTS) or '\u041f.\u0406.\u0411.'
    data = []
    for row in rows:
        pib = s(row.get(pib_col))
        if not pib: continue
        pid = pib_to_id.get(norm_pib(pib))
        data.append((
            pid, pib,
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0435 \u0437\u0432\u0430\u043d\u043d\u044f')),
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u043d\u043e\u043c\u0435\u0440 \u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u043e\u0433\u043e \u043a\u0432\u0438\u0442\u043a\u0430')),
            s(row.get('\u0406\u041f\u041d')),
            s(row.get('\u0412\u041e\u0421 (\u043a\u043e\u0434)')),
            s(row.get('\u0412\u041e\u0421 (\u043f\u043e\u0441\u0430\u0434\u0430)')),
            None,
            s(row.get('\u0425\u0442\u043e \u043f\u0440\u0438\u0439\u043c\u0430\u0432')),
            s(row.get('\u041f\u0440\u0438\u0439\u0448\u043e\u0432 \u0437')),
            s(row.get('\u041a\u041e\u041c\u0410\u041d\u0414\u0423\u0412\u0410\u041d\u041d\u042f')),
            s(row.get('\u0412 \u044f\u043a\u0438\u0439 \u0411\u0420\u0415\u0417 \u043d\u0430\u043f\u0440\u0430\u0432\u043b\u044f\u0454\u0442\u044c\u0441\u044f?')),
            s(row.get('\u0421\u0422\u0410\u0422\u0423\u0421 \u0421\u0417\u0427')),
            s(row.get('\u0421\u0422\u0410\u041d')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u0437\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043d\u043d\u044f \u0443 \u0412/\u0427 \u0410\u0437020')),
            norm_service(row.get('\u0412\u0438\u0434 \u0441\u043b\u0443\u0436\u0431\u0438')),
            s(row.get('\u0441\u043b\u0443\u0436\u0438\u0432 \u0434\u043e \u0432\u0456\u0439\u043d\u0438?')),
            s(row.get('\u0446\u0438\u0432\u0456\u043b\u044c\u043d\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0430 \u043e\u0441\u0432\u0456\u0442\u0430, \u0440\u0456\u043a \u0437\u0430\u043a\u0456\u043d\u0447\u0435\u043d\u043d\u044f')),
            s(row.get('\u043f\u0435\u0440\u0456\u043e\u0434\u0438 \u0441\u043b\u0443\u0436\u0431\u0438')),
            norm_family(row.get('\u0421\u0456\u043c\u0435\u0439\u043d\u0438\u0439 \u0441\u0442\u0430\u0442\u0443\u0441')),
            s(row.get('\u041c\u0456\u0441\u0446\u0435 \u043d\u0430\u0440\u043e\u0434\u0436\u0435\u043d\u043d\u044f')),
            s(row.get('\u0424\u0430\u043a\u0442\u0438\u0447\u043d\u0435 \u043c\u0456\u0441\u0446\u0435 \u043f\u0440\u043e\u0436\u0438\u0432\u0430\u043d\u043d\u044f')),
            s(row.get('\u041d\u043e\u043c\u0435\u0440 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0443')),
            parse_date(row.get('\u0414\u0430\u0442\u0430 \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
            s(row.get('\u042f\u043a\u0438\u043c \u0422\u0426\u041a \u043f\u0440\u0438\u0437\u0432\u0430\u043d\u043e\n(\u0432 \u043e\u0440\u0443\u0434\u043d\u043e\u043c\u0443 \u0432\u0456\u0434\u043c\u0456\u043d\u043a\u0443)')),
            s(row.get('\u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u0440\u0438\u0437\u043e\u0432\u0443')),
            s(row.get('\u0423\u0447\u0430\u0441\u043d\u0438\u043a \u0410\u0422\u041e/\u041e\u041e\u0421 (40)')),
            s(row.get('\u0441\u0435\u0440\u0456\u044f \u0442\u0430 \u043d\u043e\u043c\u0435\u0440 \u0423\u0411\u0414')),
        ))
    conn.executemany("""
        INSERT INTO transit_journal (
            personnel_id, pib_raw, date_of_birth_raw, rank_raw,
            military_ticket_raw, ipn_raw, vos_code_raw, vos_position_raw,
            runner_no, who_accepted, source_unit, command, target_brez,
            szc_status, state, enrollment_date, service_type,
            served_before_war, civil_education, military_education,
            service_periods, family_status, birth_place, residence, phone,
            conscription_date, conscription_tck, conscription_oblast,
            ato_participant, ubd_number
        ) VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?,?, ?,?,?, ?,?)
    """, data)
    conn.commit()
    return len(data)

# ── Головна функція ───────────────────────────────────────────────────────────

def db_count(conn, t):
    try: return conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    except: return 0


def main():
    ap = argparse.ArgumentParser(description='В/Ч А7020 — оновлення БД з Excel')
    ap.add_argument('--excel',   required=True)
    ap.add_argument('--db',      required=True)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--yes',     action='store_true')
    ap.add_argument('--sheets',  nargs='*',
                    choices=['personnel','szc','transit'],
                    default=['personnel','szc','transit'])
    args = ap.parse_args()

    excel_path = Path(args.excel)
    db_path    = Path(args.db)

    if not db_path.exists():
        print(red(f'[!] БД не знайдено: {db_path}')); sys.exit(1)

    print()
    print(bold('═══════════════════════════════════════════════'))
    print(bold('  В/Ч А7020 · Оновлення БД з Excel'))
    print(bold('═══════════════════════════════════════════════'))
    print(f'  Excel: {cyan(str(excel_path))}  ({excel_path.stat().st_size//1024} KB)')
    print(f'  БД:    {cyan(str(db_path))}  ({db_path.stat().st_size//1024} KB)')
    if args.dry_run:
        print(f'  {yellow("DRY-RUN — запис не виконується")}')
    print()

    xl = load_excel(excel_path)
    print()

    conn = sqlite3.connect(str(db_path))

    print(bold('  ┌─ Поточний стан БД ────────────────────────┐'))
    print(f'  │  personnel:       {db_count(conn,"personnel"):>6} записів')
    print(f'  │  szc_journal:     {db_count(conn,"szc_journal"):>6} записів')
    print(f'  │  transit_journal: {db_count(conn,"transit_journal"):>6} записів')
    print(bold('  └───────────────────────────────────────────┘'))
    print()

    # ── Аналіз ───────────────────────────────────────────────────────────────
    PERS = '\u041f\u0415\u0420\u0421\u041e\u041d\u0410\u041b'
    SZC  = '\u0421\u0417\u0427'
    results = {}

    if 'personnel' in args.sheets and PERS in xl:
        print(bold('  [ПЕРСОНАЛ] Аналіз...'))
        df = xl[PERS]
        new_rows, dup_pib, dup_ipn = analyze_personnel(df, conn)
        results['personnel'] = new_rows
        print(f'    Рядків у файлі: {len(df)}')
        print(f'    {green(f"Нових: {len(new_rows)}")}')
        print(f'    {dim(f"Дублікати ПІБ: {len(dup_pib)}")}')
        if dup_ipn:
            print(f'    {dim(f"Дублікати ІПН (в межах Excel): {len(dup_ipn)}")}')
        if new_rows:
            print(f'    {bold("Перші 5 нових:")}')
            for row in new_rows[:5]:
                pib = norm_pib(row.get('\u041f.\u0406.\u0411.') or row.get('\u041f\u0406\u0411')) or '—'
                rank = s(row.get('\u0432\u0456\u0439\u0441\u044c\u043a\u043e\u0432\u0435 \u0437\u0432\u0430\u043d\u043d\u044f')) or '—'
                print(f'      • {pib:<35} {rank}')
            if len(new_rows) > 5:
                print(f'      ... та ще {len(new_rows)-5}')
        print()

    if 'szc' in args.sheets and SZC in xl:
        print(bold('  [СЗЧ] Аналіз...'))
        df = xl[SZC]
        new_rows, dups = analyze_szc(df, conn)
        results['szc'] = new_rows
        print(f'    Рядків у файлі: {len(df)}')
        print(f'    {green(f"Нових: {len(new_rows)}")}')
        print(f'    {dim(f"Дублікати: {len(dups)}")}')
        print()

    if 'transit' in args.sheets and 'TRANZIT' in xl:
        print(bold('  [ТРАНЗИТ] Аналіз...'))
        df = xl['TRANZIT']
        new_rows, dups = analyze_transit(df, conn)
        results['transit'] = new_rows
        print(f'    Рядків у файлі: {len(df)}')
        print(f'    {green(f"Нових: {len(new_rows)}")}')
        print(f'    {dim(f"Дублікати: {len(dups)}")}')
        print()

    total_new = sum(len(v) for v in results.values())

    print(bold('  ┌─ Підсумок ─────────────────────────────────┐'))
    print(f'  │  Нових в/с:       {green(str(len(results.get("personnel",[]))))}')
    print(f'  │  Нових СЗЧ:       {green(str(len(results.get("szc",[]))))}')
    print(f'  │  Нових транзитних: {green(str(len(results.get("transit",[]))))}')
    print(f'  │  {bold(f"Всього: {total_new}")}')
    print(bold('  └───────────────────────────────────────────┘'))
    print()

    if total_new == 0:
        print(green('  ✓ Нових записів немає. БД актуальна.'))
        conn.close(); return

    if args.dry_run:
        print(yellow('  DRY-RUN: запис не виконувався.'))
        conn.close(); return

    if not args.yes:
        ans = input(f'  Записати {bold(str(total_new))} нових записів? [y/N]: ').strip().lower()
        if ans not in ('y', 'yes', 'т', 'так'):
            print(yellow('  Скасовано.'))
            conn.close(); return

    # ── Резервна копія ────────────────────────────────────────────────────────
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    bdir = db_path.parent / 'backups'
    bdir.mkdir(exist_ok=True)
    bpath = bdir / f'military_{ts}.db'
    shutil.copy2(str(db_path), str(bpath))
    print(f'  {dim(f"Резервна копія: {bpath.name}")}')
    print()

    # ── Запис ─────────────────────────────────────────────────────────────────
    if results.get('personnel'):
        print(f'  Запис {len(results["personnel"])} в/с...', end='', flush=True)
        n = insert_personnel(results['personnel'], conn)
        print(f'  {green(f"✓ {n} додано")}')

    # Будуємо pib->id для szc/transit
    pib_to_id = {}
    for row in conn.execute('SELECT id, pib FROM personnel'):
        pn = norm_pib(row[1])
        if pn: pib_to_id[pn] = row[0]

    if results.get('szc'):
        print(f'  Запис {len(results["szc"])} СЗЧ...', end='', flush=True)
        n = insert_szc(results['szc'], conn, pib_to_id)
        print(f'  {green(f"✓ {n} додано")}')

    if results.get('transit'):
        print(f'  Запис {len(results["transit"])} транзитних...', end='', flush=True)
        n = insert_transit(results['transit'], conn, pib_to_id)
        print(f'  {green(f"✓ {n} додано")}')

    print()
    print(bold('  ┌─ Стан БД після оновлення ─────────────────┐'))
    print(f'  │  personnel:       {db_count(conn,"personnel"):>6} записів')
    print(f'  │  szc_journal:     {db_count(conn,"szc_journal"):>6} записів')
    print(f'  │  transit_journal: {db_count(conn,"transit_journal"):>6} записів')
    print(bold('  └───────────────────────────────────────────┘'))
    print()
    print(green(f'  ✅ Готово! Додано {total_new} нових записів.'))
    print()
    conn.close()


if __name__ == '__main__':
    main()
