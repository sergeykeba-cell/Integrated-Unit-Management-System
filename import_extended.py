#!/usr/bin/env python3
"""
import_extended.py — Імпорт додаткових файлів до military.db

Підтримувані формати:
  1. Додаток А7020 (СЗЧ реєстр) — .xlsx з аркушем 'А7020' та 'по в_ч'
  2. Прийомка — .ods або .xlsx з колонками: номер, в/з, ПІБ, д.н., СЗЧ В/Ч, придатність

Використання:
  python import_extended.py --file Додаток.xlsx --db military.db
  python import_extended.py --file Прийомка.ods  --db military.db
  python import_extended.py --file Додаток.xlsx --db military.db --dry-run
  python import_extended.py --file Прийомка.ods  --db military.db --clear
"""
import sqlite3, re, sys, argparse, datetime, os
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("[!] pandas not installed. Run: python -m pip install pandas openpyxl odfpy")
    sys.exit(1)

# ── Helpers ───────────────────────────────────────────────────────────────────

def s(v):
    if v is None: return None
    r = str(v).strip()
    return None if r in ('', 'nan', 'NaN', 'None', 'NaT', 'nat') else r

def norm_pib(v):
    r = s(v)
    if not r: return None
    return re.sub(r' +', ' ', r)

def parse_date(v):
    r = s(v)
    if not r: return None
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%Y'):
        try:
            return datetime.datetime.strptime(r[:10], fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    try:
        return pd.Timestamp(v).strftime('%Y-%m-%d')
    except Exception:
        return r[:20]

def clean_cols(df):
    df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in df.columns]
    return df

def detect_type(path: Path, df_sheets=None):
    """Detect file type by sheet names or columns."""
    ext = path.suffix.lower()
    if ext == '.ods':
        return 'reception'
    if ext in ('.xlsx', '.xls'):
        if df_sheets and 'А7020' in df_sheets:
            return 'szc_extended'
        # Try to detect by columns
        return 'reception'
    return None

# ── ANSI colors ───────────────────────────────────────────────────────────────

def c(t, code): return f"\033[{code}m{t}\033[0m"
def green(t):   return c(t, '32')
def yellow(t):  return c(t, '33')
def red(t):     return c(t, '31')
def bold(t):    return c(t, '1')
def dim(t):     return c(t, '2')
def cyan(t):    return c(t, '36')

# ── Schema creation ───────────────────────────────────────────────────────────

def ensure_tables(conn):
    # Migrate existing tables if needed
    for table, col, col_def in [
        ('szc_extended', 'source_file', 'TEXT'),
        ('szc_extended', 'erdr_info',   'TEXT'),
        ('szc_extended', 'bzvp_info',   'TEXT'),
        ('reception',    'source_file', 'TEXT'),
        ('military_units','source_file','TEXT'),
        ('military_units','branch_type','TEXT'),
        ('military_units','short_upper','TEXT'),
        ('military_units','priority',   'TEXT'),
    ]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        except Exception:
            pass
    conn.commit()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS military_units (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_code   TEXT UNIQUE,
            full_name   TEXT,
            branch      TEXT,
            branch_type TEXT,
            short_name  TEXT,
            short_upper TEXT,
            priority    TEXT
        );

        CREATE TABLE IF NOT EXISTS szc_extended (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            row_no              INTEGER,
            category            TEXT,
            rank_raw            TEXT,
            pib_raw             TEXT,
            date_of_birth       TEXT,
            source_unit         TEXT,
            source_unit_name    TEXT,
            suspension_info     TEXT,
            erdr_no             TEXT,
            erdr_info           TEXT,
            szc_date            TEXT,
            arrival_date        TEXT,
            bzvp_info           TEXT,
            sedo_out_no         TEXT,
            sedo_out_date       TEXT,
            branch_oc           TEXT,
            hr_decisions        TEXT,
            note                TEXT,
            fitness             TEXT,
            target_unit         TEXT,
            wish                TEXT,
            movement_order      TEXT,
            status              TEXT,
            status_date         TEXT,
            destination         TEXT,
            transfer_plan       TEXT,
            detachment_status   TEXT,
            detachment_date     TEXT,
            service_notes       TEXT,
            treatment_vlk       TEXT,
            unit_reply          TEXT,
            age_note            TEXT,
            health_state        TEXT,
            residence           TEXT,
            investigation_body  TEXT,
            dbr_sent            TEXT,
            court_materials     TEXT,
            court_session       TEXT,
            court_pending       TEXT,
            suspicion_served    TEXT,
            ruling_date         TEXT,
            court_decision      TEXT,
            vlk_fitness         TEXT,
            vlk_doc             TEXT,
            hospital_status     TEXT,
            hospital_since      TEXT,
            treatment_period    TEXT,
            diagnosis           TEXT,
            hospital_name       TEXT,
            discharge_date      TEXT,
            scars               TEXT,
            tattoos             TEXT,
            criminal_record     TEXT,
            source_file         TEXT,
            imported_at         TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_szce_pib ON szc_extended(pib_raw);
        CREATE INDEX IF NOT EXISTS idx_szce_unit ON szc_extended(source_unit);
        CREATE INDEX IF NOT EXISTS idx_szce_status ON szc_extended(status);

        CREATE TABLE IF NOT EXISTS reception (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_no     INTEGER,
            rank_raw     TEXT,
            pib_raw      TEXT,
            date_of_birth TEXT,
            source_unit  TEXT,
            fitness      TEXT,
            age_50plus   TEXT,
            fit_mark     TEXT,
            limited_mark TEXT,
            relation     TEXT,
            target_unit  TEXT,
            doc_ref      TEXT,
            source_file  TEXT,
            imported_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_rec_pib ON reception(pib_raw);
    """)
    conn.commit()

# ── Import: military_units ────────────────────────────────────────────────────

def import_units(df, conn, dry_run=False, source_file=''):
    df = clean_cols(df)
    print(f"  Колонки: {list(df.columns)}")

    # Try to find unit code column
    code_col = next((c for c in df.columns if 'А0000' in c or 'код' in c.lower()), df.columns[0])

    data = []
    for _, row in df.iterrows():
        code = s(row.get(code_col))
        if not code: continue
        data.append((
            code,
            s(row.get('відкриті назви в/ч')),
            s(row.get('Розр. по в/ч')),
            s(row.get('ВИД')),
            s(row.get('Скороч. мал.')),
            s(row.get('Скороч. велик.')),
            s(row.get('Приор.')),
            source_file,
        ))

    if not dry_run:
        conn.executemany("""
            INSERT OR IGNORE INTO military_units
                (unit_code, full_name, branch, branch_type, short_name, short_upper, priority, source_file)
            VALUES (?,?,?,?,?,?,?,?)
        """, data)
        conn.commit()

    return len(data)

# ── Import: szc_extended ──────────────────────────────────────────────────────

def import_szc_extended(df, conn, dry_run=False, source_file=''):
    df = clean_cols(df)

    # Filter out empty PIB rows
    pib_col = next((c for c in df.columns if c == 'ПІБ'), None)
    if not pib_col:
        print(red("  [!] Колонка 'ПІБ' не знайдена"))
        return 0

    df = df[df[pib_col].notna() & (df[pib_col].str.strip() != '') & (df[pib_col] != 'nan')]

    # Check for existing records to avoid duplicates
    existing = set()
    for row in conn.execute("SELECT pib_raw, szc_date FROM szc_extended WHERE source_file=?", (source_file,)):
        existing.add((row[0], row[1]))

    g = lambda col: None  # placeholder
    data = []
    skipped = 0

    for _, row in df.iterrows():
        pib = norm_pib(row.get('ПІБ'))
        if not pib: continue

        szc_date = parse_date(row.get('Дата здійснення СЗЧ'))
        key = (pib, szc_date)
        if key in existing:
            skipped += 1
            continue

        data.append((
            s(row.get('№')),
            s(row.get('Категорія')),
            s(row.get('Військове звання')),
            pib,
            parse_date(row.get('Дата народження')),
            s(row.get('Військова частина, яку військовослужбовець самовільно залишив')),
            s(row.get('Назви в/ч')),
            s(row.get('Відомості про призупинення служби')),
            s(row.get('Номер ЄРДР')),
            s(row.get('ЄРДР')),
            szc_date,
            parse_date(row.get('Дата прибуття військовослужбовця до резервного підрозділу')),
            s(row.get('БЗВП, періоди проходження, навчальний підрозділ та номер сертифікату')),
            s(row.get('Номер вихідного повідомлення СЕДО на попередню частину')),
            parse_date(row.get('Дата вихідного повідомлення на навчальні центри')),
            s(row.get('Рід військ, оперативне командування')),
            s(row.get('Кадрові рішення')),
            s(row.get('Примітка')),
            s(row.get('Придатність до військової служби')),
            s(row.get('Куди по наказу')),
            s(row.get('Бажання військовослужбовця')),
            s(row.get('Наказ на переміщення з А7020')),
            s(row.get('Статус')),
            parse_date(row.get('Дата')),
            s(row.get('Куди вибув')),
            s(row.get('Поданий план переміщення про зарахування в А7020')),
            s(row.get('Статус у відрядженні')),
            parse_date(row.get('Дата зміни статусу у відрядженні')),
            s(row.get('Службові примітки')),
            s(row.get('ЛІКУВАННЯ або ВЛК')),
            s(row.get('Відповідь від військової частини чи навчального центру')),
            s(row.get('Примітка Вік, 50+ в/ч жінки')),
            s(row.get("Відомості про стан здоров'я")),
            s(row.get('Проживання')),
            s(row.get('Орган досудового розслідування')),
            s(row.get('Направлено повідомлення до ДБР')),
            s(row.get('Направлені матеріали до суду')),
            s(row.get('Судове засідання')),
            s(row.get('Очікують рішення суду')),
            s(row.get('Вручено підозру')),
            parse_date(row.get('Дата коли ухвала набрала законної сили')),
            s(row.get('Ухвала суду про звільнення від кримінальної відповідальності')),
            s(row.get('Придатність до військової служби.1')),
            s(row.get('Номер, дата документа і де проходив ВЛК')),
            s(row.get('Знаходиться на лікуванні, обстежень')),
            parse_date(row.get('Дата з якого перебуває в лікарні')),
            s(row.get('Термін лікування')),
            s(row.get('Діагноз')),
            s(row.get('Лікувальний заклад')),
            parse_date(row.get('Дата виписки')),
            s(row.get('Шрами')),
            s(row.get('Татуювання')),
            s(row.get('Судимість')),
            source_file,
        ))

    if not dry_run and data:
        conn.executemany("""
            INSERT INTO szc_extended (
                row_no, category, rank_raw, pib_raw, date_of_birth,
                source_unit, source_unit_name, suspension_info, erdr_no, erdr_info,
                szc_date, arrival_date, bzvp_info, sedo_out_no, sedo_out_date,
                branch_oc, hr_decisions, note, fitness, target_unit,
                wish, movement_order, status, status_date, destination,
                transfer_plan, detachment_status, detachment_date, service_notes,
                treatment_vlk, unit_reply, age_note, health_state, residence,
                investigation_body, dbr_sent, court_materials, court_session,
                court_pending, suspicion_served, ruling_date, court_decision,
                vlk_fitness, vlk_doc, hospital_status, hospital_since,
                treatment_period, diagnosis, hospital_name, discharge_date,
                scars, tattoos, criminal_record, source_file
            ) VALUES (""" + ",".join(["?"] * 54) + ")", data)
        conn.commit()

    return len(data), skipped

# ── Import: reception ─────────────────────────────────────────────────────────

def import_reception(df, conn, dry_run=False, source_file=''):
    df = clean_cols(df)
    print(f"  Колонки: {list(df.columns)}")

    pib_col = next((c for c in df.columns if c in ('ПІБ', 'піб', 'Пib')), None)
    if not pib_col:
        print(red("  [!] Колонка 'ПІБ' не знайдена"))
        return 0, 0

    df = df[df[pib_col].notna() & (df[pib_col].str.strip() != '') & (df[pib_col] != 'nan')]

    # Check duplicates by source_file+entry_no
    existing_nos = set(
        r[0] for r in conn.execute(
            "SELECT entry_no FROM reception WHERE source_file=?", (source_file,)
        )
    )

    data = []
    skipped = 0
    for _, row in df.iterrows():
        pib = norm_pib(row.get(pib_col))
        if not pib: continue
        entry_no = s(row.get('номер'))
        if entry_no and entry_no in existing_nos:
            skipped += 1
            continue
        data.append((
            entry_no,
            s(row.get('в/з')),
            pib,
            parse_date(row.get('д.н.')),
            s(row.get('СЗЧ В/Ч')),
            s(row.get('придатність')),
            s(row.get('50+')),
            s(row.get('прид.')),
            s(row.get('обмеж.')),
            s(row.get('відношення')),
            s(row.get('в/ч куди')),
            s(row.get('Unnamed: 11')),
            source_file,
        ))

    if not dry_run and data:
        conn.executemany("""
            INSERT INTO reception
                (entry_no,rank_raw,pib_raw,date_of_birth,source_unit,fitness,
                 age_50plus,fit_mark,limited_mark,relation,target_unit,doc_ref,source_file)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, data)
        conn.commit()

    return len(data), skipped

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Імпорт Додатку А7020 та Прийомки до military.db',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади:
  python import_extended.py --file Dodatok.xlsx --db military.db
  python import_extended.py --file "Priyomka 17.03.26.xlsx" --db military.db
  python import_extended.py --file Dodatok.xlsx --db military.db --dry-run
  python import_extended.py --file "Priyomka 17.03.26.xlsx" --db military.db --clear

Тип визначається автоматично по вмісту:
  - xlsx з аркушем 'А7020' → реєстр СЗЧ (Додаток)
  - xlsx/ods без аркуша 'А7020' → прийомка
        """
    )
    ap.add_argument('--file',    required=True, help='Шлях до .xlsx або .ods файлу')
    ap.add_argument('--db',      required=True, help='Шлях до military.db')
    ap.add_argument('--dry-run', action='store_true', help='Тільки аналіз, без запису')
    ap.add_argument('--clear',   action='store_true', help='Очистити таблицю перед імпортом')
    ap.add_argument('--type',    choices=['szc_extended','reception','auto'],
                    default='auto', help='Тип файлу (auto = визначити автоматично)')
    args = ap.parse_args()

    file_path = Path(args.file)
    db_path   = Path(args.db)

    if not db_path.exists():
        print(red(f"[!] БД не знайдено: {db_path}")); sys.exit(1)

    try:
        open(str(file_path), 'rb').close()
    except Exception as e:
        print(red(f"[!] Не вдається відкрити файл: {e}")); sys.exit(1)

    print()
    print(bold("═══════════════════════════════════════════════"))
    print(bold("  В/Ч А7020 · Імпорт розширених даних"))
    print(bold("═══════════════════════════════════════════════"))
    print(f"  Файл: {cyan(str(file_path))}  ({file_path.stat().st_size//1024} KB)")
    print(f"  БД:   {cyan(str(db_path))}")
    if args.dry_run:
        print(f"  {yellow('DRY-RUN — запис не виконується')}")
    print()

    conn = sqlite3.connect(str(db_path))
    ensure_tables(conn)

    source_file = file_path.name
    ext = file_path.suffix.lower()

    # Auto-detect type
    file_type = args.type
    if file_type == 'auto':
        if ext == '.ods':
            file_type = 'reception'
        elif ext in ('.xlsx', '.xls'):
            try:
                xf = pd.ExcelFile(str(file_path))
                file_type = 'szc_extended' if 'А7020' in xf.sheet_names else 'reception'
            except Exception:
                file_type = 'reception'
        else:
            file_type = 'reception'
        print(f"  Тип файлу: {bold(file_type)}")
    print()

    # ── szc_extended ──────────────────────────────────────────────────────────
    if file_type == 'szc_extended':
        xf = pd.ExcelFile(str(file_path))

        # Import units dictionary
        if 'по в_ч' in xf.sheet_names:
            print(bold("  [по в_ч] Довідник в/ч..."))
            df_units = xf.parse('по в_ч', header=0, dtype=str)
            n_units = import_units(df_units, conn, dry_run=args.dry_run, source_file=source_file)
            print(f"    {green(f'✓ {n_units} в/ч оброблено')}")
            print()

        # Import А7020 sheet
        if 'А7020' in xf.sheet_names:
            print(bold("  [А7020] СЗЧ реєстр..."))

            if args.clear and not args.dry_run:
                conn.execute("DELETE FROM szc_extended WHERE source_file=?", (source_file,))
                conn.commit()
                print(f"  {yellow('Попередні записи з цього файлу видалено')}")

            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df_szc = xf.parse('А7020', skiprows=5, header=0, dtype=str)

            df_szc = clean_cols(df_szc)
            total_rows = len(df_szc[df_szc.get('ПІБ', pd.Series()).notna()])
            print(f"    Рядків у файлі: {len(df_szc)}")

            n_new, n_skip = import_szc_extended(df_szc, conn,
                                                  dry_run=args.dry_run,
                                                  source_file=source_file)
            print(f"    {green(f'Нових: {n_new}')}  {dim(f'Пропущено (дублікати): {n_skip}')}")
            print()

        # Stats
        if not args.dry_run:
            total = conn.execute("SELECT COUNT(*) FROM szc_extended").fetchone()[0]
            by_status = conn.execute("""
                SELECT status, COUNT(*) c FROM szc_extended
                WHERE status IS NOT NULL GROUP BY status ORDER BY c DESC LIMIT 7
            """).fetchall()
            print(bold("  ┌─ Стан szc_extended ───────────────────────┐"))
            print(f"  │  Всього записів: {total}")
            for st, cnt in by_status:
                print(f"  │  {st:<20} {cnt:>6}")
            print(bold("  └───────────────────────────────────────────┘"))

    # ── reception ─────────────────────────────────────────────────────────────
    elif file_type == 'reception':
        print(bold("  [Прийомка] Завантаження..."))

        if args.clear and not args.dry_run:
            conn.execute("DELETE FROM reception WHERE source_file=?", (source_file,))
            conn.commit()
            print(f"  {yellow('Попередні записи з цього файлу видалено')}")

        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if ext == '.ods':
                df_rec = pd.read_excel(str(file_path), engine='odf', dtype=str)
            else:
                # Try multiple sheets - use first sheet with ПІБ column
                xf_rec = pd.ExcelFile(str(file_path))
                df_rec = None
                for sh in xf_rec.sheet_names:
                    _df = xf_rec.parse(sh, dtype=str)
                    _df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in _df.columns]
                    if any(c in ('ПІБ','піб') for c in _df.columns):
                        df_rec = _df
                        print(f"    Аркуш: '{sh}'")
                        break
                if df_rec is None:
                    df_rec = pd.read_excel(str(file_path), dtype=str)

        print(f"    Рядків у файлі: {len(df_rec)}")
        n_new, n_skip = import_reception(df_rec, conn,
                                          dry_run=args.dry_run,
                                          source_file=source_file)
        print(f"    {green(f'Нових: {n_new}')}  {dim(f'Пропущено: {n_skip}')}")
        print()

        if not args.dry_run:
            total = conn.execute("SELECT COUNT(*) FROM reception").fetchone()[0]
            by_fit = conn.execute("""
                SELECT fitness, COUNT(*) c FROM reception
                WHERE fitness IS NOT NULL GROUP BY fitness ORDER BY c DESC
            """).fetchall()
            print(bold("  ┌─ Стан reception ───────────────────────────┐"))
            print(f"  │  Всього записів: {total}")
            for ft, cnt in by_fit:
                print(f"  │  {ft:<30} {cnt:>4}")
            print(bold("  └───────────────────────────────────────────┘"))

    print()
    if args.dry_run:
        print(yellow("  DRY-RUN завершено. Змін не внесено."))
    else:
        print(green("  ✅ Імпорт завершено."))
    print()
    conn.close()


if __name__ == '__main__':
    main()
