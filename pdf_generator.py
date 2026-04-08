"""
pdf_generator.py
Модуль генерації PDF-документів для ЗСУ
Відповідає вимогам Інструкції з діловодства у ЗСУ (наказ №40 від 31.01.2024)
Поля: ліве ≥30мм, праве 10мм, верхнє/нижнє 20мм
Шрифт: Liberation Serif (метрично сумісний з Times New Roman)
"""

import os
import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors


# ─── Шляхи до шрифтів ───────────────────────────────────────────────────────
FONT_DIR = "/usr/share/fonts/truetype/liberation"
FONT_REGULAR = os.path.join(FONT_DIR, "LiberationSerif-Regular.ttf")
FONT_BOLD = os.path.join(FONT_DIR, "LiberationSerif-Bold.ttf")
FONT_ITALIC = os.path.join(FONT_DIR, "LiberationSerif-Italic.ttf")
FONT_BOLD_ITALIC = os.path.join(FONT_DIR, "LiberationSerif-BoldItalic.ttf")


def register_fonts():
    """Реєстрація кириличних шрифтів у ReportLab"""
    try:
        pdfmetrics.registerFont(TTFont("TimesUkr", FONT_REGULAR))
        pdfmetrics.registerFont(TTFont("TimesUkr-Bold", FONT_BOLD))
        pdfmetrics.registerFont(TTFont("TimesUkr-Italic", FONT_ITALIC))
        pdfmetrics.registerFont(TTFont("TimesUkr-BoldItalic", FONT_BOLD_ITALIC))
        from reportlab.pdfbase.pdfmetrics import registerFontFamily
        registerFontFamily(
            "TimesUkr",
            normal="TimesUkr",
            bold="TimesUkr-Bold",
            italic="TimesUkr-Italic",
            boldItalic="TimesUkr-BoldItalic",
        )
        return True
    except Exception as e:
        print(f"[WARN] Помилка реєстрації шрифтів: {e}")
        return False


# ─── Відступи згідно Інструкції ─────────────────────────────────────────────
LEFT_MARGIN = 30 * mm
RIGHT_MARGIN = 10 * mm
TOP_MARGIN = 20 * mm
BOTTOM_MARGIN = 20 * mm

# Ширина тексту
PAGE_WIDTH = A4[0]  # 210mm
TEXT_WIDTH = PAGE_WIDTH - LEFT_MARGIN - RIGHT_MARGIN  # ≈170mm


def get_styles():
    """Повертає набір стилів для документів ЗСУ"""
    register_fonts()

    base = dict(fontName="TimesUkr", fontSize=14, leading=20)

    styles = {
        "normal": ParagraphStyle(
            "Normal_ZSU",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_JUSTIFY,
            firstLineIndent=12.5 * mm,
        ),
        "normal_no_indent": ParagraphStyle(
            "Normal_NoIndent",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_JUSTIFY,
        ),
        "center": ParagraphStyle(
            "Center_ZSU",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_CENTER,
        ),
        "center_bold": ParagraphStyle(
            "CenterBold_ZSU",
            fontName="TimesUkr-Bold",
            fontSize=14,
            leading=20,
            alignment=TA_CENTER,
        ),
        "right": ParagraphStyle(
            "Right_ZSU",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_RIGHT,
        ),
        "right_bold": ParagraphStyle(
            "RightBold_ZSU",
            fontName="TimesUkr-Bold",
            fontSize=14,
            leading=20,
            alignment=TA_RIGHT,
        ),
        "left": ParagraphStyle(
            "Left_ZSU",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_LEFT,
        ),
        "heading": ParagraphStyle(
            "Heading_ZSU",
            fontName="TimesUkr-Bold",
            fontSize=14,
            leading=20,
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
        "small": ParagraphStyle(
            "Small_ZSU",
            fontName="TimesUkr",
            fontSize=12,
            leading=16,
            alignment=TA_LEFT,
        ),
        "signature_label": ParagraphStyle(
            "SigLabel",
            fontName="TimesUkr",
            fontSize=14,
            leading=20,
            alignment=TA_LEFT,
        ),
    }
    return styles


def sp(n=1):
    """Відступ між блоками"""
    return Spacer(1, n * 6 * mm)


def format_date_ukr(date_str: str) -> str:
    """Перетворює YYYY-MM-DD у форматований рядок: '15 червня 2025 р.'"""
    months = [
        "", "січня", "лютого", "березня", "квітня", "травня", "червня",
        "липня", "серпня", "вересня", "жовтня", "листопада", "грудня"
    ]
    try:
        d = datetime.date.fromisoformat(date_str)
        return f"{d.day} {months[d.month]} {d.year} р."
    except Exception:
        return date_str


# ══════════════════════════════════════════════════════════════════════════════
#  ГЕНЕРАТОРЫ ДОКУМЕНТІВ
# ══════════════════════════════════════════════════════════════════════════════

def _rapport_header(data: dict, s: dict, story: list):
    """
    Уніфікована шапка рапорту:
    права колонка (60%) — адресат (кому), потім від кого.
    Ліва колонка (40%) — порожня.
    """
    right_col_w = TEXT_WIDTH * 0.60
    left_col_w  = TEXT_WIDTH - right_col_w

    def addr_table(lines):
        rows = [[Paragraph("", s["left"]), Paragraph(line, s["left"])] for line in lines]
        t = Table(rows, colWidths=[left_col_w, right_col_w])
        t.setStyle(TableStyle([
            ("ALIGN",         (0, 0), (-1, -1), "LEFT"),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        return t

    # Кому
    cmd_rank = data.get("commander_rank", "")
    cmd_pos  = data.get("commander_position", "")
    cmd_name = data.get("commander_name", "")
    addr_lines = [l for l in [cmd_rank, cmd_pos, cmd_name] if l]
    if addr_lines:
        story.append(addr_table(addr_lines))

    story.append(Spacer(1, 3 * mm))

    # Від кого
    from_rank = data.get("author_rank", "")
    from_pos  = data.get("author_position", "")
    from_name = data.get("author_name_full", "")
    from_lines = [l for l in [from_rank, from_pos, from_name] if l]
    if from_lines:
        story.append(addr_table(from_lines))


def _rapport_signature(data: dict, s: dict, story: list):
    """
    Підпис рапорту:
    звання ліво — ПІБ скорочено право
    дата окремим рядком зліва
    """
    rank_short = data.get("author_rank", "")
    name_short = data.get("author_name_short", "")
    doc_date   = format_date_ukr(data.get("doc_date", str(datetime.date.today())))

    if rank_short or name_short:
        story.append(_sig_rank_name(rank_short, name_short, s))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(doc_date, s["left"]))


def generate_rapport_vidpustka(data: dict, output_path: str) -> str:
    """Рапорт на відпустку"""
    s = get_styles()
    story = []

    _rapport_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>РАПОРТ</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    leave_type  = data.get("leave_type", "щорічну основну")
    leave_days  = data.get("leave_days", "")
    leave_start = format_date_ukr(data.get("leave_start", ""))
    leave_end   = format_date_ukr(data.get("leave_end", ""))
    leave_addr  = data.get("leave_address", "")

    text = (
        f"Прошу надати мені <u>{leave_type}</u> відпустку тривалістю "
        f"<b>{leave_days}</b> календарних днів з {leave_start} по {leave_end}."
    )
    story.append(Paragraph(text, s["normal"]))

    if leave_addr:
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph(
            f"Під час відпустки перебуватиму за адресою: {leave_addr}.",
            s["normal"]
        ))

    story.append(Spacer(1, 8 * mm))
    _rapport_signature(data, s, story)

    return _build_doc(story, output_path)


def generate_rapport_materialna(data: dict, output_path: str) -> str:
    """Рапорт на матеріальну допомогу"""
    s = get_styles()
    story = []

    _rapport_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>РАПОРТ</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    reason = data.get("help_reason", "")
    amount = data.get("help_amount", "")
    text = (
        f"Прошу надати мені матеріальну допомогу {reason}"
        + (f" у розмірі <b>{amount} грн</b>" if amount else "")
        + "."
    )
    story.append(Paragraph(text, s["normal"]))

    story.append(Spacer(1, 8 * mm))
    _rapport_signature(data, s, story)

    return _build_doc(story, output_path)


def generate_rapport_freestyle(data: dict, output_path: str) -> str:
    """Рапорт довільної форми"""
    s = get_styles()
    story = []

    _rapport_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>РАПОРТ</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    subject = data.get("rapport_subject", "Прошу Вас...")
    for para in subject.split("\n"):
        if para.strip():
            story.append(Paragraph(para.strip(), s["normal"]))

    story.append(Spacer(1, 8 * mm))
    _rapport_signature(data, s, story)

    return _build_doc(story, output_path)



def generate_dovidka_sluzhba(data: dict, output_path: str) -> str:
    """Довідка про проходження служби"""
    s = get_styles()
    story = []

    unit     = data.get("unit_number", "")
    doc_num  = data.get("doc_number", "")
    doc_date = format_date_ukr(data.get("doc_date", str(datetime.date.today())))
    location = data.get("location", "")

    # Шапка: назва частини по центру
    if unit:
        story.append(Paragraph(f"ВІЙСЬКОВА ЧАСТИНА {unit}", s["center_bold"]))
        story.append(Spacer(1, 2 * mm))

    # Рядок номера, дати, місця
    meta_parts = []
    if doc_num:
        meta_parts.append(f"№ {doc_num}")
    meta_parts.append(doc_date)
    if location:
        meta_parts.append(location)
    story.append(Paragraph("  ".join(meta_parts), s["center"]))
    story.append(Spacer(1, 8 * mm))

    # Заголовок
    story.append(Paragraph("<b>ДОВІДКА</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    # Текст
    rank    = data.get("author_rank", "")
    pos     = data.get("author_position", "")
    name    = data.get("author_name_full", "")
    svc_start = format_date_ukr(data.get("service_start", ""))

    body = (
        f"Дано {rank} {name}, що він (вона) дійсно проходить військову службу "
        f"на посаді {pos}"
        + (f" з {svc_start}" if svc_start and svc_start.strip() != "р." else "")
        + "."
    )
    story.append(Paragraph(body, s["normal"]))
    story.append(Spacer(1, 3 * mm))

    purpose = data.get("cert_purpose", "для пред'явлення за місцем вимоги")
    story.append(Paragraph(f"Довідка видана {purpose}.", s["normal"]))
    story.append(Spacer(1, 10 * mm))

    # Підпис: посада окремо, потім звання ліво — прізвище право
    sig_rank = data.get("signer_rank", "")
    sig_pos  = data.get("signer_position", "")
    sig_name = data.get("signer_name", "")

    if sig_pos:
        story.append(Paragraph(sig_pos, s["left"]))
    if sig_rank or sig_name:
        story.append(_sig_rank_name(sig_rank, sig_name, s))

    return _build_doc(story, output_path)


def generate_nakaz(data: dict, output_path: str) -> str:
    """Наказ по особовому складу"""
    s = get_styles()
    story = []

    unit     = data.get("unit_number", "")
    location = data.get("location", "")
    doc_date = format_date_ukr(data.get("doc_date", str(datetime.date.today())))
    doc_num  = data.get("doc_number", "")
    title    = data.get("nakaz_title", "По особовому складу").upper()

    # Шапка
    if unit:
        story.append(Paragraph(f"ВІЙСЬКОВА ЧАСТИНА {unit}", s["center_bold"]))
        story.append(Spacer(1, 2 * mm))

    story.append(Paragraph("<b>НАКАЗ</b>", s["center"]))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(f"<b>{title}</b>", s["center"]))
    story.append(Spacer(1, 4 * mm))

    # Дата, місце, номер
    parts = [doc_date]
    if location:
        parts.append(location)
    parts.append(f"№ {doc_num}")
    story.append(Paragraph("  ".join(parts), s["center"]))
    story.append(Spacer(1, 6 * mm))

    # Преамбула
    preamble = data.get("nakaz_preamble", "")
    if preamble:
        for para in preamble.split("\n"):
            if para.strip():
                story.append(Paragraph(para.strip(), s["normal"]))
        story.append(Spacer(1, 3 * mm))

    # НАКАЗУЮ
    story.append(Paragraph("<b>НАКАЗУЮ:</b>", s["normal_no_indent"]))
    story.append(Spacer(1, 3 * mm))

    body = data.get("nakaz_body", "")
    for para in body.split("\n"):
        if para.strip():
            story.append(Paragraph(para.strip(), s["normal"]))

    story.append(Spacer(1, 10 * mm))

    # Підпис: посада окремо, потім звання ліво — прізвище право
    sig_rank = data.get("signer_rank", "")
    sig_pos  = data.get("signer_position", "")
    sig_name = data.get("signer_name", "")

    if sig_pos:
        story.append(Paragraph(sig_pos, s["left"]))
    if sig_rank or sig_name:
        story.append(_sig_rank_name(sig_rank, sig_name, s))

    return _build_doc(story, output_path)



# ══════════════════════════════════════════════════════════════════════════════
#  ДОПОМІЖНІ ФУНКЦІЇ
# ══════════════════════════════════════════════════════════════════════════════

def _addressee_block(right_paragraphs: list):
    """
    Реквізит 'Адресат' у правому верхньому куті.
    Реалізується таблицею 2 колонки: ліва порожня, права з текстом.
    """
    right_col_width = TEXT_WIDTH * 0.45
    left_col_width = TEXT_WIDTH - right_col_width

    rows = [[Paragraph("", ParagraphStyle("e", fontName="TimesUkr", fontSize=14)), p]
            for p in right_paragraphs]

    t = Table(rows, colWidths=[left_col_width, right_col_width])
    t.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    return t


def _signature_block(left_label: str, right_label: str, styles: dict) -> list:
    """
    Блок підпису: [звання/дата] ліво — [ПІБ] право.
    Реалізація через tab-stop у одному параграфі (без таблиці).
    """
    from reportlab.platypus import Paragraph as _P
    from reportlab.lib.styles import ParagraphStyle as _PS
    from reportlab.lib.enums import TA_LEFT as _TL

    sig_style = _PS(
        "SigTab",
        fontName="TimesUkr",
        fontSize=14,
        leading=20,
        alignment=_TL,
    )
    # Tab-stop до правого краю → прізвище вирівнюється по правому полю
    # Використовуємо HTML-трюк: два span у одному параграфі
    text = (
        f'<para><seq id="s"/>'
        f'<font name="TimesUkr">{left_label}</font>'
        f'<tabStop alignment="right" leader=" "/>'
        f'</para>'
    )
    # Простіший варіант: два окремих параграфи в одній рядковій таблиці
    # але з явно встановленою шириною = TEXT_WIDTH
    row = [[
        Paragraph(left_label, styles["left"]),
        Paragraph(right_label, styles["right"]),
    ]]
    col_l = TEXT_WIDTH * 0.42
    col_r = TEXT_WIDTH - col_l
    t = Table(row, colWidths=[col_l, col_r])
    t.setStyle(TableStyle([
        ("ALIGN",         (0, 0), (0, 0), "LEFT"),
        ("ALIGN",         (1, 0), (1, 0), "RIGHT"),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",   (0, 0), (0, -1), 0),
        ("LEFTPADDING",   (1, 0), (1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (0, -1), 0),
        ("RIGHTPADDING",  (1, 0), (1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return [t]


def _sig_rank_name(rank_text: str, name_text: str, styles: dict):
    """
    Рядок підпису: звання/посада ліво — Ім'я ПРІЗВИЩЕ право.
    Використовує таблицю з нульовими відступами та точними колонками.
    """
    col_l = TEXT_WIDTH * 0.42
    col_r = TEXT_WIDTH - col_l
    t = Table(
        [[Paragraph(rank_text, styles["left"]), Paragraph(name_text, styles["right"])]],
        colWidths=[col_l, col_r]
    )
    t.setStyle(TableStyle([
        ("ALIGN",         (0, 0), (0, 0), "LEFT"),
        ("ALIGN",         (1, 0), (1, 0), "RIGHT"),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",   (0, 0), (0, -1), 0),
        ("LEFTPADDING",   (1, 0), (1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (0, -1), 0),
        ("RIGHTPADDING",  (1, 0), (1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return t


def _build_doc(story: list, output_path: str) -> str:
    """Збирає PDF із ReportLab SimpleDocTemplate"""
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=LEFT_MARGIN,
        rightMargin=RIGHT_MARGIN,
        topMargin=TOP_MARGIN,
        bottomMargin=BOTTOM_MARGIN,
        title="Документ ЗСУ",
    )
    doc.build(story)
    return output_path


# ══════════════════════════════════════════════════════════════════════════════
#  ДИСПЕТЧЕР
# ══════════════════════════════════════════════════════════════════════════════

def generate_rapport_vlk(data: dict, output_path: str) -> str:
    """
    Рапорт на ВЛК з резолюціями командира взводу та роти.
    Верстка точно відповідає шаблону rapport_keba_template.docx:
      - Адресат у правому верхньому куті (60% правої колонки)
      - РАПОРТ по центру жирним
      - Текст з відступом першого рядка, по ширині
      - Підпис: звання ліво, Ім'я ПРІЗВИЩЕ право (без риски)
      - Дата окремим рядком зліва
      - Блок 2: резолюція ком. взводу
      - Блок 3: резолюція ком. роти
    """
    s = get_styles()

    right_col_w = TEXT_WIDTH * 0.60
    left_col_w  = TEXT_WIDTH - right_col_w

    def addressee(lines):
        rows = [[Paragraph("", s["left"]), Paragraph(line, s["left"])] for line in lines]
        t = Table(rows, colWidths=[left_col_w, right_col_w])
        t.setStyle(TableStyle([
            ("ALIGN",         (0, 0), (-1, -1), "LEFT"),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        return t

    def sig_line(rank_text, name_text):
        """Підпис через таблицю з фіксованими колонками без внутрішніх відступів."""
        col_l = TEXT_WIDTH * 0.42
        col_r = TEXT_WIDTH - col_l
        t = Table(
            [[Paragraph(rank_text, s["left"]), Paragraph(name_text, s["right"])]],
            colWidths=[col_l, col_r]
        )
        t.setStyle(TableStyle([
            ("ALIGN",         (0, 0), (0, 0), "LEFT"),
            ("ALIGN",         (1, 0), (1, 0), "RIGHT"),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",   (0, 0), (0, -1), 0),
            ("LEFTPADDING",   (1, 0), (1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (0, -1), 0),
            ("RIGHTPADDING",  (1, 0), (1, -1), 0),
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        return t

    ssp = lambda: Spacer(1, 2 * mm)
    bsp = lambda: Spacer(1, 4 * mm)

    story = []
    doc_date     = format_date_ukr(data.get("doc_date", str(datetime.date.today())))
    surname_ini  = data.get("sender_surname_initials", "")
    name_dat     = data.get("sender_full_name_dative", "")
    dob          = data.get("sender_dob", "")
    phone        = data.get("sender_phone", "")
    proc_type    = data.get("procedure_type", "проходження ВЛК")
    hospital     = data.get("vlk_hospital", "")
    diagnosis    = data.get("full_diagnosis", "")
    attachment   = data.get("medical_attachment", "")
    sender_short = data.get("sender_short_name", "")
    plt_rank     = data.get("plt_cmd_rank", "лейтенант")
    plt_name     = data.get("plt_cmd_name", "")
    coy_rank     = data.get("coy_cmd_rank", "лейтенант")
    coy_name     = data.get("coy_cmd_name", "")

    # ── Блок 1: РАПОРТ ───────────────────────────────────────────────
    story.append(addressee([
        "Командиру взводу резерву роти резерву",
        "сержантського (старшинського) складу",
        "військової частини А7020",
    ]))
    story.append(bsp())
    story.append(Paragraph("<b>РАПОРТ</b>", s["center"]))
    story.append(bsp())

    # Компактний стиль для тіла рапорту (14pt, щільний)
    body_style = ParagraphStyle(
        "BodyVLK",
        fontName="TimesUkr",
        fontSize=12,
        leading=16,
        alignment=4,  # JUSTIFY
        firstLineIndent=12.5 * mm,
    )
    compact = ParagraphStyle(
        "CompactVLK",
        fontName="TimesUkr",
        fontSize=12,
        leading=16,
        alignment=0,  # LEFT
    )
    body = (
        f"Прошу Вашого клопотання перед командуванням частини про надання мені, "
        f"солдату {name_dat}, {dob} року народження, моб. {phone}, "
        f"направлення на {proc_type} в {hospital} "
        f"у зв\'язку зі зміною стану здоров\'я. "
        f"Останній діагноз: {diagnosis}."
    )
    story.append(Paragraph(body, body_style))
    story.append(ssp())
    story.append(Paragraph(f"Додаток: {attachment} на 1 арк. в 1 прим.", compact))
    story.append(ssp())
    story.append(Paragraph("Тимчасово облікований солдат взводу резерву", compact))
    story.append(Paragraph("сержантського (старшинського) складу роти резерву", compact))
    story.append(Paragraph("рядового складу військової частини А7020", compact))
    story.append(ssp())
    story.append(sig_line("солдат", sender_short))
    story.append(ssp())
    story.append(Paragraph(doc_date, s["left"]))
    story.append(Spacer(1, 2 * mm))

    # ── Блок 2: Резолюція ком. взводу ────────────────────────────────
    story.append(addressee([
        "Командиру роти резерву сержантського",
        "(старшинського) складу в/ч А7020",
    ]))
    story.append(ssp())
    story.append(Paragraph(f"Клопочу по суті рапорту солдата {surname_ini}.", compact))
    story.append(ssp())
    story.append(Paragraph("Командир взводу резерву сержантського (старшинського) складу", compact))
    story.append(Paragraph("роти резерву сержантського (старшинського) складу в/ч А7020", compact))
    story.append(ssp())
    story.append(sig_line(plt_rank, plt_name))
    story.append(ssp())
    story.append(Paragraph(doc_date, s["left"]))
    story.append(Spacer(1, 2 * mm))

    # ── Блок 3: Резолюція ком. роти ──────────────────────────────────
    story.append(addressee(["Командиру військової частини А7020"]))
    story.append(ssp())
    story.append(Paragraph(f"Клопочу по суті рапорту солдата {surname_ini}.", compact))
    story.append(ssp())
    story.append(Paragraph("Командир роти резерву сержантського (старшинського) складу", compact))
    story.append(Paragraph("військової частини А7020", compact))
    story.append(ssp())
    story.append(sig_line(coy_rank, coy_name))
    story.append(ssp())
    story.append(Paragraph(doc_date, s["left"]))

    return _build_doc(story, output_path)


GENERATORS = {
    "rapport_vidpustka": generate_rapport_vidpustka,
    "rapport_materialna": generate_rapport_materialna,
    "rapporт_svoechasno": generate_rapport_freestyle,
    "dovidka_sluzhba": generate_dovidka_sluzhba,
    "nakaz_viddil": generate_nakaz,
    "rapport_vlk": generate_rapport_vlk,
}


def generate_document(doc_type: str, data: dict, output_path: str) -> str:
    """
    Головна точка входу для генерації PDF.
    :param doc_type: ключ типу документа (з templates.json)
    :param data: словник із заповненими полями
    :param output_path: шлях для збереження PDF
    :return: шлях до згенерованого файлу
    """
    generator = GENERATORS.get(doc_type)
    if not generator:
        raise ValueError(f"Невідомий тип документа: {doc_type}")
    register_fonts()
    return generator(data, output_path)


if __name__ == "__main__":
    # Тест
    test_data = {
        "commander_rank": "полковнику",
        "commander_position": "командиру військової частини А1234",
        "commander_name": "Петренку П.П.",
        "author_rank": "старший лейтенант",
        "author_position": "командир взводу",
        "author_name_full": "Іваненко Іван Іванович",
        "author_name_short": "І.І. Іваненко",
        "leave_type": "щорічну основну",
        "leave_days": "30",
        "leave_start": "2025-07-01",
        "leave_end": "2025-07-30",
        "leave_address": "м. Київ, вул. Хрещатик, 1",
        "unit_number": "А1234",
        "location": "м. Дніпро",
        "doc_date": str(datetime.date.today()),
    }
    out = generate_document("rapport_vidpustka", test_data, "/tmp/test_rapport.pdf")
    print(f"[OK] Згенеровано: {out}")
