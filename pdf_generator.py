"""
pdf_generator.py
Модуль генерації PDF-документів (заяви, довідки, накази)
Стандартний шаблон офісного діловодства.
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


# ─── Відступи ────────────────────────────────────────────────────────────
LEFT_MARGIN = 30 * mm
RIGHT_MARGIN = 10 * mm
TOP_MARGIN = 20 * mm
BOTTOM_MARGIN = 20 * mm

PAGE_WIDTH = A4[0]
TEXT_WIDTH = PAGE_WIDTH - LEFT_MARGIN - RIGHT_MARGIN


def get_styles():
    """Повертає набір стилів для документів"""
    register_fonts()

    styles = {
        "normal": ParagraphStyle(
            "Normal", fontName="TimesUkr", fontSize=14, leading=20,
            alignment=TA_JUSTIFY, firstLineIndent=12.5 * mm,
        ),
        "normal_no_indent": ParagraphStyle(
            "Normal_NoIndent", fontName="TimesUkr", fontSize=14, leading=20,
            alignment=TA_JUSTIFY,
        ),
        "center": ParagraphStyle(
            "Center", fontName="TimesUkr", fontSize=14, leading=20,
            alignment=TA_CENTER,
        ),
        "center_bold": ParagraphStyle(
            "CenterBold", fontName="TimesUkr-Bold", fontSize=14, leading=20,
            alignment=TA_CENTER,
        ),
        "right": ParagraphStyle(
            "Right", fontName="TimesUkr", fontSize=14, leading=20,
            alignment=TA_RIGHT,
        ),
        "right_bold": ParagraphStyle(
            "RightBold", fontName="TimesUkr-Bold", fontSize=14, leading=20,
            alignment=TA_RIGHT,
        ),
        "left": ParagraphStyle(
            "Left", fontName="TimesUkr", fontSize=14, leading=20,
            alignment=TA_LEFT,
        ),
        "heading": ParagraphStyle(
            "Heading", fontName="TimesUkr-Bold", fontSize=14, leading=20,
            alignment=TA_CENTER, spaceAfter=6,
        ),
        "small": ParagraphStyle(
            "Small", fontName="TimesUkr", fontSize=12, leading=16,
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


# ══════════════════════════════════════════════════════════════════════════
#  ГЕНЕРАТОРИ ДОКУМЕНТІВ
# ══════════════════════════════════════════════════════════════════════════

def _zayava_header(data: dict, s: dict, story: list):
    """
    Уніфікована шапка заяви:
    права колонка (60%) — адресат (кому), потім від кого.
    """
    right_col_w = TEXT_WIDTH * 0.60
    left_col_w = TEXT_WIDTH - right_col_w

    def addr_table(lines):
        rows = [[Paragraph("", s["left"]), Paragraph(line, s["left"])] for line in lines]
        t = Table(rows, colWidths=[left_col_w, right_col_w])
        t.setStyle(TableStyle([
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        return t

    recipient_pos = data.get("recipient_position", "")
    recipient_name = data.get("recipient_name", "")
    addr_lines = [l for l in [recipient_pos, recipient_name] if l]
    if addr_lines:
        story.append(addr_table(addr_lines))

    story.append(Spacer(1, 3 * mm))

    author_pos = data.get("author_position", "")
    author_name = data.get("author_name_full", "")
    from_lines = [l for l in [author_pos, author_name] if l]
    if from_lines:
        story.append(addr_table(from_lines))


def _zayava_signature(data: dict, s: dict, story: list):
    """
    Підпис заяви:
    посада ліво — ПІБ скорочено право
    дата окремим рядком зліва
    """
    pos_short = data.get("author_position", "")
    name_short = data.get("author_name_short", "")
    doc_date = format_date_ukr(data.get("doc_date", str(datetime.date.today())))

    if pos_short or name_short:
        story.append(_sig_line(pos_short, name_short, s))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(doc_date, s["left"]))


def generate_zayava_vidpustka(data: dict, output_path: str) -> str:
    """Заява на відпустку"""
    s = get_styles()
    story = []

    _zayava_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>ЗАЯВА</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    leave_type = data.get("leave_type", "щорічну основну")
    leave_days = data.get("leave_days", "")
    leave_start = format_date_ukr(data.get("leave_start", ""))
    leave_end = format_date_ukr(data.get("leave_end", ""))
    leave_addr = data.get("leave_address", "")

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
    _zayava_signature(data, s, story)

    return _build_doc(story, output_path)


def generate_zayava_dopomoga(data: dict, output_path: str) -> str:
    """Заява на матеріальну допомогу"""
    s = get_styles()
    story = []

    _zayava_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>ЗАЯВА</b>", s["center"]))
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
    _zayava_signature(data, s, story)

    return _build_doc(story, output_path)


def generate_zayava_freestyle(data: dict, output_path: str) -> str:
    """Заява довільної форми"""
    s = get_styles()
    story = []

    _zayava_header(data, s, story)

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("<b>ЗАЯВА</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    subject = data.get("zayava_subject", "Прошу Вас...")
    for para in subject.split("\n"):
        if para.strip():
            story.append(Paragraph(para.strip(), s["normal"]))

    story.append(Spacer(1, 8 * mm))
    _zayava_signature(data, s, story)

    return _build_doc(story, output_path)


def generate_dovidka_robota(data: dict, output_path: str) -> str:
    """Довідка про роботу"""
    s = get_styles()
    story = []

    org_name = data.get("org_name", "")
    doc_num = data.get("doc_number", "")
    doc_date = format_date_ukr(data.get("doc_date", str(datetime.date.today())))
    location = data.get("location", "")

    if org_name:
        story.append(Paragraph(org_name.upper(), s["center_bold"]))
        story.append(Spacer(1, 2 * mm))

    meta_parts = []
    if doc_num:
        meta_parts.append(f"№ {doc_num}")
    meta_parts.append(doc_date)
    if location:
        meta_parts.append(location)
    story.append(Paragraph("  ".join(meta_parts), s["center"]))
    story.append(Spacer(1, 8 * mm))

    story.append(Paragraph("<b>ДОВІДКА</b>", s["center"]))
    story.append(Spacer(1, 6 * mm))

    pos = data.get("author_position", "")
    name = data.get("author_name_full", "")
    empl_start = format_date_ukr(data.get("employment_start", ""))

    body = (
        f"Дано {name}, що він (вона) дійсно працює на посаді {pos}"
        + (f" з {empl_start}" if empl_start and empl_start.strip() != "р." else "")
        + "."
    )
    story.append(Paragraph(body, s["normal"]))
    story.append(Spacer(1, 3 * mm))

    purpose = data.get("cert_purpose", "для пред'явлення за місцем вимоги")
    story.append(Paragraph(f"Довідка видана {purpose}.", s["normal"]))
    story.append(Spacer(1, 10 * mm))

    sig_pos = data.get("signer_position", "")
    sig_name = data.get("signer_name", "")

    if sig_pos:
        story.append(Paragraph(sig_pos, s["left"]))
    if sig_name:
        story.append(_sig_line("", sig_name, s))

    return _build_doc(story, output_path)


def generate_nakaz(data: dict, output_path: str) -> str:
    """Наказ по особовому складу"""
    s = get_styles()
    story = []

    org_name = data.get("org_name", "")
    location = data.get("location", "")
    doc_date = format_date_ukr(data.get("doc_date", str(datetime.date.today())))
    doc_num = data.get("doc_number", "")
    title = data.get("nakaz_title", "По особовому складу").upper()

    if org_name:
        story.append(Paragraph(org_name.upper(), s["center_bold"]))
        story.append(Spacer(1, 2 * mm))

    story.append(Paragraph("<b>НАКАЗ</b>", s["center"]))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(f"<b>{title}</b>", s["center"]))
    story.append(Spacer(1, 4 * mm))

    parts = [doc_date]
    if location:
        parts.append(location)
    parts.append(f"№ {doc_num}")
    story.append(Paragraph("  ".join(parts), s["center"]))
    story.append(Spacer(1, 6 * mm))

    preamble = data.get("nakaz_preamble", "")
    if preamble:
        for para in preamble.split("\n"):
            if para.strip():
                story.append(Paragraph(para.strip(), s["normal"]))
        story.append(Spacer(1, 3 * mm))

    story.append(Paragraph("<b>НАКАЗУЮ:</b>", s["normal_no_indent"]))
    story.append(Spacer(1, 3 * mm))

    body = data.get("nakaz_body", "")
    for para in body.split("\n"):
        if para.strip():
            story.append(Paragraph(para.strip(), s["normal"]))

    story.append(Spacer(1, 10 * mm))

    sig_pos = data.get("signer_position", "")
    sig_name = data.get("signer_name", "")

    if sig_pos:
        story.append(Paragraph(sig_pos, s["left"]))
    if sig_name:
        story.append(_sig_line("", sig_name, s))

    return _build_doc(story, output_path)


# ══════════════════════════════════════════════════════════════════════════
#  ДОПОМІЖНІ ФУНКЦІЇ
# ══════════════════════════════════════════════════════════════════════════

def _sig_line(left_text: str, right_text: str, styles: dict):
    """
    Рядок підпису: [посада] ліво — [ПІБ] право.
    Таблиця з нульовими відступами та фіксованими колонками.
    """
    col_l = TEXT_WIDTH * 0.42
    col_r = TEXT_WIDTH - col_l
    t = Table(
        [[Paragraph(left_text, styles["left"]), Paragraph(right_text, styles["right"])]],
        colWidths=[col_l, col_r]
    )
    t.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, 0), "LEFT"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (0, -1), 0),
        ("LEFTPADDING", (1, 0), (1, -1), 0),
        ("RIGHTPADDING", (0, 0), (0, -1), 0),
        ("RIGHTPADDING", (1, 0), (1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
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
        title="Документ",
    )
    doc.build(story)
    return output_path


# ══════════════════════════════════════════════════════════════════════════
#  ДИСПЕТЧЕР
# ══════════════════════════════════════════════════════════════════════════

GENERATORS = {
    "zayava_vidpustka": generate_zayava_vidpustka,
    "zayava_dopomoga": generate_zayava_dopomoga,
    "zayava_freestyle": generate_zayava_freestyle,
    "dovidka_robota": generate_dovidka_robota,
    "nakaz_viddil": generate_nakaz,
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
    test_data = {
        "recipient_position": "директору",
        "recipient_name": "Петренко П.П.",
        "author_position": "менеджер відділу продажів",
        "author_name_full": "Іваненко Іван Іванович",
        "author_name_short": "І.І. Іваненко",
        "leave_type": "щорічну основну",
        "leave_days": "24",
        "leave_start": "2025-07-01",
        "leave_end": "2025-07-24",
        "leave_address": "м. Київ, вул. Хрещатик, 1",
        "org_name": "ТОВ Приклад",
        "location": "м. Дніпро",
        "doc_date": str(datetime.date.today()),
    }
    out = generate_document("zayava_vidpustka", test_data, "/tmp/test_zayava.pdf")
    print(f"[OK] Згенеровано: {out}")
