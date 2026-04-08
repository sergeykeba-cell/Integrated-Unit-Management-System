# Integrated-Unit-Management-System
A lightweight, self-hosted web application for personnel records, document workflow, and medical registry management — built for real operational use.
# Integrated Unit Management System

> A lightweight, self-hosted web application for personnel records, document workflow, and medical registry management — built for real operational use.

---

## 🚀 What it does

This system replaced manual Excel spreadsheets and paper-based workflows with a structured, role-controlled web interface accessible from any browser on the local network.

**Core features:**
- 👥 **Personnel registry** — add, edit, search personnel records
- 📄 **PDF document generation** — auto-fill documents from templates with variable substitution (compliant with official formatting standards)
- 🏥 **Medical registry** — track and update medical records per role
- 🔐 **Role-based access control** — Admin / Operator / Medic / Reader with granular permissions
- 📋 **Audit log** — every action is logged with timestamp and user
- 📦 **SQLite backend** — zero-config, portable, no external database required
- 🌐 **Single-file frontend** — runs entirely in the browser, no framework needed

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3, standard library HTTP server |
| Database | SQLite3 |
| Frontend | Vanilla HTML/CSS/JS (single file) |
| PDF generation | ReportLab |
| Auth | PBKDF2-HMAC-SHA256 (600k iterations, NIST SP 800-132) |

---

## 🔒 Security

- Password hashing: PBKDF2-HMAC-SHA256, 600,000 iterations
- Timing-safe comparison via `hmac.compare_digest`
- Rate limiting on login endpoint (5 attempts / 5 min per IP)
- Session expiry with hourly garbage collection
- Path traversal protection (`os.path.commonpath`)
- CORS restricted to same origin
- POST body size limited to 10MB
- Admin-only SQL query endpoint with hard row limit (500)

---

## ⚡ Quick Start

```bash
# Clone the repo
git clone https://github.com/sergeykeba-cell/integrated-system
cd integrated

# Install dependencies
pip install reportlab

# Run
python server.py
# or on Windows: START.bat
```

Open `http://localhost:7020` in your browser.

---

## 📁 Project Structure

```
integrated/
├── server.py           # HTTP server + REST API
├── auth.py             # Authentication & session management
├── db_write.py         # Database read/write operations
├── pdf_generator.py    # PDF document generation
├── import_extended.py  # Bulk data import
├── profile_manager.py  # User profile management
├── update_db.py        # Database schema migrations
├── app.html            # Single-file frontend
├── templates/
│   └── templates.json  # Document templates with variables
└── users.json          # User accounts (excluded from repo)
```

---

## 💡 Background

This project was built to solve a real operational problem: manual data entry across multiple Excel files caused errors, duplication, and slow document turnaround. The system unified all records into a single searchable database and automated document generation — reducing processing time significantly.

**Built by:** Sergiy Keba  
**Stack:** Python · SQLite · HTML/JS · ReportLab  
**Status:** Production (actively used)

---

## 📬 Contact

Open to freelance projects involving:
- Business process automation
- Internal tools replacing Excel/paper workflows
- Logistics & warehouse management systems

**GitHub:** [github.com/sergeykeba-cell](https://github.com/sergeykeba-cell)
