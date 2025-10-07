# CNSS Salary Data Analysis System

A full-stack **data engineering and visualization platform** for analyzing salary statistics from CNSS declarations.
Built as a master’s project to demonstrate **ETL pipeline design, database optimization, statistical analysis, and interactive dashboards**.

( **CNSS = Caisse Nationale de Sécurité Sociale / national social security** ).

Built to demonstrate ETL pipelines, database optimization, statistical analysis, and interactive dashboards.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🚀 Overview

This project ingests CNSS salary declarations, cleans and loads them into PostgreSQL, and exposes an interactive Flask app for exploratory analysis and automated reporting.

- **Automated ETL**: PDF parsing → cleaning → PostgreSQL
- **Database optimization**: normalization, B-tree indexes, trigram fuzzy search, materialized views
- **Statistics**: Gini, Hoover, Atkinson, Lorenz curves, percentiles (P10–P99.9), deciles
- **Dashboards**: salary distributions, city & industry comparisons, top companies
- **Reports**: one-click PDF with 14+ visualizations

**Tech Stack:** Python, Flask, PostgreSQL, SQLAlchemy, pandas, matplotlib, Chart.js

---

## Screenshots

### Advanced Search Interface

![Search Interface](assets/screenshots/search-interface.png)

### Interactive Visualizations

![Visualization Dashboard](assets/screenshots/visualization-cards.png)

### PDF Report - Top Companies Analysis

![Top Companies](assets/screenshots/top-companies-pdf.png)

### Income Inequality Analysis (Lorenz Curve)

![Lorenz Curve](assets/screenshots/lorenz-curve.png)

## ⚙️ Features

**Web Application**

- Multi-criteria **search** with fuzzy text matching (PostgreSQL trigram)
- **Interactive charts** (Chart.js) with filters
- Export options for filtered datasets and charts

**Statistical Analysis**

- Inequality metrics: **Gini**, **Hoover**, **Atkinson**
- **Lorenz** curves, deciles, percentiles
- Automated **PDF report** with plots & summaries

**Data Pipeline**

- PDF parsing with `pdfplumber` + regex
- Cleaning, deduplication, normalization, error handling
- PostgreSQL schema optimized for analytics

---

## 🗄️ Database Schema

```sql
companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    activity_description TEXT,
    city VARCHAR(100)
);

employees (
    employee_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL
);

documents (
    document_id SERIAL PRIMARY KEY,
    filename VARCHAR(255) UNIQUE,
    company_id INTEGER REFERENCES companies,
    employee_count INTEGER,
    total_salary_mass DECIMAL(15,2)
);

salary_records (
    record_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees,
    company_id INTEGER REFERENCES companies,
    document_id INTEGER REFERENCES documents,
    salary_amount DECIMAL(10,2) NOT NULL
);
```

### 🔧 Optimizations

- Trigram indexes for fuzzy text search (company & employee names)
- B-tree indexes on foreign keys and salary amounts
- Materialized views for heavy aggregate queries

---

## 🛠️ Installation

### Prerequisites

- Python **3.8+**
- PostgreSQL **12+**
- `pip`

### Setup


#### Option A — Docker (recommended)

<pre class="overflow-visible!" data-start="1325" data-end="1755"><div class="contain-inline-size rounded-2xl relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-bash"><span><span># clone & enter the repo</span><span>
git </span><span>clone</span><span> https://github.com/<you>/cnss-salary-analysis.git
</span><span>cd</span><span> cnss-salary-analysis

</span><span># start Postgres + App (will build images on first run)</span><span>
docker compose up --build
</span><span># App → http://localhost:5000</span><span>

</span><span># (Optional) connect to DB from host:</span><span>
</span><span># If you mapped 5432:5432 -> psql -h localhost -p 5432 -U cnss_user -d cnss_db</span><span>
</span><span># If you mapped 5433:5432 -> psql -h localhost -p 5433 -U cnss_user -d cnss_db</span><span>
</span></span></code></div></div></pre>

> First run will auto-create schema and (optionally) seed data from `./sql/*.sql`.
>
> To **reset** DB: `docker compose down -v` then `docker compose up`.


#### Option B — Local (bare-metal)

```bash
# Clone repository
git clone https://github.com/hbarrada/cnss-salary-analysis.git
cd cnss-salary-analysis

# Virtual environment
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create DB (ensure PostgreSQL is running)
createdb cnss_db
psql cnss_db < sql/Tables.sql
psql cnss_db < sql/Indexes.sql
psql cnss_db < sql/Views.sql

# Environment variables
cp .env.example .env   # then edit .env with your credentials
```

### `.env` Example (matches `config.py`)

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cnss_db
DB_USER=your_username
DB_PASSWORD=your_password

FLASK_SECRET_KEY=your-secret-key-here
FLASK_ENV=development
```

### Optional: Load Sample Data

```bash
psql cnss_db < sql/sample_data.sql
```

### Run the App

```bash
python -m src.app
# then open http://localhost:5000
```

---

## 📑 Usage

**Generate Statistical Report**

```bash
python src/generate_report.py
```

Outputs: `visualizations/salary_analysis_report.pdf`

**Example Questions You Can Answer**

- Salary distribution in **Casablanca vs Rabat**
- Industries with the **highest average salaries**
- Share of income going to the **top 10%**
- Correlation between **company size** and salaries
- Current **Gini coefficient** for income inequality

---

## 🧭 Project Structure

```text
cnss-salary-analysis/
├─ src/
│  ├─ app.py
│  ├─ ...               # Flask routes, services, charts API
├─ sql/
│  ├─ Tables.sql
│  ├─ Indexes.sql
│  ├─ Views.sql
│  └─ sample_data.sql
├─ Data Processing/     # ETL scripts
├─ visualizations/      # Generated reports (PDF/PNG)
├─ assets/
│  └─ screenshots/      # UI screenshots used in README
├─ config.py
├─ requirements.txt
├─ README.md
└─ LICENSE
```

---

## 🎓 Project Context

This is a **personal project** demonstrating:

- End-to-end **data engineering** (ETL → DB → API → UI)
- **Statistical analysis** & inequality metrics
- **Full-stack development** (Flask + PostgreSQL + JS)
- **Interactive dashboards** for decision-making

## 🔒 Data ethics & privacy

* The repository ships **synthetic/anonymized** sample data only.
* Do **not** upload any real CNSS declarations or PII.
* Remove or hash all direct identifiers; audit logs are provided in ETL for traceability.

---

## 📜 License

MIT License — see [LICENSE](LICENSE)
