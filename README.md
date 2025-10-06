# CNSS Salary Data Analysis System

A full-stack data engineering and visualization platform for analyzing salary statistics. Demonstrates ETL pipeline design, statistical analysis, and interactive web-based data visualization.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

## Overview

This project showcases a complete data analysis pipeline built to process and visualize salary data from CNSS declarations:

- Automated PDF data extraction and cleaning
- PostgreSQL database with optimized indexing
- Flask web application with interactive search and filtering
- Statistical analysis including Gini coefficient, Lorenz curves, and percentile analysis
- Automated PDF report generation with 14+ visualizations

**Tech Stack:** Python, Flask, PostgreSQL, SQLAlchemy, pandas, matplotlib, Chart.js

## Features

### Web Application

- **Advanced Search**: Multi-criteria filtering with full-text fuzzy matching using PostgreSQL trigram indexes
- **Interactive Charts**: Real-time visualizations with Chart.js
  - Salary distribution histograms
  - Geographic comparisons
  - Industry sector analysis
  - Company rankings
- **Export Options**: Download filtered data and charts

### Statistical Analysis

- **Income Inequality Metrics**: Gini coefficient, Hoover index, Atkinson index
- **Distribution Analysis**: Lorenz curves, percentile breakdowns (P10-P99.9), decile analysis
- **Automated Reports**: PDF generation with comprehensive visualizations and statistical summaries

### Data Pipeline

- PDF parsing with `pdfplumber` and regex pattern matching
- Data cleaning and normalization (text cleanup, deduplication)
- Batch processing with error handling and progress tracking
- Optimized PostgreSQL schema with trigram and B-tree indexes

## Database Schema

```sql
companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    activity_description TEXT,
    city VARCHAR(100)
)

employees (
    employee_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL
)

documents (
    document_id SERIAL PRIMARY KEY,
    filename VARCHAR(255) UNIQUE,
    company_id INTEGER REFERENCES companies,
    employee_count INTEGER,
    total_salary_mass DECIMAL(15,2)
)

salary_records (
    record_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees,
    company_id INTEGER REFERENCES companies,
    document_id INTEGER REFERENCES documents,
    salary_amount DECIMAL(10,2) NOT NULL
)
```

**Optimizations:**

- Trigram indexes for fuzzy text search on company and employee names
- B-tree indexes on foreign keys and salary amounts
- Materialized views for complex aggregate queries

## Installation

### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- pip

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/cnss-salary-analysis.git
cd cnss-salary-analysis

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up database
createdb cnss_db
psql cnss_db < sql/Tables.sql
psql cnss_db < sql/Indexes.sql
psql cnss_db < sql/Views.sql

# Configure environment variables
cp .env.example .env
# Edit .env with your database credentials

# Load sample data (optional)
psql cnss_db < sql/sample_data.sql

# Run application
python src/app.py
```

Visit `http://localhost:5000`

### Environment Variables

Create a `.env` file with the following:

```env
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=cnss_db
DATABASE_USER=your_username
DATABASE_PASSWORD=your_password
FLASK_SECRET_KEY=your-secret-key-here
FLASK_ENV=development
```

## Usage

### Generate Statistical Report

```bash
python src/generate_report.py
```

Outputs a comprehensive PDF report to `visualizations/salary_analysis_report.pdf`

### Web Interface

1. **Search Tab**: Query database with multiple filters (company, employee, city, activity, salary range)
2. **Visualization Tab**: Generate dynamic charts based on filtered data

### Example Queries

The system can answer analytical questions like:

- What's the salary distribution in Casablanca vs Rabat?
- Which industries have the highest average salaries?
- What percentage of income goes to the top 10%?
- How does company size correlate with average salary?
- What is the current Gini coefficient for income inequality?

## Project Structure

```
span
```

## Technical Highlights

This project demonstrates:

- **Full-Stack Development**: Backend (Flask/PostgreSQL) + Frontend (HTML/CSS/JavaScript)
- **Data Engineering**: ETL pipeline design, database normalization, query optimization
- **Statistical Analysis**: Income inequality metrics, percentile analysis, distribution studies
- **Data Visualization**: Interactive charts (Chart.js) and static reports (matplotlib/seaborn)
- **Production Practices**: Environment management, proper project structure, comprehensive documentation
- **Database Optimization**: Strategic indexing, trigram search, materialized views

## License

MIT License - see [LICENSE](LICENSE) file for details.
