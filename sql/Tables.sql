-- Table for storing company information
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    activity_description TEXT,
    city VARCHAR(100)
);

-- Table for storing document references
CREATE TABLE documents (
    document_id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    company_id INTEGER REFERENCES companies(company_id),
    employee_count INTEGER,
    total_salary_mass DECIMAL(15, 2)
);

-- Table for storing employee information
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL
);

-- Table for storing salary records
CREATE TABLE salary_records (
    record_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    company_id INTEGER REFERENCES companies(company_id),
    document_id INTEGER REFERENCES documents(document_id),
    salary_amount DECIMAL(15, 2)
);

