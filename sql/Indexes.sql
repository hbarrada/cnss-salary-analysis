-- Enable text search extension for partial matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- For case-insensitive searching of company names
CREATE INDEX idx_company_name ON companies USING gin (lower(company_name) gin_trgm_ops);

-- For case-insensitive searching of activity
CREATE INDEX idx_activity ON companies USING gin (lower(activity_description) gin_trgm_ops); 

-- For city filtering
CREATE INDEX idx_company_city ON companies(city);

-- For employee name searching
CREATE INDEX idx_employee_name ON employees USING gin (lower(full_name) gin_trgm_ops);

-- For salary filtering
CREATE INDEX idx_salary_amount ON salary_records(salary_amount);

-- For efficient joins
CREATE INDEX idx_document_company ON documents(company_id);
CREATE INDEX idx_salary_employee ON salary_records(employee_id);
CREATE INDEX idx_salary_company ON salary_records(company_id);
CREATE INDEX idx_salary_document ON salary_records(document_id);
