-- View for company summary information
-- Updated company_summary view with COALESCE and decimal precision
CREATE OR REPLACE VIEW company_summary AS
SELECT 
    c.company_id,
    c.company_name,
    c.activity_description,
    c.city,
    COUNT(DISTINCT s.employee_id) AS employee_count,
    ROUND(COALESCE(AVG(s.salary_amount), 0), 2) AS average_salary,
    COALESCE(MAX(s.salary_amount), 0) AS max_salary,
    COALESCE(MIN(s.salary_amount), 0) AS min_salary
FROM companies c
LEFT JOIN salary_records s ON c.company_id = s.company_id
GROUP BY c.company_id, c.company_name, c.activity_description, c.city;

-- View for top earners
CREATE OR REPLACE VIEW top_earners AS
SELECT 
    e.employee_id,
    e.full_name,
    c.company_id,
    c.company_name,
    c.activity_description,
    c.city,
    s.salary_amount,
    d.filename
FROM employees e
JOIN salary_records s ON e.employee_id = s.employee_id
JOIN companies c ON s.company_id = c.company_id
JOIN documents d ON s.document_id = d.document_id
ORDER BY s.salary_amount DESC;

-- City-based salary statistics
CREATE OR REPLACE VIEW city_salary_stats AS
SELECT 
    city,
    COUNT(DISTINCT c.company_id) AS company_count,
    COUNT(DISTINCT s.employee_id) AS employee_count,
    AVG(s.salary_amount) AS average_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) AS median_salary,
    MAX(s.salary_amount) AS max_salary
FROM companies c
JOIN salary_records s ON c.company_id = s.company_id
GROUP BY city;

-- Activity-based salary statistics
CREATE OR REPLACE VIEW activity_salary_stats AS
SELECT 
    activity_description,
    COUNT(DISTINCT c.company_id) AS company_count,
    COUNT(DISTINCT s.employee_id) AS employee_count,
    AVG(s.salary_amount) AS average_salary,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) AS median_salary
FROM companies c
JOIN salary_records s ON c.company_id = s.company_id
GROUP BY activity_description;

SELECT 
    e.employee_id,
    e.full_name,
    c.company_name,
    c.activity_description,
    c.city,
    d.filename,
    s.salary_amount
FROM salary_records s
JOIN employees e ON s.employee_id = e.employee_id
JOIN companies c ON s.company_id = c.company_id
JOIN documents d ON s.document_id = d.document_id
WHERE LOWER(c.activity_description) LIKE LOWER('%journal%')  -- change this to any name fragment
ORDER BY s.salary_amount DESC;

SELECT company_id, company_name, activity_description, city
FROM companies
WHERE company_name ILIKE '%specific_name%' 
   OR activity_description ILIKE '%specific_activity%';



SELECT 
    e.full_name,
    c.company_name,
    c.city,
    s.salary_amount,
    d.filename
FROM salary_records s
JOIN employees e ON s.employee_id = e.employee_id
JOIN companies c ON s.company_id = c.company_id
JOIN documents d ON s.document_id = d.document_id
WHERE s.salary_amount BETWEEN 50000 AND 100000  -- change range as needed
ORDER BY s.salary_amount DESC;
