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

WITH first_name_stats AS (
  SELECT 
    SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1)) AS first_name,
    COUNT(*) AS person_count,
    AVG(salary_amount) AS avg_salary
  FROM 
    employees
    JOIN salary_records ON employees.employee_id = salary_records.employee_id
  GROUP BY 
    SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1))
  HAVING 
    COUNT(*) >= 20
)

SELECT 
  first_name,
  person_count,
  ROUND(avg_salary, 2) AS avg_salary
FROM 
  first_name_stats
WHERE
  first_name != ''
ORDER BY 
  avg_salary DESC
LIMIT 100;


WITH first_name_stats AS (
  SELECT 
    SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1)) AS first_name,
    COUNT(*) AS person_count,
    AVG(salary_amount) AS avg_salary
  FROM 
    employees
    JOIN salary_records ON employees.employee_id = salary_records.employee_id
  GROUP BY 
    SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1))
  HAVING 
    COUNT(*) >= 20
)

SELECT 
  first_name,
  person_count,
  ROUND(avg_salary, 2) AS avg_salary
FROM 
  first_name_stats
WHERE
  first_name != ''
ORDER BY 
  avg_salary ASC
LIMIT 100;

WITH 
salary_total AS (
  SELECT SUM(salary_amount) AS total_salary_mass
  FROM salary_records
),
salary_percentiles AS (
  SELECT 
    employee_id,
    salary_amount,
    PERCENT_RANK() OVER (ORDER BY salary_amount) as percentile
  FROM salary_records
)
SELECT 
  'Top 0.1%' as income_group,
  COUNT(*) as employee_count,
  SUM(salary_amount) as group_salary_mass,
  (SUM(salary_amount) / (SELECT total_salary_mass FROM salary_total)) * 100 as percentage_of_total
FROM salary_percentiles
WHERE percentile >= 0.999

UNION ALL

SELECT 
  'Top 1%' as income_group,
  COUNT(*) as employee_count,
  SUM(salary_amount) as group_salary_mass,
  (SUM(salary_amount) / (SELECT total_salary_mass FROM salary_total)) * 100 as percentage_of_total
FROM salary_percentiles
WHERE percentile >= 0.99

UNION ALL

SELECT 
  'Top 10%' as income_group,
  COUNT(*) as employee_count,
  SUM(salary_amount) as group_salary_mass,
  (SUM(salary_amount) / (SELECT total_salary_mass FROM salary_total)) * 100 as percentage_of_total
FROM salary_percentiles
WHERE percentile >= 0.9;

SELECT 
  COUNT(*) AS total_employees,
  AVG(salary_amount) AS mean_salary,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_amount) AS median_salary,
  AVG(salary_amount) / PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_amount) AS mean_to_median_ratio
FROM salary_records;

SELECT 
  SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1)) AS first_name,
  COUNT(*) AS person_count,
  ROUND(AVG(salary_amount), 2) AS avg_salary,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_amount)::numeric, 2) AS median_salary
FROM 
  employees
  JOIN salary_records ON employees.employee_id = salary_records.employee_id
WHERE 
  SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1)) IN ('HAMZA')
GROUP BY 
  SPLIT_PART(full_name, ' ', ARRAY_LENGTH(STRING_TO_ARRAY(full_name, ' '), 1))
ORDER BY 
  avg_salary DESC;
