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
LIMIT 20;


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
LIMIT 20;

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
