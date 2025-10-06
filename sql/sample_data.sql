-- Sample Data for CNSS Salary Analysis System
-- This generates realistic fake data for demonstration purposes

-- Clear existing data
TRUNCATE TABLE salary_records, documents, employees, companies RESTART IDENTITY CASCADE;

-- Insert sample companies (20 companies)
INSERT INTO companies (company_name, activity_description, city) VALUES
('TechCorp Morocco', 'Software Development and IT Services', 'Casablanca'),
('Atlas Industries', 'Manufacturing and Industrial Equipment', 'Tangier'),
('Sahara Consulting', 'Business Consulting Services', 'Rabat'),
('MedPharma SA', 'Pharmaceutical Sales and Distribution', 'Casablanca'),
('Atlas Bank', 'Banking and Financial Services', 'Rabat'),
('Maroc Telecom Services', 'Telecommunications', 'Rabat'),
('Royal Construction', 'Building and Construction', 'Marrakech'),
('Ocean Exports', 'Import/Export Trading', 'Casablanca'),
('Green Energy Solutions', 'Renewable Energy', 'Tangier'),
('Maghreb Airlines', 'Aviation and Transport', 'Casablanca'),
('DataTech Solutions', 'Data Analytics and AI', 'Rabat'),
('Morocco Motors', 'Automotive Sales and Service', 'Casablanca'),
('Atlas Mining Corp', 'Mining and Natural Resources', 'Marrakech'),
('Casablanca Hotels', 'Hospitality and Tourism', 'Casablanca'),
('Textile Maroc', 'Textile Manufacturing', 'Fes'),
('FoodDistro SA', 'Food Distribution', 'Tangier'),
('Cyber Security MA', 'Information Security Services', 'Rabat'),
('Atlas Insurance', 'Insurance Services', 'Casablanca'),
('Morocco Steel', 'Steel Manufacturing', 'Kenitra'),
('Digital Marketing Pro', 'Marketing and Advertising', 'Casablanca');

-- Insert sample employees (100 fake names)
INSERT INTO employees (full_name) VALUES
('Ahmed El Fassi'), ('Fatima Zahra Alami'), ('Mohammed Benjelloun'), ('Amina Tazi'),
('Youssef El Idrissi'), ('Laila Bennis'), ('Karim Chraibi'), ('Sanaa El Amrani'),
('Omar Sefrioui'), ('Nadia Berrada'), ('Hassan El Guerrouj'), ('Rajae Benkirane'),
('Rachid Bouhia'), ('Samira Zouiten'), ('Mehdi Taarji'), ('Houda Benslimane'),
('Abdelaziz Lahlou'), ('Khadija Mernissi'), ('Ismail Bennani'), ('Souad Slaoui'),
('Said Naciri'), ('Wafa Benomar'), ('Tarik Alaoui'), ('Latifa Aziz'),
('Mustapha Khalil'), ('Zineb Fassi Fihri'), ('Driss Kettani'), ('Malika Benali'),
('Aziz El Omari'), ('Jamila Sebbar'), ('Hicham Belmahi'), ('Nawal Lamrani'),
('Khalid Touzani'), ('Meriem Drissi'), ('Fouad Yacoubi'), ('Salma Bennani'),
('Adil Boukhari'), ('Ghizlane Berrada'), ('Nabil Cherkaoui'), ('Loubna Mansouri'),
('Mounir Hajji'), ('Amal Belkhadir'), ('Brahim Benabdellah'), ('Hanane Alami'),
('Hamza Salhi'), ('Imane Hakim'), ('Abderrahim Ziani'), ('Nora Benkirane'),
('Yassine El Ghazi'), ('Btissam Chakir'), ('Zakaria Ouazzani'), ('Siham Bennani'),
('Amine Rami'), ('Karima El Mouden'), ('Hossam Idrissi'), ('Samia Lahlou'),
('Jalal Boujemaa'), ('Hafsa Tazi'), ('Ilyas Bennani'), ('Dounia El Amrani'),
('Rachid Chakir'), ('Nabila Bouzid'), ('Othmane Berrada'), ('Aicha El Kadiri'),
('Samir Bennani'), ('Leila Hamdaoui'), ('Bilal Benjelloun'), ('Fatiha Alaoui'),
('Redouane Slaoui'), ('Hafida Naciri'), ('Kamal Taarji'), ('Mouna El Fassi'),
('Jamal Berrada'), ('Bahija Bennis'), ('Noura Bennani'), ('Abdelilah Chraibi'),
('Kawtar Alami'), ('Sami Kettani'), ('Meryem El Omari'), ('Farid Lamrani'),
('Ibtissam Benslimane'), ('Anas Bouhia'), ('Sabah Bennani'), ('Adnane Tazi'),
('Rim El Amrani'), ('Anass Benomar'), ('Najwa Idrissi'), ('Charaf Bennani'),
('Loubna Zouiten'), ('Noureddine Hajji'), ('Siham Lahlou'), ('Mehdi Alaoui'),
('Fatima Bennani'), ('Karim Berrada'), ('Naima El Fassi'), ('Imad Chraibi'),
('Salima Tazi'), ('Hakim Bennani'), ('Widad Alami'), ('Zakaria Lahlou');

-- Insert sample documents
INSERT INTO documents (filename, company_id, employee_count, total_salary_mass) 
SELECT 
    'CNSS_' || c.company_id || '_2024_01.pdf',
    c.company_id,
    CASE 
        WHEN c.company_id <= 5 THEN 15 + floor(random() * 35)::int  -- Large companies: 15-50 employees
        WHEN c.company_id <= 15 THEN 5 + floor(random() * 15)::int  -- Medium companies: 5-20 employees
        ELSE 2 + floor(random() * 8)::int  -- Small companies: 2-10 employees
    END,
    0  -- Will be updated
FROM companies c;

-- Insert salary records with realistic distribution
-- We'll assign employees to companies and give them salaries
WITH company_employee_assignments AS (
    SELECT 
        c.company_id,
        e.employee_id,
        ROW_NUMBER() OVER (PARTITION BY c.company_id ORDER BY random()) as emp_num,
        d.employee_count,
        d.document_id
    FROM companies c
    CROSS JOIN employees e
    JOIN documents d ON c.company_id = d.company_id
),
filtered_assignments AS (
    SELECT * FROM company_employee_assignments WHERE emp_num <= employee_count
)
INSERT INTO salary_records (employee_id, company_id, document_id, salary_amount)
SELECT 
    employee_id,
    company_id,
    document_id,
    CASE 
        -- Tech/Finance/Pharma companies (companies 1,4,5,6,11,17): Higher salaries
        WHEN company_id IN (1,4,5,6,11,17) THEN
            CASE 
                WHEN random() < 0.15 THEN 50000 + random() * 100000  -- 15% executives
                WHEN random() < 0.40 THEN 15000 + random() * 35000   -- 25% senior
                WHEN random() < 0.75 THEN 8000 + random() * 12000    -- 35% mid-level
                ELSE 4000 + random() * 6000                          -- 25% junior
            END
        
        -- Manufacturing/Construction (2,7,13,15,19): Medium salaries
        WHEN company_id IN (2,7,13,15,19) THEN
            CASE 
                WHEN random() < 0.10 THEN 30000 + random() * 50000
                WHEN random() < 0.30 THEN 12000 + random() * 18000
                WHEN random() < 0.70 THEN 6000 + random() * 8000
                ELSE 3500 + random() * 4500
            END
        
        -- Service/Retail companies: Lower salaries
        ELSE
            CASE 
                WHEN random() < 0.10 THEN 20000 + random() * 30000
                WHEN random() < 0.30 THEN 8000 + random() * 12000
                WHEN random() < 0.70 THEN 4500 + random() * 5500
                ELSE 3000 + random() * 3000
            END
    END::numeric(10,2)
FROM filtered_assignments;

-- Update total_salary_mass in documents
UPDATE documents d
SET total_salary_mass = (
    SELECT COALESCE(SUM(salary_amount), 0)
    FROM salary_records s
    WHERE s.document_id = d.document_id
);

-- Verify data
SELECT 'Companies:', COUNT(*) FROM companies
UNION ALL
SELECT 'Employees:', COUNT(*) FROM employees
UNION ALL
SELECT 'Documents:', COUNT(*) FROM documents
UNION ALL
SELECT 'Salary Records:', COUNT(*) FROM salary_records;

-- Show sample statistics
SELECT 
    'Avg Salary' as metric,
    ROUND(AVG(salary_amount), 2) as value
FROM salary_records
UNION ALL
SELECT 
    'Median Salary',
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_amount)
FROM salary_records
UNION ALL
SELECT 
    'Max Salary',
    MAX(salary_amount)
FROM salary_records;

