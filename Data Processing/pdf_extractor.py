import os
import re
import pdfplumber
import pandas as pd
from tqdm import tqdm
import logging
import sys

# Completely redirect pdfminer's logging to null
logging.getLogger('pdfminer').setLevel(logging.ERROR)

# Create a custom stdout redirect to filter out specific messages
class FilteredStdout:
    def __init__(self, real_stdout):
        self.real_stdout = real_stdout
        self.suppress_patterns = ["CropBox missing from /Page", "MediaBox"]
    
    def write(self, text):
        if not any(pattern in text for pattern in self.suppress_patterns):
            self.real_stdout.write(text)
    
    def flush(self):
        self.real_stdout.flush()

# Apply the filter to stdout
sys.stdout = FilteredStdout(sys.stdout)
# Define compiled regex patterns for better performance
COMPANY_NAME_PATTERN = re.compile(r"Nom/Raison Sociale:\s*([^\n:]+)")
ACTIVITY_PATTERN = re.compile(r"Activité exercée:\s*([^\n:]+)")
CITY_PATTERN = re.compile(r"Ville:\s*([^\n:]+)")
EMPLOYEE_PATTERN = re.compile(r"(\d{9})\s+([^0-9]+)\s+(\d+)\s+([0-9\s.,]+)")
TOTAL_PATTERN = re.compile(r"TOTAL\s+([0-9\s.,]+)")

def clean_text(text):
    """Clean text by removing Arabic characters and extra whitespace"""
    # Remove Arabic characters (Unicode range for Arabic is U+0600 to U+06FF)
    text = re.sub(r'[\u0600-\u06FF]+', '', text)
    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def process_employee_page(text, results):
    """Extract employee data from a page"""
    employee_table_start = False
    
    for line in text.split('\n'):
        # Start collecting after seeing the table header
        if "N° d'immatriculation" in line and "Nom et prénom" in line and "Salaire déclaré" in line:
            employee_table_start = True
            continue
        
        if employee_table_start:
            # Stop when we hit a line with "TOTAL"
            if "TOTAL" in line:
                break
            
            # Try to extract employee data using regex pattern
            emp_match = EMPLOYEE_PATTERN.search(line)
            if emp_match:
                name = clean_text(emp_match.group(2))
                days = emp_match.group(3).strip()
                salary = emp_match.group(4).replace(" ", "").replace(",", ".")
                
                try:
                    salary_amount = float(salary)
                    employee = {
                        'full_name': name,
                        'salary_amount': salary_amount,
                        'days_worked': int(days)
                    }
                    results['employee_info'].append(employee)
                except ValueError:
                    continue

def extract_data_from_pdf(pdf_path):
    """
    Extract relevant data from a CNSS attestation PDF.
    
    Returns a dictionary with:
    - company_info: Information about the company
    - employee_info: List of employees and their salaries
    """
    results = {
        'company_info': {},
        'employee_info': []
    }
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Extract company info from the first page
            if len(pdf.pages) > 0:
                first_page_text = pdf.pages[0].extract_text() or ""
                
                # Extract company name
                company_match = COMPANY_NAME_PATTERN.search(first_page_text)
                if company_match:
                    company_name = clean_text(company_match.group(1))
                    results['company_info']['company_name'] = company_name
                
                # Extract activity
                activity_match = ACTIVITY_PATTERN.search(first_page_text)
                if activity_match:
                    activity = clean_text(activity_match.group(1))
                    results['company_info']['activity'] = activity
                
                # Extract city
                city_match = CITY_PATTERN.search(first_page_text)
                if city_match:
                    city = clean_text(city_match.group(1))
                    results['company_info']['city'] = city
            
            # Check for employee data in all pages (usually in last pages)
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if "N° d'immatriculation" in page_text and "Nom et prénom" in page_text:
                    process_employee_page(page_text, results)
            
            # Calculate employee count and total salary from the extracted employee data
            if results['employee_info']:
                results['company_info']['employee_count'] = len(results['employee_info'])
                results['company_info']['total_salary_mass'] = sum(emp['salary_amount'] for emp in results['employee_info'])
            else:
                # No employees found, mark with zero counts
                results['company_info']['employee_count'] = 0
                results['company_info']['total_salary_mass'] = 0.0
        
        return results
    
    except Exception as e:
        print(f"Error processing {os.path.basename(pdf_path)}: {str(e)[:100]}...")
        return results

def process_pdf_directory(directory_path, output_file=None):
    """
    Process all PDFs in a directory and collect the extracted data.
    
    Args:
        directory_path: Path to directory containing PDFs
        output_file: Optional path to save results as JSON
        
    Returns:
        List of dictionaries with collected data
    """
    all_data = []
    
    # Get list of all PDF files
    pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    
    # Determine progress reporting interval (log less for large datasets)
    print_interval = min(50, max(1, total_files // 20))
    
    print(f"Found {total_files} PDF files to process")
    
    # Process each PDF with progress tracking
    for i, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(directory_path, pdf_file)
        data = extract_data_from_pdf(pdf_path)
        
        # Add filename to data
        data['filename'] = pdf_file
        all_data.append(data)
        
        # Print progress occasionally
        if (i+1) % print_interval == 0 or i+1 == total_files:
            print(f"Processed {i+1}/{total_files} PDFs ({((i+1)/total_files)*100:.1f}%)")
    
    # Convert to DataFrame and save if requested
    if output_file:
        pd.DataFrame(all_data).to_json(output_file, orient='records')
        print(f"Data saved to {output_file}")
    
    return all_data

def test_extraction(pdf_path):
    """Test the PDF extraction on a single file"""
    print(f"Testing extraction on: {pdf_path}")
    data = extract_data_from_pdf(pdf_path)
    
    print("\nCompany Information:")
    for key, value in data['company_info'].items():
        print(f"  {key}: {value}")
    
    print(f"\nEmployees Found: {len(data['employee_info'])}")
    if data['employee_info']:
        print("Sample Employee Data:")
        for i, emp in enumerate(data['employee_info'][:5]):  # Show first 5 employees
            print(f"  Employee {i+1}: {emp['full_name']} - {emp['salary_amount']} MAD")
    
    print("\nExtraction test completed.")

if __name__ == "__main__":
    # Test on a single file
    test_pdf = "path/to/your/test.pdf"  # Replace with your file path
    
    if os.path.exists(test_pdf):
        test_extraction(test_pdf)
    else:
        print(f"Error: Test file not found: {test_pdf}")
        print("Please provide a valid PDF path.")