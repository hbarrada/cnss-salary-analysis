import os
import psycopg2
from tqdm import tqdm
import time

# Import your extraction function
from pdf_extractor import extract_data_from_pdf

def connect_to_db():
    """Establish a connection to the PostgreSQL database"""
    conn = psycopg2.connect(
        dbname="cnss_db",
        user="postgres",
        password="bky2002bky",  # Replace with your actual password
        host="localhost"
    )
    return conn

def import_data_to_db(data_list, log_only_company=False):
    """
    Import extracted PDF data into PostgreSQL database.
    
    Args:
        data_list: List of dictionaries from PDF extraction.
        log_only_company: If True, only inserts company and document info (skip employee and salary inserts).
    """
    if not data_list:
        print("⚠️ No data to import.")
        return

    conn = connect_to_db()
    cursor = conn.cursor()

    company_cache = {}
    employee_cache = {}

    total_imported = 0
    error_count = 0
    zero_employee_count = 0
    start_time = time.time()

    for item in tqdm(data_list, desc="Importing data"):
        try:
            company_info = item['company_info']
            employee_info = item['employee_info']
            filename = item['filename']

            if 'company_name' not in company_info or not company_info['company_name']:
                print(f"❌ Skipping {filename}: No company name found")
                continue

            company_name = company_info['company_name']

            # Check if company already exists
            if company_name in company_cache:
                company_id = company_cache[company_name]
            else:
                cursor.execute("""
                    SELECT company_id FROM companies WHERE company_name = %s
                """, (company_name,))
                result = cursor.fetchone()
                if result:
                    company_id = result[0]
                else:
                    cursor.execute("""
                        INSERT INTO companies (company_name, activity_description, city)
                        VALUES (%s, %s, %s) RETURNING company_id
                    """, (
                        company_name,
                        company_info.get('activity'),
                        company_info.get('city')
                    ))
                    company_id = cursor.fetchone()[0]

                company_cache[company_name] = company_id

            # Insert document even if no employees
            employee_count = company_info.get('employee_count', 0)
            total_salary_mass = round(company_info.get('total_salary_mass', 0.0), 2)

            cursor.execute("""
                INSERT INTO documents (filename, company_id, employee_count, total_salary_mass)
                VALUES (%s, %s, %s, %s) RETURNING document_id
            """, (filename, company_id, employee_count, total_salary_mass))
            document_id = cursor.fetchone()[0]

            if employee_count == 0:
                zero_employee_count += 1
                print(f"ℹ️ {filename}: 0 employees declared.")
            elif not log_only_company:
                for emp in employee_info:
                    emp_name = emp.get('full_name')
                    if not emp_name:
                        continue

                    cache_key = f"{emp_name}_{company_id}"
                    if cache_key in employee_cache:
                        employee_id = employee_cache[cache_key]
                    else:
                        cursor.execute("""
                            SELECT e.employee_id
                            FROM employees e
                            JOIN salary_records s ON e.employee_id = s.employee_id
                            WHERE e.full_name = %s AND s.company_id = %s
                            LIMIT 1
                        """, (emp_name, company_id))
                        result = cursor.fetchone()
                        if result:
                            employee_id = result[0]
                        else:
                            cursor.execute("""
                                INSERT INTO employees (full_name)
                                VALUES (%s) RETURNING employee_id
                            """, (emp_name,))
                            employee_id = cursor.fetchone()[0]

                        employee_cache[cache_key] = employee_id

                    # Insert salary record
                    cursor.execute("""
                        INSERT INTO salary_records (employee_id, company_id, document_id, salary_amount)
                        VALUES (%s, %s, %s, %s)
                    """, (employee_id, company_id, document_id, emp['salary_amount']))

            conn.commit()
            total_imported += 1
            print(f"✔ Imported: {filename} | Company: {company_name} | Employees: {employee_count}")

        except Exception as e:
            conn.rollback()
            error_count += 1
            print(f"❌ Error processing {filename}: {str(e)[:200]}")

    elapsed = time.time() - start_time
    print("\n✅ Import Summary:")
    print(f"   ✔ Total Imported: {total_imported}")
    print(f"   ⚠️ Errors: {error_count}")
    print(f"   🚫 Zero employee files: {zero_employee_count}")
    print(f"⏱️ Total Time: {elapsed:.1f} seconds")

    cursor.close()
    conn.close()


def get_processed_files(conn=None):
    """Get a set of already processed filenames from the database"""
    close_conn = False
    if conn is None:
        conn = connect_to_db()
        close_conn = True
    
    cursor = conn.cursor()
    processed_files = set()
    
    try:
        cursor.execute("SELECT filename FROM documents")
        processed_files = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error retrieving processed files: {e}")
    finally:
        cursor.close()
        if close_conn:
            conn.close()
            
    return processed_files

def process_pdf_directory(directory_path, skip_processed=True):
    """
    Process all PDFs in a directory and collect the extracted data.
    
    Args:
        directory_path: Path to directory containing PDFs
        skip_processed: Whether to skip files already in the database
        
    Returns:
        List of dictionaries with collected data
    """
    all_data = []
    
    # Get list of all PDF files
    pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    
    print(f"Found {total_files} PDF files in directory")
    
    # If skipping processed files, filter the list
    processed_files = set()
    if skip_processed:
        processed_files = get_processed_files()
        print(f"Found {len(processed_files)} already processed files in database")
        pdf_files = [f for f in pdf_files if f not in processed_files]
        print(f"Remaining files to process: {len(pdf_files)}")
    
    if not pdf_files:
        print("No new files to process")
        return all_data
    
    # Determine progress reporting interval
    print_interval = min(50, max(1, len(pdf_files) // 20))
    
    # Track processing time
    start_time = time.time()
    
    # Process each PDF with occasional progress updates
    for i, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(directory_path, pdf_file)
        data = extract_data_from_pdf(pdf_path)
        
        # Add filename to data
        data['filename'] = pdf_file
        all_data.append(data)
        
        # Print progress occasionally
        if (i+1) % print_interval == 0 or i+1 == len(pdf_files):
            elapsed = time.time() - start_time
            speed = (i+1) / elapsed if elapsed > 0 else 0
            print(f"Processed {i+1}/{len(pdf_files)} PDFs ({((i+1)/len(pdf_files))*100:.1f}%) - {speed:.1f} PDFs/sec")
    
    print(f"PDF processing completed: {len(all_data)} files in {time.time() - start_time:.1f} seconds")
    return all_data

def process_and_import(pdf_directory, skip_processed=True):
    """Process all PDFs and import to database, with option to skip processed files"""
    print(f"Starting processing of PDFs from: {pdf_directory}")
    
    total_start_time = time.time()
    
    # Extract data from PDFs, skipping already processed files if requested
    print("Step 1: Extracting data from PDFs...")
    data_list = process_pdf_directory(pdf_directory, skip_processed)
    
    # Import to database if we have any new data
    if data_list:
        print("\nStep 2: Importing data to database...")
        import_data_to_db(data_list)
    else:
        print("\nNo new data to import")
    
    # Report total processing time
    total_time = time.time() - total_start_time
    print(f"\nTotal processing completed in {total_time:.1f} seconds")

if __name__ == "__main__":
    pdf_dir = "C:/CNSS/Companies"  # Replace with your PDF directory
    
    if os.path.exists(pdf_dir):
        # Set to True to skip files already in the database (default)
        process_and_import(pdf_dir, skip_processed=True)
    else:
        print(f"Error: Directory not found: {pdf_dir}")
        print("Please create the directory and add your PDF files.")