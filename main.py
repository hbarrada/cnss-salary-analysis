import os
from flask import Flask, render_template, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import json

app = Flask(__name__)

def connect_to_db():
    """Establish a connection to the PostgreSQL database"""
    conn = psycopg2.connect(
        dbname="CNSS",
        user="postgres",
        password="yourpassword", 
        host="localhost",
        port="5432"
    )
    return conn

@app.route('/')
def index():
    """Render the main search page"""
    # Get city options for dropdown only
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Get all cities
    cursor.execute("SELECT DISTINCT city FROM companies WHERE city IS NOT NULL ORDER BY city")
    cities = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return render_template('index.html', cities=cities)

@app.route('/api/search', methods=['POST'])
def search():
    """API endpoint to search the database based on criteria"""
    data = request.json
    
    # Extract search parameters
    company_name = data.get('company_name', '')
    employee_name = data.get('employee_name', '')
    city = data.get('city', '')
    activity = data.get('activity', '')
    min_salary = data.get('min_salary', 0)
    max_salary = data.get('max_salary', 1000000000)
    limit = int(data.get('limit', 100))  # Use actual limit, no cap
    
    # Build query conditions
    conditions = []
    params = []
    
    if company_name:
        conditions.append("LOWER(c.company_name) LIKE LOWER(%s)")
        params.append(f"%{company_name}%")
    
    if employee_name:
        conditions.append("LOWER(e.full_name) LIKE LOWER(%s)")
        params.append(f"%{employee_name}%")
    
    if city:
        conditions.append("LOWER(c.city) LIKE LOWER(%s)")
        params.append(f"%{city}%")
    
    if activity:
        conditions.append("LOWER(c.activity_description) LIKE LOWER(%s)")
        params.append(f"%{activity}%")
    
    conditions.append("s.salary_amount BETWEEN %s AND %s")
    params.extend([min_salary, max_salary])
    
    # Combine conditions
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # Create query
    query = f"""
        SELECT 
            e.employee_id,
            e.full_name,
            c.company_id,
            c.company_name,
            c.activity_description,
            c.city,
            s.salary_amount,
            d.filename
        FROM salary_records s
        JOIN employees e ON s.employee_id = e.employee_id
        JOIN companies c ON s.company_id = c.company_id
        JOIN documents d ON s.document_id = d.document_id
        WHERE {where_clause}
        ORDER BY s.salary_amount DESC
        LIMIT %s
    """
    params.append(limit)
    
    # Execute query
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Format salary values for display
    for result in results:
        result['salary_amount'] = float(result['salary_amount'])
    
    cursor.close()
    conn.close()
    
    return jsonify({"results": results})

@app.route('/api/stats', methods=['POST'])
def get_stats():
    """Get statistics based on search criteria for visualization"""
    data = request.json
    
    # Extract filter parameters (same as search)
    company_name = data.get('company_name', '')
    employee_name = data.get('employee_name', '')
    city = data.get('city', '')
    activity = data.get('activity', '')
    min_salary = data.get('min_salary', 0)
    max_salary = data.get('max_salary', 1000000000)
    
    # Build query conditions
    conditions = []
    params = []
    
    if company_name:
        conditions.append("LOWER(c.company_name) LIKE LOWER(%s)")
        params.append(f"%{company_name}%")
    
    if employee_name:
        conditions.append("LOWER(e.full_name) LIKE LOWER(%s)")
        params.append(f"%{employee_name}%")
    
    if city:
        conditions.append("LOWER(c.city) LIKE LOWER(%s)")
        params.append(f"%{city}%")
    
    if activity:
        conditions.append("LOWER(c.activity_description) LIKE LOWER(%s)")
        params.append(f"%{activity}%")
    
    conditions.append("s.salary_amount BETWEEN %s AND %s")
    params.extend([min_salary, max_salary])
    
    # Combine conditions
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get salary distribution by city
    city_query = f"""
        SELECT 
            c.city, 
            COUNT(DISTINCT s.employee_id) as employee_count,
            AVG(s.salary_amount) as avg_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) as median_salary,
            MAX(s.salary_amount) as max_salary
        FROM salary_records s
        JOIN employees e ON s.employee_id = e.employee_id
        JOIN companies c ON s.company_id = c.company_id
        JOIN documents d ON s.document_id = d.document_id
        WHERE {where_clause} AND c.city IS NOT NULL
        GROUP BY c.city
        ORDER BY avg_salary DESC
        LIMIT 20
    """
    cursor.execute(city_query, params)
    city_stats = cursor.fetchall()
    
    # Get salary distribution by activity
    activity_query = f"""
        SELECT 
            c.activity_description, 
            COUNT(DISTINCT s.employee_id) as employee_count,
            AVG(s.salary_amount) as avg_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) as median_salary
        FROM salary_records s
        JOIN employees e ON s.employee_id = e.employee_id
        JOIN companies c ON s.company_id = c.company_id
        JOIN documents d ON s.document_id = d.document_id
        WHERE {where_clause} AND c.activity_description IS NOT NULL
        GROUP BY c.activity_description
        ORDER BY avg_salary DESC
        LIMIT 20
    """
    cursor.execute(activity_query, params)
    activity_stats = cursor.fetchall()
    
    # Get salary distribution
    salary_buckets_query = f"""
        WITH salary_ranges AS (
            SELECT 
                CASE
                    WHEN s.salary_amount < 5000 THEN '< 5K'
                    WHEN s.salary_amount < 10000 THEN '5K-10K'
                    WHEN s.salary_amount < 15000 THEN '10K-15K'
                    WHEN s.salary_amount < 20000 THEN '15K-20K'
                    WHEN s.salary_amount < 30000 THEN '20K-30K'
                    WHEN s.salary_amount < 50000 THEN '30K-50K'
                    WHEN s.salary_amount < 100000 THEN '50K-100K'
                    WHEN s.salary_amount < 200000 THEN '100K-200K'
                    WHEN s.salary_amount < 500000 THEN '200K-500K'
                    WHEN s.salary_amount < 1000000 THEN '500K-1M'
                    ELSE '1M+'
                END AS salary_range,
                COUNT(*) as count
            FROM salary_records s
            JOIN employees e ON s.employee_id = e.employee_id
            JOIN companies c ON s.company_id = c.company_id
            JOIN documents d ON s.document_id = d.document_id
            WHERE {where_clause}
            GROUP BY salary_range
        )
        SELECT * FROM salary_ranges
        ORDER BY 
            CASE salary_range
                WHEN '< 5K' THEN 1
                WHEN '5K-10K' THEN 2
                WHEN '10K-15K' THEN 3
                WHEN '15K-20K' THEN 4
                WHEN '20K-30K' THEN 5
                WHEN '30K-50K' THEN 6
                WHEN '50K-100K' THEN 7
                WHEN '100K-200K' THEN 8
                WHEN '200K-500K' THEN 9
                WHEN '500K-1M' THEN 10
                WHEN '1M+' THEN 11
            END
    """
    cursor.execute(salary_buckets_query, params)
    salary_distribution = cursor.fetchall()
    
    # Get top companies by avg salary
    top_companies_query = f"""
        SELECT 
            c.company_name,
            c.city,
            c.activity_description,
            COUNT(DISTINCT s.employee_id) as employee_count,
            AVG(s.salary_amount) as avg_salary,
            MAX(s.salary_amount) as max_salary
        FROM salary_records s
        JOIN employees e ON s.employee_id = e.employee_id
        JOIN companies c ON s.company_id = c.company_id
        JOIN documents d ON s.document_id = d.document_id
        WHERE {where_clause}
        GROUP BY c.company_name, c.city, c.activity_description
        HAVING COUNT(DISTINCT s.employee_id) >= 3
        ORDER BY avg_salary DESC
        LIMIT 20
    """
    cursor.execute(top_companies_query, params)
    top_companies = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Format decimal values for JSON
    for stat in city_stats:
        for key in ['avg_salary', 'median_salary', 'max_salary']:
            if key in stat and stat[key] is not None:
                stat[key] = float(stat[key])
    
    for stat in activity_stats:
        for key in ['avg_salary', 'median_salary']:
            if key in stat and stat[key] is not None:
                stat[key] = float(stat[key])
    
    for company in top_companies:
        for key in ['avg_salary', 'max_salary']:
            if key in company and company[key] is not None:
                company[key] = float(company[key])
    
    return jsonify({
        "city_stats": city_stats,
        "activity_stats": activity_stats,
        "salary_distribution": salary_distribution,
        "top_companies": top_companies
    })

if __name__ == "__main__":
    # Create templates directory if it doesn't exist
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    # Create static directory if it doesn't exist
    if not os.path.exists('static'):
        os.makedirs('static')
    
    # Create CSS file
    with open('static/style.css', 'w') as f:
        f.write("""
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f5f5f5;
            color: #333;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        header {
            background-color: #2c3e50;
            color: white;
            padding: 1rem;
            text-align: center;
            margin-bottom: 2rem;
        }
        
        h1 {
            margin: 0;
        }
        
        .search-form {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
        }
        
        .form-row {
            display: flex;
            flex-wrap: wrap;
            margin-bottom: 15px;
            gap: 15px;
        }
        
        .form-group {
            flex: 1;
            min-width: 200px;
        }
        
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: 600;
        }
        
        input, select {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        
        button {
            background-color: #2c3e50;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        
        button:hover {
            background-color: #1a252f;
        }
        
        .results-container {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            margin-bottom: 30px;
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        
        th {
            background-color: #f2f2f2;
            position: sticky;
            top: 0;
        }
        
        tr:hover {
            background-color: #f5f5f5;
        }
        
        .charts-container {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .chart-box {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        
        .chart-title {
            margin-top: 0;
            margin-bottom: 15px;
            text-align: center;
            color: #2c3e50;
        }
        
        .tabs {
            display: flex;
            margin-bottom: 20px;
            border-bottom: 1px solid #ddd;
        }
        
        .tab {
            padding: 10px 20px;
            cursor: pointer;
            border: 1px solid transparent;
            border-bottom: none;
            margin-right: 5px;
        }
        
        .tab.active {
            background-color: white;
            border-color: #ddd;
            border-radius: 5px 5px 0 0;
            font-weight: bold;
        }
        
        .tab-content {
            display: none;
        }
        
        .tab-content.active {
            display: block;
        }
        
        .loading {
            text-align: center;
            padding: 20px;
        }
        
        .loading::after {
            content: "Loading...";
            font-style: italic;
            color: #666;
        }
        
        /* Better dropdown styling */
        select {
            max-height: 200px;
        }
        
        select option {
            padding: 5px;
        }
        
        /* Limit display styling */
        .limit-options {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        
        .limit-btn {
            background-color: #ecf0f1;
            color: #2c3e50;
            border: 1px solid #bdc3c7;
            padding: 5px 10px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
        }
        
        .limit-btn:hover {
            background-color: #d5dbdb;
        }
        
        .limit-btn.active {
            background-color: #2c3e50;
            color: white;
        }
        """)
    
    # Create HTML template
    with open('templates/index.html', 'w') as f:
        f.write("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>CNSS Data Explorer</title>
            <link rel="stylesheet" href="{{ url_for('static', filename='style.css') }}">
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        </head>
        <body>
            <header>
                <h1>CNSS Data Explorer</h1>
                <p>Search and analyze salary data from CNSS declarations</p>
            </header>
            
            <div class="container">
                <div class="tabs">
                    <div class="tab active" data-target="search-tab">Search</div>
                    <div class="tab" data-target="visualization-tab">Visualization</div>
                </div>
                
                <div id="search-tab" class="tab-content active">
                    <div class="search-form">
                        <div class="form-row">
                            <div class="form-group">
                                <label for="company-name">Company Name</label>
                                <input type="text" id="company-name" placeholder="Enter company name...">
                            </div>
                            <div class="form-group">
                                <label for="employee-name">Employee Name</label>
                                <input type="text" id="employee-name" placeholder="Enter employee name...">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label for="city">City</label>
                                <select id="city">
                                    <option value="">All Cities</option>
                                    {% for city in cities %}
                                    <option value="{{ city }}">{{ city }}</option>
                                    {% endfor %}
                                </select>
                            </div>
                            <div class="form-group">
                                <label for="activity">Activity (type to search)</label>
                                <input type="text" id="activity" placeholder="Enter activity keywords...">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label for="min-salary">Min Salary (MAD)</label>
                                <input type="number" id="min-salary" placeholder="Min salary..." value="0">
                            </div>
                            <div class="form-group">
                                <label for="max-salary">Max Salary (MAD)</label>
                                <input type="number" id="max-salary" placeholder="Max salary..." value="10000000">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label>Result Limit</label>
                                <div class="limit-options">
                                    <button class="limit-btn active" data-limit="100">100</button>
                                    <button class="limit-btn" data-limit="500">500</button>
                                    <button class="limit-btn" data-limit="1000">1K</button>
                                    <button class="limit-btn" data-limit="5000">5K</button>
                                    <button class="limit-btn" data-limit="10000">10K</button>
                                    <input type="number" id="limit" placeholder="Custom limit..." value="100" style="width: 120px;">
                                </div>
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <button id="search-button">Search</button>
                            <button id="reset-button">Reset</button>
                        </div>
                    </div>
                    
                    <div class="results-container" id="results-container">
                        <h2>Search Results</h2>
                        <div id="results-table"></div>
                    </div>
                </div>
                
                <div id="visualization-tab" class="tab-content">
                    <div class="search-form">
                        <h2>Visualization Filters</h2>
                        <p>Apply filters to generate visualizations based on specific criteria</p>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label for="viz-company-name">Company Name</label>
                                <input type="text" id="viz-company-name" placeholder="Enter company name...">
                            </div>
                            <div class="form-group">
                                <label for="viz-employee-name">Employee Name</label>
                                <input type="text" id="viz-employee-name" placeholder="Enter employee name...">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label for="viz-city">City</label>
                                <select id="viz-city">
                                    <option value="">All Cities</option>
                                    {% for city in cities %}
                                    <option value="{{ city }}">{{ city }}</option>
                                    {% endfor %}
                                </select>
                            </div>
                            <div class="form-group">
                                <label for="viz-activity">Activity (type to search)</label>
                                <input type="text" id="viz-activity" placeholder="Enter activity keywords...">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <div class="form-group">
                                <label for="viz-min-salary">Min Salary (MAD)</label>
                                <input type="number" id="viz-min-salary" placeholder="Min salary..." value="0">
                            </div>
                            <div class="form-group">
                                <label for="viz-max-salary">Max Salary (MAD)</label>
                                <input type="number" id="viz-max-salary" placeholder="Max salary..." value="10000000">
                            </div>
                        </div>
                        
                        <div class="form-row">
                            <button id="generate-viz-button">Generate Visualizations</button>
                            <button id="reset-viz-button">Reset</button>
                        </div>
                    </div>
                    
                    <div class="charts-container">
                        <div class="chart-box">
                            <h3 class="chart-title">Salary Distribution</h3>
                            <canvas id="salary-distribution-chart"></canvas>
                        </div>
                        <div class="chart-box">
                            <h3 class="chart-title">City Comparison</h3>
                            <canvas id="city-comparison-chart"></canvas>
                        </div>
                        <div class="chart-box">
                            <h3 class="chart-title">Top Activities by Salary</h3>
                            <canvas id="activity-salary-chart"></canvas>
                        </div>
                        <div class="chart-box">
                            <h3 class="chart-title">Top Companies by Average Salary</h3>
                            <canvas id="top-companies-chart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
            
            <script>
                // Helper function to format currency
                function formatCurrency(value) {
                    return new Intl.NumberFormat('fr-MA', {
                        style: 'currency',
                        currency: 'MAD',
                        minimumFractionDigits: 2
                    }).format(value);
                }
                
                // Tab functionality
                document.querySelectorAll('.tab').forEach(tab => {
                    tab.addEventListener('click', function() {
                        // Remove active class from all tabs
                        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                        // Add active class to clicked tab
                        this.classList.add('active');
                        
                        // Hide all tab content
                        document.querySelectorAll('.tab-content').forEach(content => {
                            content.classList.remove('active');
                        });
                        
                        // Show the target tab content
                        const targetId = this.getAttribute('data-target');
                        document.getElementById(targetId).classList.add('active');
                    });
                });
                
                // Limit button functionality
                document.querySelectorAll('.limit-btn').forEach(btn => {
                    btn.addEventListener('click', function() {
                        // Remove active class from all limit buttons
                        document.querySelectorAll('.limit-btn').forEach(b => b.classList.remove('active'));
                        // Add active class to clicked button
                        this.classList.add('active');
                        // Set the limit input value
                        document.getElementById('limit').value = this.dataset.limit;
                    });
                });
                
                // Update limit buttons when input changes
                document.getElementById('limit').addEventListener('input', function() {
                    const value = this.value;
                    let found = false;
                    document.querySelectorAll('.limit-btn').forEach(btn => {
                        if (btn.dataset.limit === value) {
                            btn.classList.add('active');
                            found = true;
                        } else {
                            btn.classList.remove('active');
                        }
                    });
                });
                
                // Handle search functionality
                document.getElementById('search-button').addEventListener('click', function() {
                    const company = document.getElementById('company-name').value;
                    const employee = document.getElementById('employee-name').value;
                    const city = document.getElementById('city').value;
                    const activity = document.getElementById('activity').value;
                    const minSalary = document.getElementById('min-salary').value;
                    const maxSalary = document.getElementById('max-salary').value;
                    const limit = document.getElementById('limit').value;
                    
                    // Show loading state
                    document.getElementById('results-table').innerHTML = '<div class="loading"></div>';
                    
                    // Make API call
                    fetch('/api/search', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            company_name: company,
                            employee_name: employee,
                            city: city,
                            activity: activity,
                            min_salary: minSalary,
                            max_salary: maxSalary,
                            limit: limit
                        }),
                    })
                    .then(response => response.json())
                    .then(data => {
                        const results = data.results;
                        
                        if (results.length === 0) {
                            document.getElementById('results-table').innerHTML = '<p>No results found.</p>';
                            return;
                        }
                        
                        // Create table
                        let table = '<table>';
                        table += '<thead><tr>';
                        table += '<th>Employee</th>';
                        table += '<th>Company</th>';
                        table += '<th>City</th>';
                        table += '<th>Activity</th>';
                        table += '<th>Salary (MAD)</th>';
                        table += '</tr></thead>';
                        
                        table += '<tbody>';
                        results.forEach(row => {
                            table += '<tr>';
                            table += `<td>${row.full_name}</td>`;
                            table += `<td>${row.company_name}</td>`;
                            table += `<td>${row.city || ''}</td>`;
                            table += `<td>${(row.activity_description || '').substring(0, 50)}${row.activity_description && row.activity_description.length > 50 ? '...' : ''}</td>`;
                            table += `<td>${formatCurrency(row.salary_amount)}</td>`;
                            table += '</tr>';
                        });
                        table += '</tbody></table>';
                        
                        table += `<p><strong>Showing ${results.length} results</strong></p>`;
                        
                        document.getElementById('results-table').innerHTML = table;
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        document.getElementById('results-table').innerHTML = '<p>Error fetching results.</p>';
                    });
                });
                
                // Reset search form
                document.getElementById('reset-button').addEventListener('click', function() {
                    document.getElementById('company-name').value = '';
                    document.getElementById('employee-name').value = '';
                    document.getElementById('city').selectedIndex = 0;
                    document.getElementById('activity').value = '';
                    document.getElementById('min-salary').value = '0';
                    document.getElementById('max-salary').value = '10000000';
                    document.getElementById('limit').value = '100';
                    // Reset limit buttons
                    document.querySelectorAll('.limit-btn').forEach(btn => btn.classList.remove('active'));
                    document.querySelector('.limit-btn[data-limit="100"]').classList.add('active');
                });
                
                // Charts
                let charts = {
                    salaryDistribution: null,
                    cityComparison: null,
                    activitySalary: null,
                    topCompanies: null
                };
                
                function initializeCharts() {
                    // Initialize empty charts
                    const salaryDistCtx = document.getElementById('salary-distribution-chart').getContext('2d');
                    charts.salaryDistribution = new Chart(salaryDistCtx, {
                        type: 'bar',
                        data: {
                            labels: [],
                            datasets: [{
                                label: 'Number of Employees',
                                data: [],
                                backgroundColor: 'rgba(54, 162, 235, 0.7)',
                                borderColor: 'rgba(54, 162, 235, 1)',
                                borderWidth: 1
                            }]
                        },
                        options: {
                            responsive: true,
                            plugins: {
                                title: {
                                    display: true,
                                    text: 'Salary Distribution'
                                }
                            },
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    title: {
                                        display: true,
                                        text: 'Number of Employees'
                                    }
                                },
                                x: {
                                    title: {
                                        display: true,
                                        text: 'Salary Range (MAD)'
                                    }
                                }
                            }
                        }
                    });
                    
                    // City comparison chart
                    const cityCompCtx = document.getElementById('city-comparison-chart').getContext('2d');
                    charts.cityComparison = new Chart(cityCompCtx, {
                        type: 'bar',
                        data: {
                            labels: [],
                            datasets: [{
                                label: 'Average Salary',
                                data: [],
                                backgroundColor: 'rgba(75, 192, 192, 0.7)',
                                borderColor: 'rgba(75, 192, 192, 1)',
                                borderWidth: 1,
                                yAxisID: 'y'
                            }, {
                                label: 'Number of Employees',
                                data: [],
                                backgroundColor: 'rgba(255, 159, 64, 0.7)',
                                borderColor: 'rgba(255, 159, 64, 1)',
                                borderWidth: 1,
                                type: 'line',
                                yAxisID: 'y1'
                            }]
                        },
                        options: {
                            responsive: true,
                            plugins: {
                                title: {
                                    display: true,
                                    text: 'Salary Comparison by City'
                                }
                            },
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    type: 'linear',
                                    position: 'left',
                                    title: {
                                        display: true,
                                        text: 'Average Salary (MAD)'
                                    }
                                },
                                y1: {
                                    beginAtZero: true,
                                    type: 'linear',
                                    position: 'right',
                                    grid: {
                                        drawOnChartArea: false
                                    },
                                    title: {
                                        display: true,
                                        text: 'Number of Employees'
                                    }
                                }
                            }
                        }
                    });
                    
                    // Activity salary chart
                    const activityCtx = document.getElementById('activity-salary-chart').getContext('2d');
                    charts.activitySalary = new Chart(activityCtx, {
                        type: 'bar',
                        data: {
                            labels: [],
                            datasets: [{
                                label: 'Average Salary',
                                data: [],
                                backgroundColor: 'rgba(153, 102, 255, 0.7)',
                                borderColor: 'rgba(153, 102, 255, 1)',
                                borderWidth: 1
                            }]
                        },
                        options: {
                            responsive: true,
                            indexAxis: 'y',
                            plugins: {
                                title: {
                                    display: true,
                                    text: 'Top Activities by Average Salary'
                                }
                            },
                            scales: {
                                x: {
                                    beginAtZero: true,
                                    title: {
                                        display: true,
                                        text: 'Average Salary (MAD)'
                                    }
                                }
                            }
                        }
                    });
                    
                    // Top companies chart
                    const companiesCtx = document.getElementById('top-companies-chart').getContext('2d');
                    charts.topCompanies = new Chart(companiesCtx, {
                        type: 'bar',
                        data: {
                            labels: [],
                            datasets: [{
                                label: 'Average Salary',
                                data: [],
                                backgroundColor: 'rgba(255, 99, 132, 0.7)',
                                borderColor: 'rgba(255, 99, 132, 1)',
                                borderWidth: 1
                            }]
                        },
                        options: {
                            responsive: true,
                            indexAxis: 'y',
                            plugins: {
                                title: {
                                    display: true,
                                    text: 'Top Companies by Average Salary'
                                }
                            },
                            scales: {
                                x: {
                                    beginAtZero: true,
                                    title: {
                                        display: true,
                                        text: 'Average Salary (MAD)'
                                    }
                                }
                            }
                        }
                    });
                }
                
                // Initialize charts on page load
                initializeCharts();
                
                // Function to update charts with new data
                function updateCharts(data) {
                    // Update salary distribution chart
                    const salaryDist = data.salary_distribution;
                    charts.salaryDistribution.data.labels = salaryDist.map(d => d.salary_range);
                    charts.salaryDistribution.data.datasets[0].data = salaryDist.map(d => d.count);
                    charts.salaryDistribution.update();
                    
                    // Update city comparison chart
                    const cityStats = data.city_stats.slice(0, 15);
                    charts.cityComparison.data.labels = cityStats.map(c => c.city);
                    charts.cityComparison.data.datasets[0].data = cityStats.map(c => c.avg_salary);
                    charts.cityComparison.data.datasets[1].data = cityStats.map(c => c.employee_count);
                    charts.cityComparison.update();
                    
                    // Update activity salary chart
                    const activityStats = data.activity_stats.slice(0, 15);
                    charts.activitySalary.data.labels = activityStats.map(a => 
                        a.activity_description.length > 40 ? 
                        a.activity_description.substring(0, 40) + '...' : a.activity_description);
                    charts.activitySalary.data.datasets[0].data = activityStats.map(a => a.avg_salary);
                    charts.activitySalary.update();
                    
                    // Update top companies chart
                    const topCompanies = data.top_companies.slice(0, 15);
                    charts.topCompanies.data.labels = topCompanies.map(c => 
                        c.company_name.length > 40 ? 
                        c.company_name.substring(0, 40) + '...' : c.company_name);
                    charts.topCompanies.data.datasets[0].data = topCompanies.map(c => c.avg_salary);
                    charts.topCompanies.update();
                }
                
                // Generate visualizations
                document.getElementById('generate-viz-button').addEventListener('click', function() {
                    const company = document.getElementById('viz-company-name').value;
                    const employee = document.getElementById('viz-employee-name').value;
                    const city = document.getElementById('viz-city').value;
                    const activity = document.getElementById('viz-activity').value;
                    const minSalary = document.getElementById('viz-min-salary').value;
                    const maxSalary = document.getElementById('viz-max-salary').value;
                    
                    // Make API call
                    fetch('/api/stats', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            company_name: company,
                            employee_name: employee,
                            city: city,
                            activity: activity,
                            min_salary: minSalary,
                            max_salary: maxSalary
                        }),
                    })
                    .then(response => response.json())
                    .then(data => {
                        updateCharts(data);
                    })
                    .catch(error => {
                        console.error('Error:', error);
                        alert('Error generating visualizations: ' + error);
                    });
                });
                
                // Reset visualization form
                document.getElementById('reset-viz-button').addEventListener('click', function() {
                    document.getElementById('viz-company-name').value = '';
                    document.getElementById('viz-employee-name').value = '';
                    document.getElementById('viz-city').selectedIndex = 0;
                    document.getElementById('viz-activity').value = '';
                    document.getElementById('viz-min-salary').value = '0';
                    document.getElementById('viz-max-salary').value = '10000000';
                });
                
                // Trigger initial search on page load
                document.getElementById('search-button').click();
                
                // Trigger initial visualization on page load
                document.getElementById('generate-viz-button').click();
            </script>
        </body>
        </html>
        """)
        
    app.run(debug=True, port=5000)