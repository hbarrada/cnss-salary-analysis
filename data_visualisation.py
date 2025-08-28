import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
from sqlalchemy import create_engine
import matplotlib.ticker as mtick
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter
import warnings
warnings.filterwarnings("ignore")

# Set the style for plots
plt.style.use('ggplot')
sns.set(font_scale=1.2)
plt.rcParams['figure.figsize'] = (12, 8)

# Create output directory for visualizations
OUTPUT_DIR = 'visualizations'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def connect_to_db():
    """Establish a connection to the PostgreSQL database"""
    conn = psycopg2.connect(
        dbname="cnss_db",
        user="postgres",
        password="bky2002bky",
        host="localhost"
    )
    return conn

def get_sqlalchemy_engine():
    """Create a SQLAlchemy engine for pandas to use"""
    return create_engine('postgresql://postgres:bky2002bky@localhost:5432/cnss_db')

def format_number(num):
    """Format large numbers for readability"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return f"{num:.0f}"

def money_formatter(x, pos):
    """Format numbers as currency for plots"""
    if x >= 1_000_000:
        return f'{x/1_000_000:.1f}M MAD'
    elif x >= 1_000:
        return f'{x/1_000:.0f}K MAD'
    else:
        return f'{x:.0f} MAD'

def calculate_gini(array):
    """Calculate the Gini coefficient of an array (measure of inequality, 0 is total equality, 1 is total inequality)"""
    array = np.array(array, dtype=np.float64)
    array = array.flatten()
    if np.amin(array) < 0:
        array -= np.amin(array)  # Make all values positive
    array += 0.0000001  # Prevent division by zero
    array = np.sort(array)  # Sort values
    index = np.arange(1, array.shape[0] + 1)  # Get indices
    n = array.shape[0]  # Number of array elements
    return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))  # Calculate Gini

def calculate_hoover_index(array):
    """Calculate the Hoover index (Robin Hood index) - represents proportion of total income that would need
    to be redistributed to achieve equality"""
    array = np.array(array, dtype=np.float64)
    array = array.flatten()
    if len(array) == 0 or np.sum(array) == 0:
        return 0
    mean = np.mean(array)
    return np.sum(np.abs(array - mean)) / (2 * np.sum(array))

def calculate_atkinson_index(array, epsilon=0.5):
    """Calculate the Atkinson index with inequality aversion parameter epsilon"""
    array = np.array(array, dtype=np.float64)
    array = array.flatten()
    array = array[array > 0]  # Remove zeros or negative values
    if len(array) == 0:
        return 0
    
    n = len(array)
    if epsilon == 1:
        # Special case for epsilon = 1
        geometric_mean = np.exp(np.sum(np.log(array)) / n)
        arithmetic_mean = np.mean(array)
        return 1 - (geometric_mean / arithmetic_mean)
    else:
        # General case
        mean = np.mean(array)
        sum_term = np.sum(np.power(array / mean, 1 - epsilon)) / n
        return 1 - np.power(sum_term, 1 / (1 - epsilon))

def fetch_data_for_analysis():
    """
    Fetch comprehensive data for in-depth analysis.
    Returns dataframes for different analysis aspects.
    """
    conn = connect_to_db()
    engine = get_sqlalchemy_engine()
    
    # Create a dictionary to store our dataframes
    data = {}
    
    try:
        # 1. Get overall salary statistics for national analysis
        query = """
        SELECT 
            s.salary_amount,
            c.city,
            c.activity_description,
            c.company_name,
            s.employee_id
        FROM salary_records s
        JOIN companies c ON s.company_id = c.company_id
        WHERE s.salary_amount >= 1
        """
        print("Fetching overall salary data...")
        data['salary_df'] = pd.read_sql_query(query, engine)
        
        # 2. Get city statistics
        query = """
        SELECT 
            c.city,
            COUNT(DISTINCT c.company_id) AS company_count,
            COUNT(DISTINCT s.employee_id) AS employee_count,
            AVG(s.salary_amount) AS avg_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) AS median_salary,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s.salary_amount) AS p25_salary,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s.salary_amount) AS p75_salary,
            STDDEV(s.salary_amount) AS stddev_salary,
            MAX(s.salary_amount) AS max_salary,
            MIN(s.salary_amount) AS min_salary
        FROM companies c
        JOIN salary_records s ON c.company_id = s.company_id
        WHERE c.city IS NOT NULL and s.salary_amount >= 1
        GROUP BY c.city
        ORDER BY employee_count DESC
        """
        print("Fetching city statistics...")
        data['city_df'] = pd.read_sql_query(query, engine)
        
        # 3. Get activity statistics
        query = """
        SELECT 
            c.activity_description,
            COUNT(DISTINCT c.company_id) AS company_count,
            COUNT(DISTINCT s.employee_id) AS employee_count,
            AVG(s.salary_amount) AS avg_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) AS median_salary,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s.salary_amount) AS p25_salary,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s.salary_amount) AS p75_salary,
            STDDEV(s.salary_amount) AS stddev_salary,
            MAX(s.salary_amount) AS max_salary,
            MIN(s.salary_amount) AS min_salary
        FROM companies c
        JOIN salary_records s ON c.company_id = s.company_id
        WHERE c.activity_description IS NOT NULL and s.salary_amount >= 1
        GROUP BY c.activity_description
        ORDER BY employee_count DESC
        """
        print("Fetching activity statistics...")
        data['activity_df'] = pd.read_sql_query(query, engine)
        
        # 4. Get company statistics
        query = """
        SELECT 
            c.company_name,
            c.city,
            c.activity_description,
            COUNT(DISTINCT s.employee_id) AS employee_count,
            AVG(s.salary_amount) AS avg_salary,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.salary_amount) AS median_salary,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s.salary_amount) AS p25_salary,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s.salary_amount) AS p75_salary,
            STDDEV(s.salary_amount) AS stddev_salary,
            MAX(s.salary_amount) AS max_salary,
            MIN(s.salary_amount) AS min_salary
        FROM companies c
        JOIN salary_records s ON c.company_id = s.company_id
        WHERE s.salary_amount >= 1
        GROUP BY c.company_name, c.city, c.activity_description
        HAVING COUNT(DISTINCT s.employee_id) >= 10
        ORDER BY employee_count DESC
        """
        print("Fetching company statistics...")
        data['company_df'] = pd.read_sql_query(query, engine)
        
        # 5. Get salary distribution
        query = """
        WITH salary_buckets AS (
            SELECT 
                CASE
                    WHEN salary_amount < 3000 THEN '< 3K'
                    WHEN salary_amount < 5000 THEN '3K-5K'
                    WHEN salary_amount < 8000 THEN '5K-8K'
                    WHEN salary_amount < 10000 THEN '8K-10K'
                    WHEN salary_amount < 15000 THEN '10K-15K'
                    WHEN salary_amount < 20000 THEN '15K-20K'
                    WHEN salary_amount < 30000 THEN '20K-30K'
                    WHEN salary_amount < 50000 THEN '30K-50K'
                    WHEN salary_amount < 100000 THEN '50K-100K'
                    WHEN salary_amount < 200000 THEN '100K-200K'
                    WHEN salary_amount < 500000 THEN '200K-500K'
                    WHEN salary_amount < 1000000 THEN '500K-1M'
                    ELSE '1M+'
                END AS salary_range,
                COUNT(*) as count
            FROM salary_records
            WHERE salary_amount >= 1
            GROUP BY salary_range
        )
        SELECT * FROM salary_buckets
        ORDER BY 
            CASE salary_range
                WHEN '< 3K' THEN 1
                WHEN '3K-5K' THEN 2
                WHEN '5K-8K' THEN 3
                WHEN '8K-10K' THEN 4
                WHEN '10K-15K' THEN 5
                WHEN '15K-20K' THEN 6
                WHEN '20K-30K' THEN 7
                WHEN '30K-50K' THEN 8
                WHEN '50K-100K' THEN 9
                WHEN '100K-200K' THEN 10
                WHEN '200K-500K' THEN 11
                WHEN '500K-1M' THEN 12
                WHEN '1M+' THEN 13
            END
        """
        print("Fetching salary distribution...")
        data['salary_dist_df'] = pd.read_sql_query(query, engine)
        
        # 6. Get percentile data for national analysis
        query = """
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY salary_amount) as p01,
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY salary_amount) as p05,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY salary_amount) as p10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_amount) as p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_amount) as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_amount) as p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salary_amount) as p90,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY salary_amount) as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY salary_amount) as p99,
            PERCENTILE_CONT(0.999) WITHIN GROUP (ORDER BY salary_amount) as p999,
            AVG(salary_amount) as avg,
            MIN(salary_amount) as min,
            MAX(salary_amount) as max,
            COUNT(*) as count,
            SUM(salary_amount) as total_salary
        FROM salary_records s
        WHERE s.salary_amount >= 1

        """
        print("Fetching percentile data...")
        data['percentiles_df'] = pd.read_sql_query(query, engine)
        
        # 7. Get employee distribution by company size
        query = """
        WITH company_sizes AS (
            SELECT 
                c.company_id,
                COUNT(DISTINCT s.employee_id) as size
            FROM companies c
            JOIN salary_records s ON c.company_id = s.company_id
            WHERE s.salary_amount >= 1
            GROUP BY c.company_id
        ),
        size_buckets AS (
            SELECT
                CASE
                    WHEN size = 1 THEN '1 employee'
                    WHEN size BETWEEN 2 AND 5 THEN '2-5 employees'
                    WHEN size BETWEEN 6 AND 10 THEN '6-10 employees'
                    WHEN size BETWEEN 11 AND 20 THEN '11-20 employees'
                    WHEN size BETWEEN 21 AND 50 THEN '21-50 employees'
                    WHEN size BETWEEN 51 AND 100 THEN '51-100 employees'
                    WHEN size BETWEEN 101 AND 200 THEN '101-200 employees'
                    WHEN size BETWEEN 201 AND 500 THEN '201-500 employees'
                    WHEN size BETWEEN 501 AND 1000 THEN '501-1000 employees'
                    ELSE '1000+ employees'
                END AS size_range,
                COUNT(*) as company_count,
                SUM(size) as employee_count
            FROM company_sizes
            GROUP BY size_range
        )
        SELECT * FROM size_buckets
        ORDER BY 
            CASE size_range
                WHEN '1 employee' THEN 1
                WHEN '2-5 employees' THEN 2
                WHEN '6-10 employees' THEN 3
                WHEN '11-20 employees' THEN 4
                WHEN '21-50 employees' THEN 5
                WHEN '51-100 employees' THEN 6
                WHEN '101-200 employees' THEN 7
                WHEN '201-500 employees' THEN 8
                WHEN '501-1000 employees' THEN 9
                WHEN '1000+ employees' THEN 10
            END
        """
        print("Fetching company size distribution...")
        data['company_size_df'] = pd.read_sql_query(query, engine)
        
        # 8. Get simpler income distribution data
        query = """
        WITH deciles AS (
            SELECT 
                NTILE(10) OVER (ORDER BY salary_amount) as decile,
                salary_amount
            FROM salary_records s
            WHERE s.salary_amount >= 1
        )
        SELECT 
            decile,
            COUNT(*) as employee_count,
            SUM(salary_amount) as total_salary,
            AVG(salary_amount) as avg_salary
        FROM deciles
        GROUP BY decile
        ORDER BY decile
        """
        print("Fetching income distribution by decile...")
        data['income_deciles_df'] = pd.read_sql_query(query, engine)

        # Calculate income share for each decile
        total_salary = data['income_deciles_df']['total_salary'].sum()
        data['income_deciles_df']['income_share'] = data['income_deciles_df']['total_salary'] / total_salary
                
    finally:
        conn.close()
        print("Database connection closed")
    
    return data

def create_report_pdf(data):
    """Generate a comprehensive PDF report with all analyses"""
    print("Generating PDF report...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = f"{OUTPUT_DIR}/cnss_salary_analysis_{timestamp}.pdf"
    
    with PdfPages(pdf_path) as pdf:
        # Generate title page
        fig = plt.figure(figsize=(12, 10))
        fig.suptitle("CNSS Data Analysis Report", fontsize=24, y=0.6)
        fig.text(0.5, 0.5, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}", 
                ha='center', fontsize=14)
        fig.text(0.5, 0.45, "Analysis of Moroccan Salary Data from CNSS Declarations", 
                ha='center', fontsize=16)
        
        plt.axis('off')
        pdf.savefig(fig)
        plt.close()
        
        # Add summary statistics page
        percentiles = data['percentiles_df'].iloc[0]
        total_employees = int(percentiles['count'])
        total_salary = int(percentiles['total_salary'])
        overall_mean = float(percentiles['avg'])
        overall_median = float(percentiles['p50'])
        mean_median_ratio = overall_mean / overall_median if overall_median > 0 else 0
        
        fig = plt.figure(figsize=(12, 10))
        plt.axis('off')
        
        # Create summary text
        summary_text = (
            "CNSS Data Analysis - Key Statistics\n"
            "=================================\n\n"
            f"Total Employees: {total_employees:,}\n\n"
            f"Total Monthly Salary Mass: {total_salary:,} MAD\n\n"
            f"Average (Mean) Monthly Salary: {overall_mean:,.2f} MAD\n\n"
            f"Median Monthly Salary: {overall_median:,.2f} MAD\n\n"
            f"Mean/Median Ratio: {mean_median_ratio:.2f}\n\n"
            f"Minimum Salary: {percentiles['min']:,.2f} MAD\n\n"
            f"Maximum Salary: {percentiles['max']:,.2f} MAD\n\n"
            f"Salary Range (Max-Min): {(percentiles['max'] - percentiles['min']):,.2f} MAD\n\n"
            f"Standard Deviation: {data['salary_df']['salary_amount'].std():,.2f} MAD\n\n"
            "Salary Percentiles:\n"
            f"  10th Percentile: {percentiles['p10']:,.2f} MAD\n"
            f"  25th Percentile: {percentiles['p25']:,.2f} MAD\n"
            f"  50th Percentile (Median): {percentiles['p50']:,.2f} MAD\n"
            f"  75th Percentile: {percentiles['p75']:,.2f} MAD\n"
            f"  90th Percentile: {percentiles['p90']:,.2f} MAD\n"
            f"  95th Percentile: {percentiles['p95']:,.2f} MAD\n"
            f"  99th Percentile: {percentiles['p99']:,.2f} MAD\n\n"
            "Inequality Measures:\n"
            f"  90/10 Ratio: {percentiles['p90']/percentiles['p10']:.2f}\n"
            f"  75/25 Ratio: {percentiles['p75']/percentiles['p25']:.2f}\n"
            f"  99/50 Ratio: {percentiles['p99']/percentiles['p50']:.2f}\n"
            f"  Gini Coefficient: {calculate_gini(data['salary_df']['salary_amount']):.3f}\n"
            f"  Hoover Index: {calculate_hoover_index(data['salary_df']['salary_amount']):.3f}\n"
        )
        
        fig.text(0.5, 0.95, "Summary Statistics", ha='center', fontsize=20)
        fig.text(0.1, 0.85, summary_text, fontsize=12, va='top', family='monospace')
        
        pdf.savefig(fig)
        plt.close()
        
        # 1. Overall salary distribution
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Add a new column for proper ordering
        order_map = {
            '< 3K': 1, '3K-5K': 2, '5K-8K': 3, '8K-10K': 4, '10K-15K': 5,
            '15K-20K': 6, '20K-30K': 7, '30K-50K': 8, '50K-100K': 9,
            '100K-200K': 10, '200K-500K': 11, '500K-1M': 12, '1M+': 13
        }
        salary_dist = data['salary_dist_df'].copy()
        salary_dist = salary_dist.sort_values(
            by='salary_range', 
            key=lambda x: x.map(order_map)
        )
        
        # Calculate percentages
        salary_dist['percentage'] = salary_dist['count'] / salary_dist['count'].sum() * 100
        
        bars = ax.bar(salary_dist['salary_range'], salary_dist['percentage'],
                color=sns.color_palette("viridis", len(salary_dist)))
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{height:.1f}%', ha='center', va='bottom', rotation=0, fontsize=9)
        
        ax.set_title('Salary Distribution in Morocco (CNSS Data)', fontsize=16)
        ax.set_xlabel('Monthly Salary Range (MAD)', fontsize=14)
        ax.set_ylabel('Percentage of Employees', fontsize=14)
        ax.set_ylim(0, max(salary_dist['percentage']) * 1.15)  # Add 15% headroom
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add text with key statistics
        stats_text = (
            f"Total Employees: {total_employees:,}\n"
            f"Mean Salary: {overall_mean:,.0f} MAD\n"
            f"Median Salary: {overall_median:,.0f} MAD\n"
            f"Mean/Median Ratio: {mean_median_ratio:.2f}"
        )
        
        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=10,
                verticalalignment='top', bbox=props)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 2. Percentile comparison chart
        fig, ax = plt.subplots(figsize=(12, 8))
        
        percentile_keys = ['p01', 'p05', 'p10', 'p25', 'p50', 'p75', 'p90', 'p95', 'p99', 'p999']
        percentile_labels = ['1%', '5%', '10%', '25%', '50%\n(Median)', '75%', '90%', '95%', '99%', '99.9%']
        percentile_values = [percentiles[key] for key in percentile_keys]
        
        # Plot bars with gradient color
        colors = plt.cm.viridis(np.linspace(0, 1, len(percentile_keys)))
        bars = ax.bar(percentile_labels, percentile_values, color=colors)
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height * 1.05,
                    f'{height:,.0f}', ha='center', va='bottom', rotation=0, fontsize=9)
        
        ax.set_title('Salary by Percentile in Morocco (CNSS Data)', fontsize=16)
        ax.set_xlabel('Percentile', fontsize=14)
        ax.set_ylabel('Monthly Salary (MAD)', fontsize=14)
        ax.yaxis.set_major_formatter(FuncFormatter(money_formatter))
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add a line for mean
        plt.axhline(y=overall_mean, color='red', linestyle='--', linewidth=2, 
                   label=f'Mean: {overall_mean:,.0f} MAD')
        plt.legend(loc='upper left')
        
        # Add growth rates between percentiles
        growth_text = "Percentile Ratios:\n"
        for i in range(len(percentile_keys)-1):
            ratio = percentile_values[i+1] / percentile_values[i]
            growth_text += f"{percentile_labels[i+1]}/{percentile_labels[i]}: {ratio:.1f}x\n"
        
        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.02, 0.5, growth_text, transform=ax.transAxes, fontsize=9,
                verticalalignment='center', bbox=props)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 3. Calculate and plot the Lorenz curve
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Get salary data and sort it
        salaries = data['salary_df']['salary_amount'].sort_values().reset_index(drop=True)
        
        # Calculate cumulative distribution
        lorenz_x = np.linspace(0, 1, len(salaries))
        lorenz_y = np.cumsum(salaries) / np.sum(salaries)
        
        # Calculate Gini coefficient using our function
        gini = calculate_gini(salaries)
        hoover_idx = calculate_hoover_index(salaries)
        atkinson_idx = calculate_atkinson_index(salaries, epsilon=0.5)
        
        # Plot the Lorenz curve
        ax.plot(lorenz_x, lorenz_y, 'b-', linewidth=2, label=f'Lorenz Curve (Gini={gini:.3f})')
        
        # Plot the line of perfect equality
        ax.plot([0, 1], [0, 1], 'k--', label='Perfect Equality')
        
        # Shade the area between the curves
        ax.fill_between(lorenz_x, lorenz_y, lorenz_x, alpha=0.2, color='blue')
        
        # Add grid, title, and labels
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.set_title('Lorenz Curve of Income Distribution', fontsize=16)
        ax.set_xlabel('Cumulative Share of Population', fontsize=14)
        ax.set_ylabel('Cumulative Share of Income', fontsize=14)
        
        # Format axes as percentages
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        
        # Add text annotation with inequality metrics
        textstr = (
            f'Inequality Metrics:\n'
            f'Gini Coefficient: {gini:.3f}\n'
            f'Hoover Index: {hoover_idx:.3f}\n'
            f'Atkinson Index (ε=0.5): {atkinson_idx:.3f}\n'
            f'P90/P10 Ratio: {percentiles["p90"]/percentiles["p10"]:.2f}\n'
            f'P75/P25 Ratio: {percentiles["p75"]/percentiles["p25"]:.2f}\n'
            f'P99/P50 Ratio: {percentiles["p99"]/percentiles["p50"]:.2f}\n'
            f'Top 1% Min Salary: {percentiles["p99"]:,.0f} MAD'
        )
        
        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.05, 0.95, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props)
        
        # Add legend
        ax.legend(loc='lower right')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        fig, ax = plt.subplots(figsize=(12, 8))

        # Create a bar chart showing income share by decile
        deciles = data['income_deciles_df']
        bars = ax.bar(deciles['decile'], deciles['income_share'] * 100,
                    color=sns.color_palette("viridis", len(deciles)))

        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{height:.1f}%', ha='center', va='bottom', rotation=0)

        ax.set_title('Income Share by Decile', fontsize=16)
        ax.set_xlabel('Income Decile (1 = Lowest 10%, 10 = Highest 10%)', fontsize=14)
        ax.set_ylabel('Share of Total Income (%)', fontsize=14)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # Calculate top income concentration
        top_10_percent = deciles[deciles['decile'] == 10]['income_share'].sum() * 100
        top_20_percent = deciles[deciles['decile'] >= 9]['income_share'].sum() * 100
        bottom_50_percent = deciles[deciles['decile'] <= 5]['income_share'].sum() * 100

        # Add a text box with stats
        stats_text = (
            f"Income Concentration:\n"
            f"Top 10%: {top_10_percent:.1f}%\n"
            f"Top 20%: {top_20_percent:.1f}%\n"
            f"Bottom 50%: {bottom_50_percent:.1f}%\n"
        )

        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.05, 0.95, stats_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props)

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 5. Company size distribution
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Sort company size data by size range
        company_sizes = data['company_size_df'].copy()
        
        # Create bar chart
        bars = ax.bar(company_sizes['size_range'], company_sizes['company_count'],
                     color=sns.color_palette("viridis", len(company_sizes)))
        
        # Add company counts as labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 5,
                   f'{int(height):,}', ha='center', va='bottom')
        
        ax.set_title('Distribution of Companies by Size', fontsize=16)
        ax.set_xlabel('Company Size', fontsize=14)
        ax.set_ylabel('Number of Companies', fontsize=14)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add a second y-axis for employee count
        ax2 = ax.twinx()
        ax2.plot(company_sizes['size_range'], company_sizes['employee_count'], 
                'ro-', linewidth=2, markersize=6)
        ax2.set_ylabel('Number of Employees', color='r', fontsize=14)
        ax2.tick_params(axis='y', labelcolor='r')
        
        # Add employee counts as labels
        for i, count in enumerate(company_sizes['employee_count']):
            ax2.text(i, count + 0.05 * max(company_sizes['employee_count']),
                    f'{int(count):,}', ha='center', va='bottom', color='r')
        
        # Add summary text
        summary_text = (
            f"Total Companies: {company_sizes['company_count'].sum():,}\n"
            f"Total Employees: {company_sizes['employee_count'].sum():,}\n"
            f"Avg. Company Size: {company_sizes['employee_count'].sum() / company_sizes['company_count'].sum():.1f} employees"
        )
        
        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.02, 0.98, summary_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', bbox=props)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 6. Top 15 cities by employee count
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Select top 15 cities by employee count
        top_cities = data['city_df'].sort_values('employee_count', ascending=False).head(15)
        top_cities = top_cities.sort_values('employee_count')  # For better viz
        
        # Create bar chart
        bars = ax.barh(top_cities['city'], top_cities['employee_count'],
                      color=sns.color_palette("viridis", len(top_cities)))
        
        # Add employee counts as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_cities['employee_count']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,}', va='center')
        
        ax.set_title('Top 15 Cities by Number of Employees', fontsize=16)
        ax.set_xlabel('Number of Employees', fontsize=14)
        ax.set_ylabel('City', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add city stats
        for i, city in enumerate(top_cities['city']):
            avg_salary = top_cities.iloc[i]['avg_salary']
            company_count = top_cities.iloc[i]['company_count']
            ax.text(10, i - 0.25, 
                   f"Companies: {int(company_count):,} | Avg Salary: {int(avg_salary):,} MAD", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 7. Top 15 cities by average salary
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Select top 15 cities by average salary (with at least 100 employees)
        top_salary_cities = data['city_df'][data['city_df']['employee_count'] >= 100]
        top_salary_cities = top_salary_cities.sort_values('avg_salary', ascending=False).head(15)
        top_salary_cities = top_salary_cities.sort_values('avg_salary')  # For better viz
        
        # Create bar chart
        bars = ax.barh(top_salary_cities['city'], top_salary_cities['avg_salary'],
                      color=sns.color_palette("viridis", len(top_salary_cities)))
        
        # Add salary values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_salary_cities['avg_salary']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,} MAD', va='center')
        
        ax.set_title('Top 15 Cities by Average Salary (Min 100 Employees)', fontsize=16)
        ax.set_xlabel('Average Monthly Salary (MAD)', fontsize=14)
        ax.set_ylabel('City', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add city stats
        for i, city in enumerate(top_salary_cities['city']):
            employee_count = top_salary_cities.iloc[i]['employee_count']
            company_count = top_salary_cities.iloc[i]['company_count']
            ax.text(10, i - 0.25, 
                   f"Employees: {int(employee_count):,} | Companies: {int(company_count):,}", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 8. Top 15 activities by employee count
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 activities by employee count
        top_activities = data['activity_df'].sort_values('employee_count', ascending=False).head(15)
        top_activities = top_activities.sort_values('employee_count')  # For better viz
        
        # Shorten activity names for better display
        top_activities['short_name'] = top_activities['activity_description'].apply(
            lambda x: x[:50] + '...' if len(x) > 50 else x
        )
        
        # Create bar chart
        bars = ax.barh(top_activities['short_name'], top_activities['employee_count'],
                      color=sns.color_palette("viridis", len(top_activities)))
        
        # Add employee counts as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_activities['employee_count']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,}', va='center')
        
        ax.set_title('Top 15 Business Activities by Number of Employees', fontsize=16)
        ax.set_xlabel('Number of Employees', fontsize=14)
        ax.set_ylabel('Business Activity', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add activity stats
        for i, activity in enumerate(top_activities['short_name']):
            avg_salary = top_activities.iloc[i]['avg_salary']
            company_count = top_activities.iloc[i]['company_count']
            ax.text(10, i - 0.25, 
                   f"Companies: {int(company_count):,} | Avg Salary: {int(avg_salary):,} MAD", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 9. Top 15 activities by average salary
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 activities by average salary (with at least 100 employees)
        top_salary_activities = data['activity_df'][data['activity_df']['employee_count'] >= 100]
        top_salary_activities = top_salary_activities.sort_values('avg_salary', ascending=False).head(15)
        top_salary_activities = top_salary_activities.sort_values('avg_salary')  # For better viz
        
        # Shorten activity names for better display
        top_salary_activities['short_name'] = top_salary_activities['activity_description'].apply(
            lambda x: x[:50] + '...' if len(x) > 50 else x
        )
        
        # Create bar chart
        bars = ax.barh(top_salary_activities['short_name'], top_salary_activities['avg_salary'],
                      color=sns.color_palette("viridis", len(top_salary_activities)))
        
        # Add salary values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_salary_activities['avg_salary']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,} MAD', va='center')
        
        ax.set_title('Top 15 Business Activities by Average Salary (Min 100 Employees)', fontsize=16)
        ax.set_xlabel('Average Monthly Salary (MAD)', fontsize=14)
        ax.set_ylabel('Business Activity', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add activity stats
        for i, activity in enumerate(top_salary_activities['short_name']):
            employee_count = top_salary_activities.iloc[i]['employee_count']
            company_count = top_salary_activities.iloc[i]['company_count']
            median = top_salary_activities.iloc[i]['median_salary']
            ax.text(10, i - 0.25, 
                   f"Employees: {int(employee_count):,} | Companies: {int(company_count):,} | Median: {int(median):,} MAD", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 10. Mean/Median comparison by city
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 cities by employee count
        top_mm_cities = data['city_df'].sort_values('employee_count', ascending=False).head(15)
        top_mm_cities = top_mm_cities.copy()
        
        # Calculate mean/median ratio
        top_mm_cities['mean_median_ratio'] = top_mm_cities['avg_salary'] / top_mm_cities['median_salary']
        top_mm_cities = top_mm_cities.sort_values('mean_median_ratio', ascending=False)
        
        # Create bar chart
        bars = ax.barh(top_mm_cities['city'], top_mm_cities['mean_median_ratio'],
                      color=sns.color_palette("viridis", len(top_mm_cities)))
        
        # Add ratio values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02, bar.get_y() + bar.get_height()/2.,
                   f'{width:.2f}', va='center')
        
        ax.set_title('Income Inequality: Mean/Median Ratio by City', fontsize=16)
        ax.set_xlabel('Mean/Median Ratio (Higher = More Inequality)', fontsize=14)
        ax.set_ylabel('City', fontsize=14)
        ax.set_xlim(1, max(top_mm_cities['mean_median_ratio']) * 1.1)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add city stats
        for i, city in enumerate(top_mm_cities['city']):
            avg = int(top_mm_cities.iloc[i]['avg_salary'])
            median = int(top_mm_cities.iloc[i]['median_salary'])
            employee_count = top_mm_cities.iloc[i]['employee_count']
            ax.text(1.0, i - 0.25, 
                   f"Mean: {avg:,} MAD | Median: {median:,} MAD | Employees: {int(employee_count):,}", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 11. Standard deviation by city
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 cities by employee count
        top_sd_cities = data['city_df'].sort_values('employee_count', ascending=False).head(15)
        
        # Calculate coefficient of variation (standardized measure of dispersion)
        top_sd_cities['cv'] = top_sd_cities['stddev_salary'] / top_sd_cities['avg_salary']
        top_sd_cities = top_sd_cities.sort_values('cv', ascending=False)
        
        # Create bar chart
        bars = ax.barh(top_sd_cities['city'], top_sd_cities['cv'],
                      color=sns.color_palette("viridis", len(top_sd_cities)))
        
        # Add CV values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02, bar.get_y() + bar.get_height()/2.,
                   f'{width:.2f}', va='center')
        
        ax.set_title('Salary Variability: Coefficient of Variation by City', fontsize=16)
        ax.set_xlabel('Coefficient of Variation (StdDev/Mean)', fontsize=14)
        ax.set_ylabel('City', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add city stats
        for i, city in enumerate(top_sd_cities['city']):
            stddev = int(top_sd_cities.iloc[i]['stddev_salary'])
            avg = int(top_sd_cities.iloc[i]['avg_salary'])
            employee_count = top_sd_cities.iloc[i]['employee_count']
            ax.text(0, i - 0.25, 
                   f"StdDev: {stddev:,} MAD | Mean: {avg:,} MAD | Employees: {int(employee_count):,}", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 12. Interquartile Range by city (measure of dispersion)
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 cities by employee count
        top_iqr_cities = data['city_df'].sort_values('employee_count', ascending=False).head(15)
        
        # Calculate IQR and IQR ratio
        top_iqr_cities['iqr'] = top_iqr_cities['p75_salary'] - top_iqr_cities['p25_salary']
        top_iqr_cities['iqr_ratio'] = top_iqr_cities['p75_salary'] / top_iqr_cities['p25_salary']
        top_iqr_cities = top_iqr_cities.sort_values('iqr_ratio', ascending=False)
        
        # Create bar chart
        bars = ax.barh(top_iqr_cities['city'], top_iqr_cities['iqr_ratio'],
                      color=sns.color_palette("viridis", len(top_iqr_cities)))
        
        # Add IQR ratio values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.1, bar.get_y() + bar.get_height()/2.,
                   f'{width:.2f}x', va='center')
        
        ax.set_title('Income Inequality: 75th/25th Percentile Ratio by City', fontsize=16)
        ax.set_xlabel('75th/25th Percentile Ratio', fontsize=14)
        ax.set_ylabel('City', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add city stats
        for i, city in enumerate(top_iqr_cities['city']):
            p25 = int(top_iqr_cities.iloc[i]['p25_salary'])
            p75 = int(top_iqr_cities.iloc[i]['p75_salary'])
            iqr = int(top_iqr_cities.iloc[i]['iqr'])
            employee_count = top_iqr_cities.iloc[i]['employee_count']
            ax.text(1.0, i - 0.25, 
                   f"25th: {p25:,} MAD | 75th: {p75:,} MAD | IQR: {iqr:,} MAD | Employees: {int(employee_count):,}", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 13. Top 15 companies by employee count
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 companies by employee count
        top_companies = data['company_df'].sort_values('employee_count', ascending=False).head(15)
        top_companies = top_companies.sort_values('employee_count')  # For better viz
        
        # Create bar chart
        bars = ax.barh(top_companies['company_name'], top_companies['employee_count'],
                      color=sns.color_palette("viridis", len(top_companies)))
        
        # Add employee counts as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_companies['employee_count']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,}', va='center')
        
        ax.set_title('Top 15 Companies by Number of Employees', fontsize=16)
        ax.set_xlabel('Number of Employees', fontsize=14)
        ax.set_ylabel('Company', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add company stats
        for i, company in enumerate(top_companies['company_name']):
            city = top_companies.iloc[i]['city'] or 'Unknown'
            activity = top_companies.iloc[i]['activity_description']
            if activity and len(activity) > 40:
                activity = activity[:37] + '...'
            avg_salary = int(top_companies.iloc[i]['avg_salary'])
            
            ax.text(10, i - 0.25, 
                   f"City: {city} | Activity: {activity} | Avg Salary: {avg_salary:,} MAD", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # 14. Top 15 companies by average salary (min 30 employees)
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Select top 15 companies by average salary (with at least 30 employees)
        top_salary_companies = data['company_df'][data['company_df']['employee_count'] >= 30]
        top_salary_companies = top_salary_companies.sort_values('avg_salary', ascending=False).head(15)
        top_salary_companies = top_salary_companies.sort_values('avg_salary')  # For better viz
        
        # Create bar chart
        bars = ax.barh(top_salary_companies['company_name'], top_salary_companies['avg_salary'],
                      color=sns.color_palette("viridis", len(top_salary_companies)))
        
        # Add salary values as labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.02 * max(top_salary_companies['avg_salary']), bar.get_y() + bar.get_height()/2.,
                   f'{int(width):,} MAD', va='center')
        
        ax.set_title('Top 15 Companies by Average Salary (Min 30 Employees)', fontsize=16)
        ax.set_xlabel('Average Monthly Salary (MAD)', fontsize=14)
        ax.set_ylabel('Company', fontsize=14)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add company stats
        for i, company in enumerate(top_salary_companies['company_name']):
            city = top_salary_companies.iloc[i]['city'] or 'Unknown'
            activity = top_salary_companies.iloc[i]['activity_description']
            if activity and len(activity) > 40:
                activity = activity[:37] + '...'
            employee_count = top_salary_companies.iloc[i]['employee_count']
            median = int(top_salary_companies.iloc[i]['median_salary'])
            
            ax.text(10, i - 0.25, 
                   f"City: {city} | Activity: {activity} | Employees: {int(employee_count):,} | Median: {median:,} MAD", 
                   fontsize=8, color='blue')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
    
    print(f"PDF report saved to {pdf_path}")
    return pdf_path

def generate_salary_report():
    """Generate a comprehensive salary report"""
    print("Starting CNSS data analysis...")
    
    # Fetch data
    data = fetch_data_for_analysis()
    
    # Generate the PDF report
    pdf_path = create_report_pdf(data)
    
    # Print success message
    print(f"\nAnalysis completed! Report saved to: {pdf_path}")
    
    return pdf_path

if __name__ == "__main__":
    # Generate the comprehensive salary report
    generate_salary_report()