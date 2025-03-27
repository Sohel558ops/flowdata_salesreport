import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from ip2geotools.databases.noncommercial import DbIpCity
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Logging Setup ---
logging.basicConfig(filename='app.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root@123_',
    'database': 'flowdata_salesreport'
}

# --- SQLAlchemy Engine with Connection Pooling ---
engine = create_engine(
    f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}",
    pool_size=20,
    max_overflow=10
)
Session = sessionmaker(bind=engine)

# --- Database Table Setup ---
def create_tables():
    with engine.connect() as connection:
        connection.execute(text('''
            CREATE TABLE IF NOT EXISTS orders (
                order_number VARCHAR(255) PRIMARY KEY,
                order_date DATE,
                ip_address VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                zip_code VARCHAR(255),
                sale_amount FLOAT,
                INDEX ip_address_index (ip_address)
            )'''))
        connection.execute(text('''
            CREATE TABLE IF NOT EXISTS ip_locations (
                ip_address VARCHAR(255) PRIMARY KEY,
                city VARCHAR(255),
                state VARCHAR(255),
                zip_code VARCHAR(255),
                INDEX ip_address_index (ip_address)
            )'''))
        connection.commit()

# --- Data Loading ---
def load_orders_data(orders_file):
    try:
        df = pd.read_csv(orders_file)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        df.rename(columns={'zip': 'zip_code', '$_sale': 'sale_amount'}, inplace=True)
        df["sale_amount"] = df["sale_amount"].replace('[$,]', '', regex=True).astype(float)
        df["order_date"] = pd.to_datetime(df["date"], errors='coerce').dt.date
        df.to_sql('orders', engine, if_exists='append', index=False, chunksize=10000)
    except Exception as e:
        logging.error(f"Error processing orders file: {e}")

# --- Fetch IP Location in Parallel ---
def fetch_ip_location(ip_address):
    try:
        response = DbIpCity.get(ip_address, api_key='free')
        return ip_address, response.city, response.region, response.postal
    except Exception as e:
        logging.error(f"Error fetching location for IP {ip_address}: {e}")
        return ip_address, None, None, None

def process_ips_in_parallel(ip_addresses):
    results = []
    with ThreadPoolExecutor(max_workers=50) as executor:
        future_to_ip = {executor.submit(fetch_ip_location, ip): ip for ip in ip_addresses}
        for future in as_completed(future_to_ip):
            try:
                results.append(future.result())
            except Exception as e:
                logging.error(f"Error in thread execution: {e}")
    return results

# --- Load IP Addresses with Concurrency ---
def load_ip_addresses(ip_file):
    try:
        df = pd.read_csv(ip_file)
        ip_addresses = df['ip_address'].unique()
        existing_ips = {row[0] for row in engine.execute(text("SELECT ip_address FROM ip_locations"))}
        new_ips = list(set(ip_addresses) - existing_ips)
        
        if new_ips:
            results = process_ips_in_parallel(new_ips)
            with engine.connect() as conn:
                conn.execute(
                    text('''INSERT INTO ip_locations (ip_address, city, state, zip_code) VALUES (:ip, :city, :state, :zip)'''),
                    [{"ip": ip, "city": city, "state": state, "zip": zip} for ip, city, state, zip in results]
                )
                conn.commit()
    except Exception as e:
        logging.error(f"Error processing IP file: {e}")

# --- Data Processing ---
def update_orders_with_ip_locations():
    with engine.connect() as connection:
        connection.execute(text('''
            UPDATE orders o
            JOIN ip_locations ip ON o.ip_address = ip.ip_address
            SET o.city = ip.city,
                o.state = ip.state,
                o.zip_code = ip.zip_code
            WHERE o.city IS NULL;
        '''))
        connection.commit()

# --- Exporting Data ---
def generate_export_file():
    df = pd.read_sql_query("SELECT order_number, city, state, zip_code FROM orders", engine)
    if not df.empty:
        df.to_csv("orders_export.csv", index=False)
        print("Export file generated: orders_export.csv")
    else:
        print("No data found in orders table to export.")

# --- Sales Report Generation ---
def generate_quarterly_sales_report(state_name, year):
    query = text('''
        SELECT QUARTER(order_date) as quarter, city, SUM(sale_amount) as total_sales
        FROM orders
        WHERE state = :state AND YEAR(order_date) = :year AND city IS NOT NULL
        GROUP BY quarter, city
        ORDER BY quarter, city;
    ''')
    df = pd.read_sql_query(query, engine, params={"state": state_name.upper(), "year": year})

    if not df.empty:
        # Group by Quarter and City and aggregate total_sales
        grouped_data = df.groupby(['quarter', 'city'])['total_sales'].sum().reset_index()

        # Create a DataFrame where 'Quarter' is the index and it contains 'City' and 'Total Sales'
        report = pd.DataFrame({
            'Quarter': 'Q' + grouped_data['quarter'].astype(str),  # Format quarter as 'Q1', 'Q2', etc.
            'City': grouped_data['city'],
            'Total Sales': grouped_data['total_sales']
        })

        # Save the report to an Excel file
        output_file = f"{state_name.upper()}_state_sales_report_{year}.xlsx"
        report.to_excel(output_file, index=False)  # No index column in the final output
        print(f"Sales report generated: {output_file}")
    else:
        print(f"No sales data found for {state_name} in {year}.")

# --- Main Execution ---
if __name__ == "__main__":
    try:
        create_tables()
        load_orders_data("orders_file.csv")
        load_ip_addresses("ip_addresses.csv")
        update_orders_with_ip_locations()
        generate_export_file()
        generate_quarterly_sales_report("IL", 2021)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
