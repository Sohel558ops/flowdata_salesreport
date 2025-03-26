import pandas as pd
import mysql.connector
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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

# --- SQLAlchemy Engine ---
engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
Session = sessionmaker(bind=engine)

# --- Part 1: Address Enrichment for Orders ---

def create_tables():
    """Creates the orders and ip_locations tables if they don't exist."""
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
            )
        '''))
        connection.execute(text('''
            CREATE TABLE IF NOT EXISTS ip_locations (
                ip_address VARCHAR(255) PRIMARY KEY,
                city VARCHAR(255),
                state VARCHAR(255),
                zip_code VARCHAR(255),
                INDEX ip_address_index (ip_address)
            )
        '''))
        connection.commit()

def load_orders_data(orders_file):
    """Loads order data from a CSV file into the orders table using pandas to_sql."""
    try:
        df = pd.read_csv(orders_file)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        df.rename(columns={'zip': 'zip_code', '$_sale': 'sale_amount'}, inplace=True)
        df["sale_amount"] = df["sale_amount"].replace('[$,]', '', regex=True).astype(float)
        df["order_date"] = pd.to_datetime(df["date"], errors='coerce').dt.date

        df.to_sql('orders', engine, if_exists='append', index=False, chunksize=10000) #chunksize for large datasets

    except Exception as e:
        logging.error(f"Error processing orders file: {e}")

def load_ip_addresses(ip_file):
    """Loads IP addresses from a CSV file, retrieves location details using an API, and saves them in the database."""
    try:
        df = pd.read_csv(ip_file)
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        with Session() as session_db:
            for _, row in df.iterrows():
                ip_address = row['ip_address']
                if session_db.execute(text("SELECT 1 FROM ip_locations WHERE ip_address = :ip"), {"ip": ip_address}).fetchone():
                    continue

                try:
                    response = session.get(f"https://api.iplocation.net/?ip={ip_address}")
                    data = response.json()
                    city = data.get('city', None)
                    state = data.get('state', None)
                    zip_code = data.get('zip_code', None)

                    session_db.execute(text('''
                        INSERT INTO ip_locations (ip_address, city, state, zip_code)
                        VALUES (:ip, :city, :state, :zip)
                    '''), {"ip": ip_address, "city": city, "state": state, "zip": zip_code})
                    session_db.commit()

                except Exception as e:
                    logging.error(f"Error fetching location for IP {ip_address}: {e}")

    except Exception as e:
        logging.error(f"Error processing IP file: {e}")

def update_orders_with_ip_locations():
    """Updates the orders table with location details from the ip_locations table."""
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

def generate_export_file():
    """Generates an export CSV file containing order_number, city, state, and zip_code."""
    df = pd.read_sql_query(text("SELECT order_number, city, state, zip_code FROM orders"), engine)
    if not df.empty:
        df.to_csv("orders_export.csv", index=False)
        print("Export file generated: orders_export.csv")
    else:
        print("No data found in orders table to export.")

def generate_quarterly_sales_report(state_name, year):
    """Generates a quarterly sales report for a given state and year in Excel format."""
    query = text('''
        SELECT QUARTER(order_date) as quarter, city, SUM(sale_amount) as total_sales
        FROM orders
        WHERE state = :state AND YEAR(order_date) = :year AND city IS NOT NULL
        GROUP BY city, quarter
        ORDER BY city, quarter;
    ''')
    df = pd.read_sql_query(query, engine, params={"state": state_name.upper(), "year": year})
    if not df.empty:
        output_file = f"{state_name.upper()}_state_sales_report_{year}.xlsx"
        df.to_excel(output_file, index=False)
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