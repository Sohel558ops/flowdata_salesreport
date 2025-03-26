# Optimized Data Processing and Sales Reporting Application

This application is an optimized version of the original data processing and sales reporting tool, designed to handle large datasets (100,000+ records) efficiently. It processes order data from CSV files, enriches it with location information from an IP address lookup API, and generates sales reports.

## Overview

The application performs the following tasks:

1.  **Data Loading and Cleaning:**
    * Loads order data from a CSV file (`orders_file.csv`) into a MySQL database using `pandas` and SQLAlchemy's `to_sql` method with chunking for efficient large data insertion.
    * Cleans and transforms the data, including normalizing column names, converting data types, and handling potential data inconsistencies.
    * Loads IP addresses from a separate CSV file (`ip_addresses.csv`).
2.  **IP Address Enrichment:**
    * Retrieves location information (city, state, zip code) for each IP address using the `iplocation.net` API.
    * Stores the location data in a MySQL table (`ip_locations`).
    * Updates the order data with the retrieved location information.
3.  **Data Export and Reporting:**
    * Generates a CSV export file (`orders_export.csv`) containing order numbers and location details.
    * Creates quarterly sales reports in Excel format (`.xlsx`) for a specified state and year.

## Key Optimizations

* **SQLAlchemy:**
    * Replaced `mysql.connector` with SQLAlchemy for robust database interactions, including efficient large data insertion and protection against SQL injection.
    * Uses SQLAlchemy sessions for efficient connection management and transaction control.
* **`pandas.to_sql` with Chunking:**
    * Employs `pandas.to_sql` with `chunksize` to insert data in batches, significantly reducing memory usage and improving performance for large CSV files.
* **Prepared Statements:**
    * SQLAlchemy uses prepared statements, enhancing security and query performance.
* **Direct SQL Queries for Reporting:**
    * Uses `pd.read_sql_query` to directly execute SQL queries for generating export files and reports, improving efficiency.
* **Context Managers:**
    * Uses `with engine.connect()` and `with Session()` to ensure database connections are properly closed.
* **Date Conversion:**
    * Improved date conversion using `.dt.date` for consistent SQL insertion.

## Prerequisites

* Python 3.x
* MySQL Server
* Python libraries:
    * `pandas`
    * `sqlalchemy`
    * `mysql-connector-python` (required by SQLAlchemy)
    * `requests`
    * `urllib3`
* An API key for `iplocation.net` (if required).

## Installation

1.  **Install Python Libraries:**

    ```bash
    pip install pandas sqlalchemy mysql-connector-python requests urllib3
    ```

2.  **Set up MySQL Database:**

    * Create a MySQL database named `flowdata_salesreport`.
    * Update the database connection details in the `DB_CONFIG` dictionary in the Python script.

3.  **Obtain API Key (if needed):**

    * If the `iplocation.net` API requires an API key, obtain one and add it to the API request in the `load_ip_addresses` function.

4.  **Prepare CSV Files:**

    * Create `orders_file.csv` and `ip_addresses.csv` files with the order and IP address data, respectively.
    * Place the CSV files in the same directory as the Python script or provide the correct file paths in the script.

## Usage

1.  **Run the Python Script:**

    ```bash
    python your_script_name.py
    ```

2.  **Output Files:**

    * The script will generate `orders_export.csv` and an Excel sales report file (e.g., `IL_state_sales_report_2021.xlsx`) in the same directory.
    * The application also makes use of a `app.log` file, that stores error log information.

## File Structure

.
├── your_script_name.py
├── orders_file.csv
├── ip_addresses.csv
└── app.log


## Configuration

* **Database Connection:** Modify the `DB_CONFIG` dictionary in the script to match your MySQL database credentials.
* **API URL:** Update the API URL in the `load_ip_addresses` function if necessary.
* **State and Year:** Change the state and year parameters in the `generate_quarterly_sales_report` function to generate reports for different regions and time periods.

## Error Handling

* The application includes comprehensive error handling for database operations, API requests, and file processing.
* Error messages are logged to the console and to the `app.log` file.
* SQLAlchemy helps to protect against SQL injections and provides robust error handling.
* API request retries are implemented using `requests.adapters.Retry`.

## Notes

* Ensure that the CSV files are correctly formatted and contain the expected data.
* The API rate limits should be considered when processing a large number of IP addresses.
* The database schema is created automatically if it doesn't exist.
* Add API key handling if the IP location API requires it.