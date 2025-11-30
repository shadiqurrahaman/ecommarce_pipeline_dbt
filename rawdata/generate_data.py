import pandas as pd
from faker import Faker
import pyodbc
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
import time
import json

# --- 1. CONFIGURATION (Read from Docker Environment Variables) ---

# SQL Server
SQL_SERVER = os.environ.get('SQL_SERVER', 'localhost')
SQL_DB_NAME = os.environ.get('SQL_DB_NAME', 'model')
SQL_DB_USER = os.environ.get('SQL_DB_USER', 'SA')
SQL_DB_PASSWORD = os.environ.get('SQL_DB_PASSWORD', 'YourStrong!Passw0rd')

# MongoDB
MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'events_db')

# Simulation Parameters
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
DAYS_OF_DATA = 30
NUM_ORDERS_PER_DAY = 50
NUM_WEB_EVENTS_PER_ORDER = 5 
UPDATE_PERCENTAGE = 0.05  # 5% of customers will have an update on Day 2

# --- 2. CONNECTION HELPERS ---

def connect_sql_server(initial_db='master'):
    """Connects to SQL Server, retrying if necessary."""
    conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={initial_db};UID={SQL_DB_USER};PWD={SQL_DB_PASSWORD};TrustServerCertificate=yes'
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            return conn
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            # 08001 is often a transient error or DB starting up
            if sqlstate == '08001' and i < max_retries - 1:
                print(f"SQL Server connection failed. Retrying in {2**i} seconds...")
                time.sleep(2**i)
            else:
                print(f"Failed to connect to SQL Server: {ex}")
                raise
    return None

def connect_mongodb():
    """Connects to MongoDB."""
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ping') 
        return client
    except Exception as ex:
        print(f"Failed to connect to MongoDB: {ex}")
        raise

# --- 3. DATABASE SETUP (DDL) ---

def setup_sql_server(conn):
    """Ensures DB, Schema, and Tables exist in SQL Server."""
    cursor = conn.cursor()
    
    # 1. Create Database (if needed) - requires connecting to 'master' first
    try:
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{SQL_DB_NAME}') CREATE DATABASE {SQL_DB_NAME};")
        print(f"Database {SQL_DB_NAME} ensured.")
    except Exception as e:
        print(f"Could not create DB in 'master': {e}. Proceeding assuming DB exists.")

    # Reconnect to the specific database
    conn.close()
    conn = connect_sql_server(initial_db=SQL_DB_NAME)
    cursor = conn.cursor()
    
    # 2. Create Schema
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw') EXEC('CREATE SCHEMA raw');")
    
    # 3. Create Tables (Raw Layer)
    
    # Customer Table (SCD Source - Uses TRUNCATE/INSERT strategy in the script)
    cursor.execute("""
    IF OBJECT_ID('raw.Customers', 'U') IS NOT NULL DROP TABLE raw.Customers;
    CREATE TABLE raw.Customers (
        customer_id INT PRIMARY KEY,
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        email NVARCHAR(100),
        address NVARCHAR(255),
        state_province NVARCHAR(50),
        last_modified_at DATETIMEOFFSET
    );
    """)

    # Product Catalog Table (Static/Snapshot Source)
    cursor.execute("""
    IF OBJECT_ID('raw.ProductCatalog', 'U') IS NOT NULL DROP TABLE raw.ProductCatalog;
    CREATE TABLE raw.ProductCatalog (
        product_id INT PRIMARY KEY,
        product_name NVARCHAR(100),
        category NVARCHAR(50),
        price NUMERIC(10, 2),
        stock_quantity INT,
        last_modified_at DATETIMEOFFSET
    );
    """)

    # Orders Table (Incremental Source - Uses APPEND strategy in the script)
    cursor.execute("""
    IF OBJECT_ID('raw.Orders', 'U') IS NOT NULL DROP TABLE raw.Orders;
    CREATE TABLE raw.Orders (
        order_id BIGINT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        order_date DATETIMEOFFSET,
        quantity INT,
        status NVARCHAR(50),
        total_amount NUMERIC(10, 2)
    );
    """)

    conn.commit()
    print("SQL Server schema created in 'raw' schema.")
    return conn # Return the connection to the specific database

def setup_mongodb(client):
    """Ensures MongoDB collection and index exist."""
    db = client[MONGO_DB_NAME]
    collection = db['web_events']
    
    # Ensure index on the watermark column for efficient incremental queries
    collection.create_index([("event_timestamp", 1)])
    print(f"MongoDB database '{MONGO_DB_NAME}' and 'web_events' collection ensured.")
    return collection

# --- 4. DATA GENERATION LOGIC ---
fake = Faker()

def generate_products(num_prod, base_date):
    """Generates initial product catalog data."""
    products = []
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Food']
    
    # Product name prefixes and suffixes for more realistic names
    electronics = ['Smart', 'Wireless', 'Bluetooth', 'HD', '4K', 'Pro', 'Ultra', 'Gaming']
    clothing = ['Cotton', 'Denim', 'Leather', 'Silk', 'Wool', 'Premium', 'Classic', 'Sports']
    home = ['Wooden', 'Metal', 'Ceramic', 'Glass', 'Eco', 'Modern', 'Vintage', 'Deluxe']
    sports = ['Professional', 'Training', 'Outdoor', 'Indoor', 'Fitness', 'Athletic', 'Performance']
    
    for i in range(1, num_prod + 1):
        category = fake.random_element(categories)
        
        # Generate realistic product names based on category
        if category == 'Electronics':
            product_name = f"{fake.random_element(electronics)} {fake.random_element(['Headphones', 'Speaker', 'Keyboard', 'Mouse', 'Monitor', 'Camera', 'Tablet', 'Smartwatch'])}"
        elif category == 'Clothing':
            product_name = f"{fake.random_element(clothing)} {fake.random_element(['T-Shirt', 'Jeans', 'Jacket', 'Sweater', 'Dress', 'Shoes', 'Hat', 'Scarf'])}"
        elif category == 'Home & Garden':
            product_name = f"{fake.random_element(home)} {fake.random_element(['Chair', 'Table', 'Lamp', 'Vase', 'Cushion', 'Rug', 'Plant Pot', 'Mirror'])}"
        elif category == 'Sports':
            product_name = f"{fake.random_element(sports)} {fake.random_element(['Basketball', 'Soccer Ball', 'Yoga Mat', 'Dumbbells', 'Resistance Bands', 'Water Bottle', 'Running Shoes'])}"
        elif category == 'Books':
            product_name = f"{fake.random_element(['The', 'A', 'My'])} {fake.word().title()} {fake.random_element(['Guide', 'Story', 'Journey', 'Adventure', 'Mystery', 'Chronicles'])}"
        elif category == 'Toys':
            product_name = f"{fake.random_element(['Fun', 'Educational', 'Interactive', 'Creative'])} {fake.random_element(['Puzzle', 'Building Set', 'Action Figure', 'Doll', 'Board Game', 'Robot'])}"
        elif category == 'Beauty':
            product_name = f"{fake.random_element(['Natural', 'Organic', 'Premium', 'Luxury'])} {fake.random_element(['Moisturizer', 'Serum', 'Shampoo', 'Lipstick', 'Perfume', 'Face Mask'])}"
        else:  # Food
            product_name = f"{fake.random_element(['Organic', 'Fresh', 'Gourmet', 'Artisan'])} {fake.random_element(['Coffee', 'Tea', 'Chocolate', 'Nuts', 'Cookies', 'Honey', 'Olive Oil'])}"
        
        # Generate category-based pricing
        if category == 'Electronics':
            price = round(fake.random_int(50, 1500) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Clothing':
            price = round(fake.random_int(15, 200) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Home & Garden':
            price = round(fake.random_int(20, 500) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Sports':
            price = round(fake.random_int(10, 300) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Books':
            price = round(fake.random_int(8, 50) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Toys':
            price = round(fake.random_int(5, 100) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        elif category == 'Beauty':
            price = round(fake.random_int(10, 150) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        else:  # Food
            price = round(fake.random_int(5, 80) + fake.pyfloat(min_value=0, max_value=0.99, right_digits=2), 2)
        
        products.append({
            'product_id': i,
            'product_name': product_name,
            'category': category,
            'price': price,
            'stock_quantity': fake.random_int(0, 500),
            'last_modified_at': base_date
        })
    return pd.DataFrame(products)

def generate_customers(num_cust, base_date):
    """Generates initial customer data."""
    customers = []
    for i in range(1, num_cust + 1):
        customers.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'address': fake.street_address(),
            'state_province': fake.state_abbr(),
            'last_modified_at': base_date 
        })
    return pd.DataFrame(customers)

def generate_orders(num_ord, customer_ids, product_ids, current_day):
    """Generates orders for a specific day."""
    orders = []
    for i in range(num_ord):
        order_time = current_day + timedelta(hours=fake.random_int(8, 20), minutes=fake.random_int(1, 60), seconds=fake.random_int(1, 60))
        quantity = fake.random_int(1, 5)
        unit_price = fake.random_int(20, 500)
        orders.append({
            'order_id': int(order_time.timestamp() * 1000) + i, # Unique ID
            'customer_id': fake.random_element(customer_ids),
            'product_id': fake.random_element(product_ids),
            'order_date': order_time,
            'quantity': quantity,
            'status': fake.random_element(['COMPLETED', 'SHIPPED', 'PROCESSING']),
            'total_amount': round(unit_price * quantity, 2)
        })
    return pd.DataFrame(orders)

def generate_web_events(num_events, customer_ids, current_day):
    """Generates web event documents for MongoDB."""
    events = []
    for i in range(num_events):
        event_ts = current_day + timedelta(minutes=fake.random_int(1, 1440), seconds=fake.random_int(1, 60))
        events.append({
            'user_id': fake.random_element(customer_ids + [None, None]), # More anonymous users
            'event_type': fake.random_element(['page_view', 'add_to_cart', 'search', 'session_end']),
            'event_timestamp': event_ts,
            'url': fake.uri_path(),
            'meta': {
                'browser': fake.random_element(['Chrome', 'Firefox', 'Safari']),
                'os': fake.random_element(['Windows', 'MacOS', 'iOS', 'Android'])
            }
        })
    return events

# --- 5. INSERTION LOGIC ---

def bulk_insert_sql(conn, table_name, df, columns):
    """Generic insertion for SQL Server using executemany."""
    cursor = conn.cursor()
    # Prepare tuple list for executemany
    data = [tuple(row) for row in df[columns].values]
    
    # Construct the SQL statement
    placeholders = ', '.join(['?'] * len(columns))
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    try:
        cursor.executemany(sql, data)
        conn.commit()
        print(f"Successfully inserted {len(df)} rows into {table_name}.")
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")
        conn.rollback()

# --- 6. EXECUTION (SCD Simulation) ---

if __name__ == '__main__':
    
    # 1. Setup Connections
    print("--- 🛠️ Setting up environment... ---")
    sql_conn_master = connect_sql_server(initial_db='master')
    sql_conn = setup_sql_server(sql_conn_master)
    mongo_client = connect_mongodb()
    events_collection = setup_mongodb(mongo_client)
    
    # Define start dates
    END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    START_DATE = END_DATE - timedelta(days=DAYS_OF_DATA)
    
    customer_ids = list(range(1, NUM_CUSTOMERS + 1))
    product_ids = list(range(1, NUM_PRODUCTS + 1))
    
    # --- DAY 1: Initial Load (Snapshot) ---
    print(f"\n--- 🗓️ Simulating Initial Customer Load ({START_DATE.date()}) ---")
    
    # Generate static initial customers and products
    df_customers = generate_customers(NUM_CUSTOMERS, START_DATE)
    df_products = generate_products(NUM_PRODUCTS, START_DATE)
    
    # Generate orders and events for the first 15 days
    all_orders = []
    all_events = []
    
    current_day = START_DATE
    while current_day < START_DATE + timedelta(days=15):
        day_orders = generate_orders(NUM_ORDERS_PER_DAY, customer_ids, product_ids, current_day)
        all_orders.append(day_orders)
        
        # Generate web events linked to these orders/customers
        day_events = generate_web_events(NUM_ORDERS_PER_DAY * NUM_WEB_EVENTS_PER_ORDER, customer_ids, current_day)
        all_events.extend(day_events)
        
        current_day += timedelta(days=1)
        
    df_orders_day1 = pd.concat(all_orders)
    
    # --- Day 1 Insert ---
    bulk_insert_sql(sql_conn, 'raw.Customers', df_customers, list(df_customers.columns))
    bulk_insert_sql(sql_conn, 'raw.ProductCatalog', df_products, list(df_products.columns))
    bulk_insert_sql(sql_conn, 'raw.Orders', df_orders_day1, list(df_orders_day1.columns))
    events_collection.insert_many(all_events)
    print(f"Inserted {len(df_customers)} customers, {len(df_products)} products, {len(df_orders_day1)} orders and {len(all_events)} events for Day 1 period.")

    # --- DAY 2: Incremental Load & SCD Update ---
    
    UPDATE_DATE = START_DATE + timedelta(days=20) # Update happens halfway through the month
    print(f"\n--- 🔄 Simulating SCD Update & Incremental Load ({UPDATE_DATE.date()} onwards) ---")

    # 1. SCD Simulation: Update 5% of customers
    num_updates = int(NUM_CUSTOMERS * UPDATE_PERCENTAGE)
    
    # Create the updated records
    df_updates = df_customers.sample(n=num_updates).copy()
    df_updates['address'] = df_updates.apply(lambda x: fake.street_address(), axis=1)
    df_updates['last_modified_at'] = UPDATE_DATE # New watermark
    
    # Create the new full snapshot for the SCD Source Table
    df_customers_day2_snapshot = pd.concat([
        df_customers.drop(df_updates.index), # Unchanged records (old timestamp kept)
        df_updates                           # Updated records (new timestamp)
    ]).sort_values(by='customer_id')

    # 2. Incremental Data (New Orders/Events)
    all_new_orders = []
    all_new_events = []
    
    current_day = UPDATE_DATE # Start loop from the update day
    while current_day < END_DATE:
        day_orders = generate_orders(NUM_ORDERS_PER_DAY, customer_ids, product_ids, current_day)
        all_new_orders.append(day_orders)
        
        day_events = generate_web_events(NUM_ORDERS_PER_DAY * NUM_WEB_EVENTS_PER_ORDER, customer_ids, current_day)
        all_new_events.extend(day_events)
        
        current_day += timedelta(days=1)
        
    df_orders_day2 = pd.concat(all_new_orders)

    # --- Day 2 Insert ---
    # TRUNCATE/INSERT into raw.Customers to simulate a full snapshot overwrite from the source system
    cursor = sql_conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.Customers;")
    sql_conn.commit()
    
    bulk_insert_sql(sql_conn, 'raw.Customers', df_customers_day2_snapshot, list(df_customers_day2_snapshot.columns))
    
    # APPEND new data into raw.Orders (Incremental)
    bulk_insert_sql(sql_conn, 'raw.Orders', df_orders_day2, list(df_orders_day2.columns))
    
    # APPEND new events to MongoDB (Incremental)
    events_collection.insert_many(all_new_events)
    print(f"Inserted {len(df_orders_day2)} new orders and {len(all_new_events)} new events for the incremental period.")

    # 7. Cleanup
    sql_conn.close()
    mongo_client.close()
    print("\n✅ Simulation complete. SQL Server and MongoDB connections closed.")