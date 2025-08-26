import pandas as pd
import pyodbc
import os
import random
from datetime import datetime

# === 1. Configuration ===
server = 'ALI_HANY'
database = 'Care_Stat'
table = 'Payments'

# File paths
folder_path = r'D:\instant\data analysis final\datainseart'
csv_name = 'payment_data.csv'
csv_path = os.path.join(folder_path, csv_name)

# === 2. Check if CSV file exists ===
if not os.path.exists(csv_path):
    print(f"❌ File not found: {csv_path}")
    exit()

# === 3. Read CSV file ===
try:
    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} rows from {csv_name}")
except Exception as e:
    print(f"❌ Error reading CSV file: {e}")
    exit()

# === 4. Validate required columns ===
required_cols = {
    'payment_id', 'patient_id', 'method', 'amount',
    'payment_date', 'payment_status'
}
optional_cols = {'appointment_id', 'record_id', 'department_id', 'transaction_id'}
missing_cols = required_cols - set(df.columns)

if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print(f"Available columns: {list(df.columns)}")
    exit()

# === 5. Data cleaning and type conversion ===

# Numeric columns
df['payment_id'] = pd.to_numeric(df['payment_id'], errors='coerce')
df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce')
df['appointment_id'] = pd.to_numeric(df['appointment_id'], errors='coerce')
df['record_id'] = pd.to_numeric(df['record_id'], errors='coerce')
df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce')
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# String normalization
df['method'] = df['method'].astype(str).str.strip().str.lower()
df['payment_status'] = df['payment_status'].astype(str).str.strip().str.lower()
df['transaction_id'] = df['transaction_id'].astype(str).str.strip()

# Validate method and status
valid_methods = {'cash', 'credit_card', 'debit_card', 'insurance', 'online'}
valid_statuses = {'pending', 'completed', 'failed', 'refunded'}

df = df[df['method'].isin(valid_methods)]
df = df[df['payment_status'].isin(valid_statuses)]

# Drop rows with missing required fields
df.dropna(subset=['payment_id', 'patient_id', 'method', 'amount', 'payment_date'], inplace=True)

# Ensure positive amount
df = df[df['amount'] >= 0]

# Parse payment_date
df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
df.dropna(subset=['payment_date'], inplace=True)

# Convert IDs to int
df['payment_id'] = df['payment_id'].astype(int)
df['patient_id'] = df['patient_id'].astype(int)

# Handle optional IDs
for col in ['appointment_id', 'record_id', 'department_id']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].where(pd.notnull(df[col]), None)  # Keep as float/int or None

df['transaction_id'] = df['transaction_id'].where(pd.notnull(df['transaction_id']), None)

# Drop duplicates on payment_id
df.drop_duplicates(subset=['payment_id'], keep='first', inplace=True)

print(f"✅ Rows after cleaning: {len(df)}")

# === 6. Connect to SQL Server ===
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Successfully connected to the database.")
except Exception as e:
    print(f"❌ Failed to connect to SQL Server: {e}")
    exit()

# === 7. Fetch valid IDs from related tables ===
def fetch_column_values(query):
    try:
        cursor.execute(query)
        return {row[0] for row in cursor.fetchall() if row[0] is not None}
    except Exception as e:
        print(f"❌ Error executing query: {query[:50]}... -> {e}")
        return set()

valid_patients = fetch_column_values("SELECT patient_id FROM Patients")
valid_appointments = fetch_column_values("SELECT appointment_id FROM Appointments")
valid_records = fetch_column_values("SELECT record_id FROM Medical_Records")
valid_departments = fetch_column_values("SELECT department_id FROM Departments")
existing_payments = fetch_column_values(f"SELECT payment_id FROM {table}")

print(f"✅ Valid patients: {len(valid_patients)}")
print(f"✅ Valid appointments: {len(valid_appointments)}")
print(f"✅ Valid medical records: {len(valid_records)}")
print(f"✅ Valid departments: {len(valid_departments)}")
print(f"✅ Existing payments: {len(existing_payments)}")

# === 8. Prepare records for insertion ===
records_to_insert = []

for _, row in df.iterrows():
    payment_id = int(row['payment_id'])

    # Skip if already exists
    if payment_id in existing_payments:
        continue

    # Fix patient_id
    patient_id = int(row['patient_id'])
    if patient_id not in valid_patients:
        if valid_patients:
            patient_id = random.choice(list(valid_patients))
        else:
            print("❌ No valid patient IDs found in database. Cannot assign.")
            continue

    # Optional IDs: set to None if invalid
    appointment_id = int(row['appointment_id']) if pd.notna(row['appointment_id']) else None
    if appointment_id is not None and appointment_id not in valid_appointments:
        appointment_id = None

    record_id = int(row['record_id']) if pd.notna(row['record_id']) else None
    if record_id is not None and record_id not in valid_records:
        record_id = None

    department_id = int(row['department_id']) if pd.notna(row['department_id']) else None
    if department_id is not None and department_id not in valid_departments:
        department_id = None

    # Other fields
    method = row['method']
    amount = float(row['amount'])
    payment_date = row['payment_date'].strftime('%Y-%m-%d %H:%M:%S')
    payment_status = row['payment_status']
    transaction_id = row['transaction_id'] if row['transaction_id'] != 'None' and pd.notna(row['transaction_id']) else None

    records_to_insert.append((
        payment_id, patient_id, appointment_id, record_id,
        department_id, method, amount, payment_date,
        payment_status, transaction_id
    ))

if not records_to_insert:
    print("\n✅ No new records to insert.")
    conn.close()
    exit()

print(f"\n✅ Ready to insert {len(records_to_insert)} new records.")

# === 9. Insert records into the table ===
insert_query = f"""
INSERT INTO {table} (
    payment_id, patient_id, appointment_id, record_id, department_id,
    method, amount, payment_date, payment_status, transaction_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

success_count = 0
error_count = 0

for record in records_to_insert:
    try:
        cursor.execute(insert_query, record)
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record (payment_id={record[0]}): {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error for record (payment_id={record[0]}): {e}")
        error_count += 1

# === 10. Commit and close connection ===
try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success_count} records.")
    if error_count:
        print(f"⚠️ Failed to insert {error_count} records.")
except Exception as e:
    conn.rollback()
    print(f"❌ Commit failed: {e}")
finally:
    conn.close()
    print("✅ Connection closed.")