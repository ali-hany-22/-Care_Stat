import pandas as pd
import pyodbc
import os
import random
from datetime import datetime

# === 1. Connection & file settings ===
SERVER   = r'ALI\SQLEXPRESS'          # Change if needed
DATABASE = 'Care_Stat'
TABLE    = 'Payments'

CSV_FOLDER = r'E:\instant\Project\EXCEL Care stat'  # Change to your CSV folder
CSV_NAME   = 'payment_data.csv'
CSV_PATH   = os.path.join(CSV_FOLDER, CSV_NAME)

# === 2. Read CSV ===
if not os.path.exists(CSV_PATH):
    print(f'❌ File not found: {CSV_PATH}')
    exit()

try:
    df = pd.read_csv(CSV_PATH)
    print(f'✅ Loaded {len(df)} rows from {CSV_NAME}')
except Exception as e:
    print(f'❌ Error reading CSV: {e}')
    exit()

# === 3. Validate & clean columns ===
required_cols = {
    'payment_id', 'patient_id', 'method', 'amount',
    'payment_date', 'payment_status'
}
optional_cols = {'appointment_id', 'record_id', 'department_id', 'transaction_id'}
missing = required_cols - set(df.columns)
if missing:
    print(f'❌ Missing required columns: {missing}')
    exit()

# Basic type casting
df['payment_id']    = pd.to_numeric(df['payment_id'], errors='coerce').astype('Int64')
df['patient_id']    = pd.to_numeric(df['patient_id'], errors='coerce').astype('Int64')
df['appointment_id']= pd.to_numeric(df['appointment_id'], errors='coerce').astype('Int64')
df['record_id']     = pd.to_numeric(df['record_id'], errors='coerce').astype('Int64')
df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce').astype('Int64')
df['amount']        = pd.to_numeric(df['amount'], errors='coerce')

# Normalize strings
df['method']         = df['method'].astype(str).str.strip().str.lower()
df['payment_status'] = df['payment_status'].astype(str).str.strip().str.lower()
df['transaction_id'] = df['transaction_id'].astype(str).str.strip()

# Drop rows with NULL required keys
df.dropna(subset=['payment_id', 'patient_id', 'method', 'amount', 'payment_date'], inplace=True)

# Enforce valid value lists
valid_methods  = {'cash', 'credit_card', 'debit_card', 'insurance', 'online'}
valid_statuses = {'pending', 'completed', 'failed', 'refunded'}
df = df[df['method'].isin(valid_methods)]
df = df[df['payment_status'].isin(valid_statuses)]

# Ensure positive amount
df = df[df['amount'] >= 0]

# Parse payment_date
df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
df.dropna(subset=['payment_date'], inplace=True)

# Drop duplicates on payment_id
df.drop_duplicates(subset=['payment_id'], keep='first', inplace=True)

print(f'Rows after cleaning: {len(df)}')

# === 4. Connect to SQL Server & fetch valid IDs ===
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(conn_str, autocommit=False)
    cursor = conn.cursor()
    print('✅ Connected to SQL Server')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit()

def fetch_ids(query):
    return set(pd.read_sql(query, conn).iloc[:, 0].astype(int))

valid_patients    = fetch_ids('SELECT patient_id FROM Patients')
valid_appointments= fetch_ids('SELECT appointment_id FROM Appointments') if 'appointment_id' in df.columns else set()
valid_records     = fetch_ids('SELECT record_id FROM Medical_Records') if 'record_id' in df.columns else set()
valid_departments = fetch_ids('SELECT department_id FROM Departments') if 'department_id' in df.columns else set()
existing_payments = fetch_ids(f'SELECT payment_id FROM {TABLE}')

print(f'Valid patients: {len(valid_patients)}')
print(f'Existing payment IDs: {len(existing_payments)}')

# === 5. Force-fix invalid IDs ===
records_to_insert = []

for _, row in df.iterrows():
    pid   = int(row['payment_id'])
    patid = int(row['patient_id'])

    if pid in existing_payments:   # Skip duplicates
        continue
    if patid not in valid_patients:
        patid = random.choice(list(valid_patients))

    aid = int(row['appointment_id']) if pd.notna(row['appointment_id']) else None
    if aid is not None and aid not in valid_appointments:
        aid = None

    rid = int(row['record_id']) if pd.notna(row['record_id']) else None
    if rid is not None and rid not in valid_records:
        rid = None

    did = int(row['department_id']) if pd.notna(row['department_id']) else None
    if did is not None and did not in valid_departments:
        did = None

    method   = row['method']
    amount   = float(row['amount'])
    paydate  = row['payment_date'].strftime('%Y-%m-%d %H:%M:%S')
    status   = row['payment_status']
    transid  = row['transaction_id'] if pd.notna(row['transaction_id']) and row['transaction_id'] else None

    records_to_insert.append(
        (pid, patid, aid, rid, did, method, amount, paydate, status, transid)
    )

if not records_to_insert:
    print('\n✅ No new records to insert.')
    conn.close()
    exit()

print(f'\nReady to insert {len(records_to_insert)} records.')

# === 6. Insert into table ===
insert_sql = f"""
INSERT INTO {TABLE} (
    payment_id, patient_id, appointment_id, record_id, department_id,
    method, amount, payment_date, payment_status, transaction_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

success = errors = 0
for rec in records_to_insert:
    try:
        cursor.execute(insert_sql, rec)
        success += 1
    except pyodbc.IntegrityError as ie:
        print(f'⚠️ Duplicate or constraint error: {rec} -> {ie}')
        errors += 1
    except Exception as ex:
        print(f'❌ Unexpected error: {rec} -> {ex}')
        errors += 1

# === 7. Commit & close ===
try:
    conn.commit()
    print(f'\n✅ Successfully inserted {success} records.')
    if errors:
        print(f'⚠️ Failed to insert {errors} records.')
except Exception as e:
    conn.rollback()
    print(f'❌ Commit failed: {e}')
finally:
    conn.close()
    print('✅ Connection closed.')