import pandas as pd
import pyodbc
import os
import random
from datetime import datetime

# === 1. Connection & file settings ===
SERVER   = r'ALI\SQLEXPRESS'
DATABASE = 'Care_Stat'
TABLE    = 'Visits'

CSV_FOLDER = r'E:\instant\Project\EXCEL Care stat'
CSV_NAME   = 'Visit_data.csv'  
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
required_cols = {'visit_id', 'patient_id', 'visit_date'}
missing = required_cols - set(df.columns)
if missing:
    print(f'❌ Missing required columns: {missing}')
    exit()

# Cast types
df['visit_id']   = pd.to_numeric(df['visit_id'], errors='coerce').astype('Int64')
df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce').astype('Int64')
df['visit_date'] = pd.to_datetime(df['visit_date'], errors='coerce')

# Drop invalid rows
df.dropna(subset=['visit_id', 'patient_id', 'visit_date'], inplace=True)
df.drop_duplicates(subset=['visit_id'], inplace=True)

print(f'Rows after cleaning: {len(df)}')

# === 4. Connect to SQL Server & fetch valid patient IDs ===
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

valid_patients   = fetch_ids('SELECT patient_id FROM Patients')
existing_visits  = fetch_ids(f'SELECT visit_id FROM {TABLE}')

print(f'Valid patients: {len(valid_patients)}')
print(f'Existing visit IDs: {len(existing_visits)}')

# === 5. Prepare records to insert ===
records_to_insert = []

for _, row in df.iterrows():
    vid = int(row['visit_id'])
    pid = int(row['patient_id'])

    if vid in existing_visits:
        continue
    if pid not in valid_patients:
        pid = random.choice(list(valid_patients))

    vdate = row['visit_date'].strftime('%Y-%m-%d')

    records_to_insert.append((vid, pid, vdate))

if not records_to_insert:
    print('\n✅ No new records to insert.')
    conn.close()
    exit()

print(f'\nReady to insert {len(records_to_insert)} records.')

# === 6. Insert into table ===
insert_sql = f"""
INSERT INTO {TABLE} (visit_id, patient_id, visit_date)
VALUES (?, ?, ?)
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