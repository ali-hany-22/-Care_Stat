import pandas as pd
import pyodbc
import os
import random

# === 1. Connection & file settings ===
SERVER   = r'ALI\SQLEXPRESS'           # Change if needed
DATABASE = 'Care_Stat'
TABLE    = 'PatientPhones'

CSV_FOLDER = r'E:\instant\Project\EXCEL Care stat'  # Change to your CSV folder
CSV_NAME   = 'Phone_patient.csv'
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
required_cols = {'phone', 'patient_id'}
missing = required_cols - set(df.columns)
if missing:
    print(f'❌ Missing columns: {missing}')
    exit()

df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce').astype('Int64')
df['phone']      = df['phone'].astype(str).str.strip()

# Drop NULL keys & duplicates
df.dropna(subset=['patient_id', 'phone'], inplace=True)
df.drop_duplicates(subset=['patient_id', 'phone'], keep='first', inplace=True)

# Keep only 11-digit numeric phones
df = df[df['phone'].str.len() == 11]
df = df[df['phone'].str.isdigit()]

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

valid_patients = fetch_ids('SELECT patient_id FROM Patients')
existing_pairs = fetch_ids(f'SELECT patient_id FROM {TABLE}')

print(f'Valid patients: {len(valid_patients)} | Existing pairs: {len(existing_pairs)}')

# === 5. Filter & force-fix invalid patient IDs ===
records_to_insert = []

for _, row in df.iterrows():
    pid   = int(row['patient_id'])
    phone = str(row['phone'])

    # Skip if pair already exists
    if (pid, phone) in {(p, ph) for p, ph in existing_pairs}:
        continue

    # Fix invalid patient_id
    if pid not in valid_patients:
        pid = random.choice(list(valid_patients))

    records_to_insert.append((pid, phone))

if not records_to_insert:
    print('\n✅ No new records to insert.')
    conn.close()
    exit()

print(f'\nReady to insert {len(records_to_insert)} records.')

# === 6. Insert into table ===
insert_sql = f"""
INSERT INTO {TABLE} (patient_id, phone)
VALUES (?, ?)
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