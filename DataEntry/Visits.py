import pandas as pd
import pyodbc
import os
import random
from datetime import datetime

# === 1. Configuration ===
server = 'ALI_HANY'
database = 'Care_Stat'
table = 'Visits'

# File paths
folder_path = r'D:\instant\data analysis final\datainseart'
csv_name = 'Visit_data.csv'
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
required_cols = {'visit_id', 'patient_id', 'visit_date'}
missing_cols = required_cols - set(df.columns)
if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print(f"Available columns: {list(df.columns)}")
    exit()

# === 5. Data cleaning and type conversion ===
df['visit_id'] = pd.to_numeric(df['visit_id'], errors='coerce')
df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce')
df['visit_date'] = pd.to_datetime(df['visit_date'], errors='coerce', format='%Y-%m-%d')

# Drop invalid or duplicate visit_id
df.dropna(subset=['visit_id', 'patient_id', 'visit_date'], inplace=True)
df.drop_duplicates(subset=['visit_id'], keep='first', inplace=True)

# Convert to int after cleaning
df['visit_id'] = df['visit_id'].astype(int)
df['patient_id'] = df['patient_id'].astype(int)

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

# === 7. Fetch valid patient IDs and existing visit IDs ===
try:
    # Get valid patient IDs
    cursor.execute("SELECT patient_id FROM Patients")
    valid_patients = {row[0] for row in cursor.fetchall()}

    # Get existing visit IDs
    cursor.execute(f"SELECT visit_id FROM {table}")
    existing_visits = {row[0] for row in cursor.fetchall()}

    print(f"✅ Valid patients count: {len(valid_patients)}")
    print(f"✅ Existing visits count: {len(existing_visits)}")
except Exception as e:
    print(f"❌ Error fetching reference data: {e}")
    conn.close()
    exit()

# === 8. Prepare records for insertion ===
records_to_insert = []

for _, row in df.iterrows():
    vid = row['visit_id']
    pid = row['patient_id']
    vdate = row['visit_date'].strftime('%Y-%m-%d')  # Format as string

    # Skip if visit_id already exists
    if vid in existing_visits:
        continue

    # Fix invalid patient_id by assigning a random valid one
    if pid not in valid_patients:
        pid = random.choice(list(valid_patients))

    records_to_insert.append((vid, pid, vdate))

if not records_to_insert:
    print("\n✅ No new records to insert.")
    conn.close()
    exit()

print(f"\n✅ Ready to insert {len(records_to_insert)} new records.")

# === 9. Insert records into the table ===
insert_query = f"""
INSERT INTO {table} (visit_id, patient_id, visit_date)
VALUES (?, ?, ?)
"""

success_count = 0
error_count = 0

for record in records_to_insert:
    try:
        cursor.execute(insert_query, record)
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record {record}: {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error for record {record}: {e}")
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