import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI_HANY'
database = 'Care_Stat'
table = 'PatientPhones'

# Paths
csv_folder = r'D:\instant\data analysis final\datainseart'  # ⚠️ تم تعريفه الآن
csv_name = 'Phone_patient.csv'
csv_path = os.path.join(csv_folder, csv_name)

# === 2. Check and read CSV file ===
if not os.path.exists(csv_path):
    print(f"❌ File not found: {csv_path}")
    exit()

try:
    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} rows from {csv_name}")
except Exception as e:
    print(f"❌ Error reading CSV file: {e}")
    exit()

# === 3. Validate required columns ===
required_cols = {'patient_id', 'phone'}
missing_cols = required_cols - set(df.columns)
if missing_cols:
    print(f"❌ Missing columns: {missing_cols}")
    exit()

# === 4. Data cleaning ===
df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce')
df['phone'] = df['phone'].astype(str).str.strip()

# Drop rows with invalid patient_id or missing data
df.dropna(subset=['patient_id', 'phone'], inplace=True)
df['patient_id'] = df['patient_id'].astype(int)

# Keep only valid 11-digit numeric Egyptian-style phone numbers
df = df[df['phone'].str.len() == 11]
df = df[df['phone'].str.isdigit()]

# Remove duplicates (same patient + same phone)
df.drop_duplicates(subset=['patient_id', 'phone'], keep='first', inplace=True)

print(f"✅ Rows after cleaning: {len(df)}")

# === 5. Connect to SQL Server ===
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

# === 6. Fetch valid patient IDs and existing (patient_id, phone) pairs ===
try:
    # Get all valid patient IDs from Patients table
    cursor.execute("SELECT patient_id FROM Patients")
    valid_patients = {row[0] for row in cursor.fetchall()}

    # Get all existing (patient_id, phone) pairs from PatientPhones
    cursor.execute(f"SELECT patient_id, phone FROM {table}")
    existing_pairs = {(row[0], row[1]) for row in cursor.fetchall()}

    print(f"✅ Found {len(valid_patients)} valid patients.")
    print(f"✅ Found {len(existing_pairs)} existing phone records.")
except Exception as e:
    print(f"❌ Error fetching reference data: {e}")
    conn.close()
    exit()

# === 7. Prepare records for insertion ===
records_to_insert = []

for _, row in df.iterrows():
    pid = int(row['patient_id'])
    phone = str(row['phone'])

    # Skip if this (patient_id, phone) already exists
    if (pid, phone) in existing_pairs:
        continue

    # If patient_id is invalid, assign a random valid one
    if pid not in valid_patients:
        pid = random.choice(list(valid_patients))

    records_to_insert.append((pid, phone))

if not records_to_insert:
    print("\n✅ No new records to insert.")
    conn.close()
    exit()

print(f"\n✅ Ready to insert {len(records_to_insert)} new records.")

# === 8. Insert records into the table ===
insert_query = f"INSERT INTO {table} (patient_id, phone) VALUES (?, ?)"

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

# === 9. Commit and close ===
try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success_count} records.")
    if error_count:
        print(f"⚠️ Failed to insert {error_count} records.")
except Exception as e:
    conn.rollback()
    print(f"❌ Transaction rolled back due to error: {e}")
finally:
    conn.close()
    print("✅ Database connection closed.")