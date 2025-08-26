import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'DoctorDepartment'
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Department_workload.csv'
file_path = os.path.join(folder_path, file_name)

# === 2. Read CSV ===
if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    try:
        print('Files in the specified directory:')
        print(os.listdir(folder_path))
    except Exception as e:
        print(f'Error reading directory contents: {e}')
    raise SystemExit(1)

try:
    df = pd.read_csv(file_path)
    print(f"✅ Successfully read file: {file_name}")
    print(f"Number of records in CSV: {len(df)}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    raise SystemExit(1)

# === 3. Basic validation ===
required_columns = ['doctor_id', 'department_id', 'workload_hours_week']
missing = [c for c in required_columns if c not in df.columns]
if missing:
    print(f"❌ Missing required columns: {missing}")
    print(f"Available columns: {list(df.columns)}")
    raise SystemExit(1)

# === 4. Clean & normalize types ===
df['doctor_id'] = pd.to_numeric(df['doctor_id'], errors='coerce').astype('Int64')
df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce').astype('Int64')
df['workload_hours_week'] = pd.to_numeric(df['workload_hours_week'], errors='coerce')

# Drop rows missing mandatory keys (we will remap later if needed)
df.dropna(subset=['doctor_id', 'department_id'], inplace=True)

# Fix invalid workload values according to CHECK (>= 0)
# Strategy: fill NaN or negative values with a reasonable random value between 10 and 60
invalid_mask = df['workload_hours_week'].isna() | (df['workload_hours_week'] < 0)
if invalid_mask.any():
    count_invalid = int(invalid_mask.sum())
    print(f"ℹ️ Fixing {count_invalid} invalid workload values (NaN or negative)")
    replacement_values = [random.randint(10, 60) for _ in range(count_invalid)]
    df.loc[invalid_mask, 'workload_hours_week'] = replacement_values

# Convert to integer type safely
df['workload_hours_week'] = df['workload_hours_week'].round().astype(int)

# Remove duplicates on composite key within the CSV (keep first)
df.drop_duplicates(subset=['doctor_id', 'department_id'], keep='first', inplace=True)
print(f"Number of unique records after CSV cleaning: {len(df)}")

# === 5. Connect to SQL Server and fetch reference data ===
try:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Successfully connected to the database.")

    # Existing Doctors
    doctors_df = pd.read_sql("SELECT doctor_id FROM Doctors", conn)
    valid_doctor_ids = set(doctors_df['doctor_id'].astype(int))
    print(f"Found {len(valid_doctor_ids)} valid doctor IDs in Doctors table.")

    # Existing Departments
    depts_df = pd.read_sql("SELECT department_id FROM Departments", conn)
    valid_department_ids = set(depts_df['department_id'].astype(int))
    print(f"Found {len(valid_department_ids)} valid department IDs in Departments table.")

    # Existing composite keys in DoctorDepartment
    existing_keys_df = pd.read_sql(f"SELECT doctor_id, department_id FROM {table_name}", conn)
    existing_keys = set(tuple(row) for row in existing_keys_df.values)
    print(f"Found {len(existing_keys)} existing records in {table_name}.")

except Exception as e:
    print(f"❌ Failed to connect or read reference data: {e}")
    try:
        conn.close()
    except Exception:
        pass
    raise SystemExit(1)

# === 6. Build corrected batch ensuring FK validity and composite uniqueness ===
print("\n🔄 Starting force-fix process for remaining records...")

records_to_insert = []
seen_batch_keys = set(existing_keys)  # start with existing

for _, row in df.iterrows():
    doc_id = int(row['doctor_id']) if pd.notna(row['doctor_id']) else None
    dept_id = int(row['department_id']) if pd.notna(row['department_id']) else None
    hours = int(row['workload_hours_week']) if pd.notna(row['workload_hours_week']) else random.randint(10, 60)

    # Remap invalid doctor_id
    if (doc_id is None) or (doc_id not in valid_doctor_ids):
        doc_id = random.choice(list(valid_doctor_ids))

    # Remap invalid department_id
    if (dept_id is None) or (dept_id not in valid_department_ids):
        dept_id = random.choice(list(valid_department_ids))

    # Ensure composite (doc_id, dept_id) uniqueness across DB and this batch
    key = (doc_id, dept_id)
    guard = 0
    while key in seen_batch_keys and guard < 1000:
        # try a different random department to avoid collision
        dept_id = random.choice(list(valid_department_ids))
        key = (doc_id, dept_id)
        guard += 1

    if guard >= 1000 and key in seen_batch_keys:
        # fallback: try changing doctor instead
        doc_id = random.choice(list(valid_doctor_ids))
        key = (doc_id, dept_id)
        inner_guard = 0
        while key in seen_batch_keys and inner_guard < 1000:
            dept_id = random.choice(list(valid_department_ids))
            key = (doc_id, dept_id)
            inner_guard += 1

    # Double-check workload constraint
    if hours < 0:
        hours = abs(hours)
    
    seen_batch_keys.add(key)
    records_to_insert.append((doc_id, dept_id, hours))

print(f"Prepared {len(records_to_insert)} corrected records for insertion.")

# === 7. Insert corrected batch ===
insert_sql = f"""
INSERT INTO {table_name} (doctor_id, department_id, workload_hours_week) VALUES (?, ?, ?)
"""

success, failed = 0, 0
for (doc_id, dept_id, hours) in records_to_insert:
    try:
        cursor.execute(insert_sql, (doc_id, dept_id, hours))
        success += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for (Doctor ID={doc_id}, Dept ID={dept_id}): {e}")
        failed += 1
    except Exception as e:
        print(f"❌ Unexpected error for (Doctor ID={doc_id}, Dept ID={dept_id}): {e}")
        failed += 1

try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success} records.")
    if failed:
        print(f"⚠️ Failed to insert {failed} records.")
except Exception as e:
    print(f"❌ Error committing changes: {e}")
    conn.rollback()

conn.close()
print("✅ Script execution completed and connection closed.")





