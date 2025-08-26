import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'Medical_Records'
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Medical_record_data.csv'
file_path = os.path.join(folder_path, file_name)

# === 2. Read CSV file ===
if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    try:
        print("Files in the specified directory:")
        print(os.listdir(folder_path))
    except Exception as e:
        print(f"Error reading directory contents: {e}")
    exit()

try:
    df = pd.read_csv(file_path)
    print(f"✅ Successfully read file: {file_name}")
    print(f"Number of records in CSV: {len(df)}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit()

# === 3. Basic validation and cleaning ===
required_columns = ["record_id", "patient_id", "doctor_id", "department_id", "diagnosis", "severity_level", "prescription_cost", "record_date"]
missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print("Available columns in the file:", list(df.columns))
    exit()

# Convert numeric columns
df["record_id"] = pd.to_numeric(df["record_id"], errors="coerce").astype("Int64")
df["patient_id"] = pd.to_numeric(df["patient_id"], errors="coerce").astype("Int64")
df["doctor_id"] = pd.to_numeric(df["doctor_id"], errors="coerce").astype("Int64")
df["department_id"] = pd.to_numeric(df["department_id"], errors="coerce").astype("Int64")
df["prescription_cost"] = pd.to_numeric(df["prescription_cost"], errors="coerce")

# Convert record_date to datetime
df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

# Drop rows with missing critical data (record_id, patient_id, doctor_id, department_id, record_date)
df.dropna(subset=["record_id", "patient_id", "doctor_id", "department_id", "record_date"], inplace=True)

# Remove duplicates based on the primary key (record_id) within the CSV
df.drop_duplicates(subset=["record_id"], keep="first", inplace=True)

print(f"Number of unique records after CSV cleaning: {len(df)}")

# === 4. Connect to SQL Server and get existing data ===

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
    
    # Get existing record_ids from Medical_Records table
    existing_record_ids_query = f"SELECT record_id FROM {table_name}"
    existing_record_ids_df = pd.read_sql(existing_record_ids_query, conn)
    existing_record_ids = set(existing_record_ids_df["record_id"].astype(int))
    print(f"Found {len(existing_record_ids)} existing record IDs in the {table_name} table.")

    # Get existing patient_ids from Patients table (for FK check)
    existing_patient_ids_query = "SELECT patient_id FROM Patients"
    existing_patient_ids_df = pd.read_sql(existing_patient_ids_query, conn)
    valid_patient_ids = set(existing_patient_ids_df["patient_id"].astype(int))
    print(f"Found {len(valid_patient_ids)} valid patient IDs in the Patients table.")

    # Get existing doctor_ids from Doctors table (for FK check)
    existing_doctor_ids_query = "SELECT doctor_id FROM Doctors"
    existing_doctor_ids_df = pd.read_sql(existing_doctor_ids_query, conn)
    valid_doctor_ids = set(existing_doctor_ids_df["doctor_id"].astype(int))
    print(f"Found {len(valid_doctor_ids)} valid doctor IDs in the Doctors table.")

    # Get existing department_ids from Departments table (for FK check)
    existing_department_ids_query = "SELECT department_id FROM Departments"
    existing_department_ids_df = pd.read_sql(existing_department_ids_query, conn)
    valid_department_ids = set(existing_department_ids_df["department_id"].astype(int))
    print(f"Found {len(valid_department_ids)} valid department IDs in the Departments table.")

except Exception as e:
    print(f"❌ Failed to connect or read from the database: {e}")
    conn.close()
    exit()

# === 5. Filter out records that already exist and force-fix others ===
records_to_insert = []
max_existing_record_id = max(existing_record_ids) if existing_record_ids else 0
next_record_id = max_existing_record_id + 1

valid_severity_levels = {"low", "moderate", "high", "critical"}

for index, row in df.iterrows():
    rec_id = row["record_id"]
    pat_id = row["patient_id"]
    doc_id = row["doctor_id"]
    dept_id = row["department_id"]
    diagnosis = str(row["diagnosis"]).strip()
    severity_level = str(row["severity_level"]).strip().lower()
    prescription_cost = row["prescription_cost"]
    record_date = row["record_date"]

    # --- Force-Fix 1: Ensure record_id is unique (Primary Key) ---
    if rec_id in existing_record_ids:
        rec_id = next_record_id
        next_record_id += 1
    existing_record_ids.add(rec_id) # Add to set to handle duplicates within the current batch

    # --- Force-Fix 2: Ensure patient_id is valid (Foreign Key) ---
    if pat_id not in valid_patient_ids:
        pat_id = random.choice(list(valid_patient_ids)) # Assign a random valid patient_id
        
    # --- Force-Fix 3: Ensure doctor_id is valid (Foreign Key) ---
    if doc_id not in valid_doctor_ids:
        doc_id = random.choice(list(valid_doctor_ids)) # Assign a random valid doctor_id

    # --- Force-Fix 4: Ensure department_id is valid (Foreign Key) ---
    if dept_id not in valid_department_ids:
        dept_id = random.choice(list(valid_department_ids)) # Assign a random valid department_id

    # --- Force-Fix 5: Ensure severity_level is valid (Check Constraint) ---
    if severity_level not in valid_severity_levels:
        severity_level = random.choice(list(valid_severity_levels)) # Assign a random valid severity_level

    # --- Force-Fix 6: Ensure prescription_cost is valid (Check Constraint) ---
    if pd.isna(prescription_cost) or prescription_cost < 0:
        prescription_cost = round(random.uniform(10.0, 500.0), 2) # Assign a random valid cost

    records_to_insert.append({
        "record_id": rec_id,
        "patient_id": pat_id,
        "doctor_id": doc_id,
        "department_id": dept_id,
        "diagnosis": diagnosis,
        "severity_level": severity_level,
        "prescription_cost": prescription_cost,
        "record_date": record_date,
    })

df_to_insert = pd.DataFrame(records_to_insert)

if df_to_insert.empty:
    print("\n✅ No new unique records to insert after filtering and force-fixing. All relevant data might already be in the database.")
    conn.close()
    exit()

print(f"\nFound {len(df_to_insert)} unique records to insert after force-fixing.")

# === 6. Insert data row by row ===
insert_query = f"""
INSERT INTO {table_name} (
    record_id, patient_id, doctor_id, department_id, diagnosis, severity_level, prescription_cost, record_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

success_count = 0
error_count = 0
print("\n🔄 Starting data insertion into the table...")

for index, row in df_to_insert.iterrows():
    try:
        record_tuple = (
            row["record_id"],
            row["patient_id"],
            row["doctor_id"],
            row["department_id"],
            row["diagnosis"],
            row["severity_level"],
            row["prescription_cost"],
            row["record_date"],
        )
        cursor.execute(insert_query, record_tuple)
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record (Record ID={row["record_id"]}, Patient ID={row["patient_id"]}, Doctor ID={row["doctor_id"]}, Dept ID={row["department_id"]}): {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error inserting record (Record ID={row["record_id"]}, Patient ID={row["patient_id"]}, Doctor ID={row["doctor_id"]}, Dept ID={row["department_id"]}): {e}")
        error_count += 1

# === 7. Commit changes and close connection ===
try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success_count} new records.")
    if error_count > 0:
        print(f"⚠️ Failed to insert {error_count} records due to database errors.")
except Exception as e:
    print(f"❌ Error committing changes: {e}")
    conn.rollback()

conn.close()
print("✅ Script execution completed and connection closed.")









