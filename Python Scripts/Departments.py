import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'Departments'
folder_path = r'E:\instant\Project\EXCEL Care stat'
doctors_file = 'Doctor_data.csv'
departments_file = 'Department_data.csv'

doctors_path = os.path.join(folder_path, doctors_file)
departments_path = os.path.join(folder_path, departments_file)

# === 2. Read Data ===
try:
    df_doctors = pd.read_csv(doctors_path)
    df_departments = pd.read_csv(departments_path)
    print(f"✅ Successfully read source files.")
except Exception as e:
    print(f"❌ Error reading files: {e}")
    exit()

# --- Prepare data ---
valid_doctor_ids = list(df_doctors['doctor_id'].unique())
df_departments.rename(columns={'doctor_id': 'head_doctor_id'}, inplace=True)

# === 3. Connect to SQL Server and get existing data ===
try:
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("\n✅ Successfully connected to the database.")
    
    # Get ALL existing data to avoid any conflicts
    existing_depts_df = pd.read_sql(f"SELECT department_id, department_code FROM {table_name}", conn)
    existing_dept_ids = set(existing_depts_df['department_id'])
    existing_dept_codes = set(existing_depts_df['department_code'])
    
    # Find the maximum existing ID to generate new ones safely
    max_existing_id = 0
    if not existing_depts_df.empty:
        max_existing_id = existing_depts_df['department_id'].max()
        
    print(f"Found {len(existing_dept_ids)} records in the database. Max ID is {max_existing_id}.")

except Exception as e:
    print(f"❌ Failed to connect or read from the database: {e}")
    exit()

# === 4. Isolate and Correct Records That Failed Previously ===
# We only care about records whose original department_id is NOT in the database yet.
records_to_insert_df = df_departments[~df_departments['department_id'].isin(existing_dept_ids)].copy()

if records_to_insert_df.empty:
    print("\n✅ No new records to insert. The table seems complete.")
    conn.close()
    exit()

print(f"\nFound {len(records_to_insert_df)} records that failed previously. Preparing them for insertion...")

# --- This is the new, more robust correction logic ---
print("🔄 Generating new unique IDs and Codes for all failed records...")
new_id_counter = max_existing_id + 1
corrected_rows = []

for index, row in records_to_insert_df.iterrows():
    new_row = row.copy()
    
    # Generate a new, guaranteed unique department_id
    new_row['department_id'] = new_id_counter
    
    # Generate a new, guaranteed unique department_code
    original_code = new_row['department_code']
    new_code = f"{original_code}_{new_id_counter}" # Append the new unique ID to the code
    new_row['department_code'] = new_code
    
    # Ensure head_doctor_id is valid
    if new_row['head_doctor_id'] not in valid_doctor_ids:
        new_row['head_doctor_id'] = random.choice(valid_doctor_ids)
        
    corrected_rows.append(new_row)
    new_id_counter += 1

# Create a new DataFrame with the fully corrected data
df_corrected = pd.DataFrame(corrected_rows)
print("✅ All failed records have been assigned new unique IDs and codes.")

# === 5. Insert the Corrected Batch ===
insert_query = f"""
INSERT INTO {table_name} (
    department_id, department_name, department_code, head_doctor_id,
    current_occupancy, max_capacity, num_staff, working_hours, emergency_support
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

success_count = 0
error_count = 0
print(f"\n🔄 Inserting the {len(df_corrected)} corrected records...")

for index, row in df_corrected.iterrows():
    try:
        record_tuple = (
            row['department_id'], row['department_name'], row['department_code'],
            row['head_doctor_id'], row['current_occupancy'], row['max_capacity'],
            row['num_staff'], row['working_hours'], bool(row['emergency_support'])
        )
        cursor.execute(insert_query, record_tuple)
        success_count += 1
    except Exception as e:
        print(f"❌ FINAL ATTEMPT FAILED for new ID ({row['department_id']}): {e}")
        error_count += 1

# === 6. Commit and Close ===
try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success_count} new records.")
    if error_count > 0:
        print(f"⚠️ Failed to insert {error_count} records.")
except Exception as e:
    print(f"❌ Error committing changes: {e}")
    conn.rollback()

conn.close()
print("✅ Script execution completed and connection closed.")









