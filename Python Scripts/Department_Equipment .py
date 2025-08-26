import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'Department_Equipment'
folder_path = r'E:\instant\Project\EXCEL Care stat'
equipment_file = 'Equipment_data.csv'
departments_table_name = 'Departments' # To check foreign key

equipment_path = os.path.join(folder_path, equipment_file)

# === 2. Read Data ===
try:
    df_equipment = pd.read_csv(equipment_path)
    print(f"✅ Successfully read '{equipment_file}'.")
except Exception as e:
    print(f"❌ Error reading equipment file: {e}")
    exit()

# Basic cleaning for the incoming data
df_equipment['department_id'] = pd.to_numeric(df_equipment['department_id'], errors='coerce').astype('Int64')
df_equipment.dropna(subset=['department_id', 'equipment_name'], inplace=True)

# === 3. Connect to SQL Server and get existing data ===
try:
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};' # Ensure this driver is installed
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Successfully connected to the database.")
    
    # Get existing valid department_ids from the Departments table (for FK check)
    existing_dept_ids_query = f"SELECT department_id FROM {departments_table_name}"
    existing_dept_ids_df = pd.read_sql(existing_dept_ids_query, conn)
    valid_department_ids = set(existing_dept_ids_df['department_id'].astype(int))
    print(f"Found {len(valid_department_ids)} valid department IDs in the {departments_table_name} table.")

    # Get existing composite primary keys from Department_Equipment table
    existing_equipment_query = f"SELECT department_id, equipment_name FROM {table_name}"
    existing_equipment_df = pd.read_sql(existing_equipment_query, conn)
    existing_equipment_keys = set(tuple(row) for row in existing_equipment_df.values)
    print(f"Found {len(existing_equipment_keys)} existing equipment records in the {table_name} table.")

except Exception as e:
    print(f"❌ Failed to connect or read from the database: {e}")
    conn.close()
    exit()

# === 4. Identify Records That Need to Be Inserted ===
# Filter out records that are already in the database
records_to_insert_df = df_equipment[~df_equipment.apply(lambda x: (x['department_id'], str(x['equipment_name']).strip()) in existing_equipment_keys, axis=1)].copy()

if records_to_insert_df.empty:
    print("\n✅ No new records to insert. The table seems complete or all remaining records are duplicates.")
    conn.close()
    exit()

print(f"\nFound {len(records_to_insert_df)} records that need to be inserted (after filtering existing).")

# === 5. Force-Fix Foreign Keys and Composite Primary Keys ===
print("\n🔄 Starting force-fix process for remaining records...")

# --- Fix 1: Ensure department_id is valid (Foreign Key) ---
invalid_fk_mask = ~records_to_insert_df['department_id'].isin(valid_department_ids)
if invalid_fk_mask.any():
    print(f"  - Correcting {invalid_fk_mask.sum()} records with invalid 'department_id' (FK violation).")
    # Assign a random valid department_id
    records_to_insert_df.loc[invalid_fk_mask, 'department_id'] = [random.choice(list(valid_department_ids)) for _ in range(invalid_fk_mask.sum())]

# --- Fix 2: Ensure (department_id, equipment_name) is unique (Composite PK) ---
# This is the crucial part: we need to ensure uniqueness against ALL existing keys
# and also within the batch we are about to insert.

# Create a working set of all keys (existing + those we are about to insert)
current_batch_keys = set(existing_equipment_keys)

corrected_equipment_names = {}
for index, row in records_to_insert_df.iterrows():
    original_dept_id = row['department_id']
    original_equip_name = str(row['equipment_name']).strip()
    
    current_key = (original_dept_id, original_equip_name)
    
    suffix_counter = 1
    new_equip_name = original_equip_name
    
    # Loop until we find a unique key
    while current_key in current_batch_keys:
        new_equip_name = f"{original_equip_name}_{suffix_counter}"
        current_key = (original_dept_id, new_equip_name)
        suffix_counter += 1
    
    if new_equip_name != original_equip_name:
        corrected_equipment_names[index] = new_equip_name
    
    current_batch_keys.add(current_key) # Add the (potentially new) key to our working set

if corrected_equipment_names:
    print(f"  - Correcting {len(corrected_equipment_names)} records with duplicate (department_id, equipment_name) keys.")
    for index, new_name in corrected_equipment_names.items():
        records_to_insert_df.loc[index, 'equipment_name'] = new_name

print("✅ Force-fix process complete.")

# === 6. Insert the Corrected Records ===
insert_query = f"""
INSERT INTO {table_name} (
    department_id, equipment_name
) VALUES (?, ?)
"""

success_count = 0
error_count = 0
print(f"\n🔄 Starting data insertion into the table for {len(records_to_insert_df)} corrected records...")

for index, row in records_to_insert_df.iterrows():
    try:
        record_tuple = (
            row['department_id'],
            row['equipment_name']
        )
        cursor.execute(insert_query, record_tuple)
        success_count += 1
    except Exception as e:
        print(f"❌ Error inserting record (Dept ID={row['department_id']}, Equipment={row['equipment_name']}): {e}")
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





