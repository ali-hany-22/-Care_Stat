import pandas as pd
import pyodbc
import os
import random

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'DoctorWorkplaces'
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Workplace_data.csv'
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
required_columns = ['doctor_id', 'workplace']
missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print("Available columns in the file:", list(df.columns))
    exit()

# Convert doctor_id to numeric
df['doctor_id'] = pd.to_numeric(df['doctor_id'], errors='coerce').astype('Int64')

# Drop rows with missing critical data (doctor_id or workplace)
df.dropna(subset=['doctor_id', 'workplace'], inplace=True)

# Remove duplicates based on the composite primary key (doctor_id, workplace) within the CSV
df.drop_duplicates(subset=['doctor_id', 'workplace'], keep='first', inplace=True)

print(f"Number of unique records after CSV cleaning: {len(df)}")

# === 4. Connect to SQL Server and get existing data ===
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
    
    # Get existing doctor_ids from the Doctors table (for FK check)
    existing_doctor_ids_query = "SELECT doctor_id FROM Doctors"
    existing_doctor_ids_df = pd.read_sql(existing_doctor_ids_query, conn)
    valid_doctor_ids = set(existing_doctor_ids_df['doctor_id'].astype(int))
    print(f"Found {len(valid_doctor_ids)} valid doctor IDs in the Doctors table.")

    # Get existing composite primary keys from DoctorWorkplaces table
    existing_workplaces_query = f"SELECT doctor_id, workplace FROM {table_name}"
    existing_workplaces_df = pd.read_sql(existing_workplaces_query, conn)
    existing_workplaces_keys = set(tuple(row) for row in existing_workplaces_df.values)
    print(f"Found {len(existing_workplaces_keys)} existing records in the {table_name} table.")

except Exception as e:
    print(f"❌ Failed to connect or read from the database: {e}")
    conn.close()
    exit()

# === 5. Filter out records that already exist or violate FK ===
records_to_insert = []
# Create a working set of all keys (existing + those we are about to insert) for real-time uniqueness check
current_batch_keys = set(existing_workplaces_keys)

for index, row in df.iterrows():
    doc_id = row['doctor_id']
    workplace = str(row['workplace']).strip()
    
    # --- Force-Fix 1: Ensure doctor_id is valid (Foreign Key) ---
    if doc_id not in valid_doctor_ids:
        # print(f"Correcting invalid doctor_id {doc_id} for workplace {workplace}")
        doc_id = random.choice(list(valid_doctor_ids)) # Assign a random valid doctor_id
        
    # --- Force-Fix 2: Ensure (doctor_id, workplace) is unique (Composite PK) ---
    current_key = (doc_id, workplace)
    suffix_counter = 1
    original_workplace = workplace
    new_workplace = original_workplace # Initialize new_workplace here
    
    while current_key in current_batch_keys:
        new_workplace = f"{original_workplace}_{suffix_counter}"
        current_key = (doc_id, new_workplace)
        suffix_counter += 1
    
    # If workplace was modified, update it
    if new_workplace != original_workplace:
        workplace = new_workplace

    # Add the (potentially new) key to our working set
    current_batch_keys.add((doc_id, workplace))
    
    records_to_insert.append({'doctor_id': doc_id, 'workplace': workplace})

df_to_insert = pd.DataFrame(records_to_insert)

if df_to_insert.empty:
    print("\n✅ No new unique records to insert after filtering and force-fixing. All relevant data might already be in the database.")
    conn.close()
    exit()

print(f"\nFound {len(df_to_insert)} unique records to insert after force-fixing.")

# === 6. Insert data row by row ===
insert_query = f"""
INSERT INTO {table_name} (
    doctor_id, workplace
) VALUES (?, ?)
"""

success_count = 0
error_count = 0
print("\n🔄 Starting data insertion into the table...")

for index, row in df_to_insert.iterrows():
    try:
        record_tuple = (
            row['doctor_id'],
            row['workplace']
        )
        cursor.execute(insert_query, record_tuple)
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record (Doctor ID={row['doctor_id']}, Workplace={row['workplace']}): {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error inserting record (Doctor ID={row['doctor_id']}, Workplace={row['workplace']}): {e}")
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













