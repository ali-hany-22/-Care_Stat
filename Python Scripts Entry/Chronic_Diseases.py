import pandas as pd
import pyodbc
import os

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'ChronicDiseases'
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Disease_data.csv'
file_path = os.path.join(folder_path, file_name)

# === 2. Check if the file exists ===
if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    try:
        print("Files in the specified directory:")
        print(os.listdir(folder_path))
    except Exception as e:
        print(f"Error reading directory contents: {e}")
    exit()

# === 3. Read CSV file ===
try:
    df = pd.read_csv(file_path)
    print(f"✅ Successfully read file: {file_name}")
    print(f"Number of records in CSV: {len(df)}")
    # Ensure disease_id is integer for comparison
    df['disease_id'] = pd.to_numeric(df['disease_id'], errors='coerce').astype('Int64')
    df.dropna(subset=['disease_id', 'disease_name'], inplace=True) # Drop rows with missing critical data
    df.drop_duplicates(subset=['disease_id'], keep='first', inplace=True) # Remove duplicate disease_id in CSV
    df.drop_duplicates(subset=['disease_name'], keep='first', inplace=True) # Remove duplicate disease_name in CSV
    print(f"Number of unique records after CSV cleaning: {len(df)}")
except Exception as e:
    print(f"❌ Error reading or cleaning file: {e}")
    exit()

# === 4. Connect to SQL Server and get existing data ===
try:
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Successfully connected to the database.")
    
    # Get existing disease_ids and disease_names from the database
    existing_data_query = f"SELECT disease_id, disease_name FROM {table_name}"
    existing_df = pd.read_sql(existing_data_query, conn)
    
    existing_disease_ids = set(existing_df['disease_id'].astype(int))
    existing_disease_names = set(existing_df['disease_name'].str.lower())
    
    print(f"Found {len(existing_disease_ids)} existing disease IDs and {len(existing_disease_names)} existing disease names in the database.")

except Exception as e:
    print(f"❌ Failed to connect or read from the database: {e}")
    conn.close()
    exit()

# === 5. Filter out records that already exist or are duplicates ===
records_to_insert = []
for index, row in df.iterrows():
    disease_id = row['disease_id']
    disease_name = str(row['disease_name']).lower()
    
    # Check if disease_id or disease_name already exists in DB
    if disease_id in existing_disease_ids:
        # print(f"Skipping record {disease_id} - ID already exists in DB.")
        continue
    if disease_name in existing_disease_names:
        # print(f"Skipping record {disease_name} - Name already exists in DB.")
        continue
    
    records_to_insert.append(row)
    existing_disease_ids.add(disease_id) # Add to set to handle duplicates within the current batch
    existing_disease_names.add(disease_name) # Add to set to handle duplicates within the current batch

df_to_insert = pd.DataFrame(records_to_insert)

if df_to_insert.empty:
    print("\n✅ No new unique records to insert after filtering. All relevant data might already be in the database.")
    conn.close()
    exit()

print(f"\nFound {len(df_to_insert)} unique records to insert after filtering against existing data.")

# === 6. Insert data row by row ===
insert_query = f"""
INSERT INTO {table_name} (
    disease_id, disease_name
) VALUES (?, ?)
"""

success_count = 0
error_count = 0
print("\n🔄 Starting data insertion into the table...")

for index, row in df_to_insert.iterrows():
    try:
        record_tuple = (
            row['disease_id'],
            row['disease_name']
        )
        cursor.execute(insert_query, record_tuple)
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record (ID={row['disease_id']}, Name={row['disease_name']}): {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error inserting record (ID={row['disease_id']}, Name={row['disease_name']}): {e}")
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
