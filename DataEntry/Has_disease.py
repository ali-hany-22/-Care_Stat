import pandas as pd
import pyodbc
import os
import random

# === 1. Connection & file settings ===
SERVER   = r'ALI\SQLEXPRESS'
DATABASE = 'Care_Stat'
TABLE    = 'PatientChronicDiseases'

CSV_FOLDER = r'E:\instant\Project\EXCEL Care stat'
CSV_NAME   = 'Has_disease_data.csv'
CSV_PATH   = os.path.join(CSV_FOLDER, CSV_NAME)

# === 2. Read CSV ===
if not os.path.exists(CSV_PATH):
    print(f'❌ File not found: {CSV_PATH}')
    exit()

try:
    df = pd.read_csv(CSV_PATH)
    print(f'✅ Read {len(df)} rows from {CSV_NAME}')
except Exception as e:
    print(f'❌ Error reading CSV: {e}')
    exit()

# === 3. Clean & cast columns ===
required_cols = {'patient_id', 'disease_id', 'has_disease'}
missing = required_cols - set(df.columns)
if missing:
    print(f'❌ Missing columns: {missing}')
    exit()

df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce').astype('Int64')
df['disease_id'] = pd.to_numeric(df['disease_id'], errors='coerce').astype('Int64')

# Convert 'YES/NO' -> 1/0
df['has_disease'] = df['has_disease'].astype(str).str.strip().str.upper()
df['has_disease'] = df['has_disease'].map({'YES': 1, 'NO': 0}).fillna(0).astype(int)

# Drop rows with NULL keys
df.dropna(subset=['patient_id', 'disease_id'], inplace=True)
df.drop_duplicates(subset=['patient_id', 'disease_id'], keep='first', inplace=True)
print(f'Rows after cleaning: {len(df)}')

# === 4. Connect to DB & fetch valid IDs ===
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(conn_str, autocommit=False)
    cursor = conn.cursor()
    print('✅ Connected to database')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit()

def fetch_ids(query):
    return set(pd.read_sql(query, conn).iloc[:, 0].astype(int))

valid_patients = fetch_ids('SELECT patient_id FROM Patients')
valid_diseases = fetch_ids('SELECT disease_id FROM ChronicDiseases')
existing_pairs = {(p, d) for p, d in conn.execute(f'SELECT patient_id, disease_id FROM {TABLE}')}

print(f'Valid patients: {len(valid_patients)} | Valid diseases: {len(valid_diseases)} | Existing pairs: {len(existing_pairs)}')

# === 5. Filter & force-fix invalid FKs ===
records_to_insert = []

for _, row in df.iterrows():
    pid, did, has = int(row['patient_id']), int(row['disease_id']), int(row['has_disease'])

    if (pid, did) in existing_pairs:          # skip duplicates
        continue

    if pid not in valid_patients:             # fix invalid patient
        pid = random.choice(list(valid_patients))

    if did not in valid_diseases:             # fix invalid disease
        did = random.choice(list(valid_diseases))

    records_to_insert.append((pid, did, has))

if not records_to_insert:
    print('\n✅ No new records to insert.')
    conn.close(); exit()

print(f'Records ready for insert: {len(records_to_insert)}')

# === 6. Insert into table ===
insert_sql = f'INSERT INTO {TABLE} (patient_id, disease_id, has_disease) VALUES (?, ?, ?)'

success = errors = 0
for rec in records_to_insert:
    try:
        cursor.execute(insert_sql, rec)
        success += 1
    except pyodbc.IntegrityError as ie:
        print(f'⚠️ Integrity error on {rec}: {ie}')
        errors += 1
    except Exception as ex:
        print(f'❌ Unexpected error on {rec}: {ex}')
        errors += 1

# === 7. Commit & close ===
try:
    conn.commit()
    print(f'\n✅ Inserted {success} rows successfully.')
    if errors:
        print(f'⚠️ Failed to insert {errors} rows.')
except Exception as e:
    conn.rollback()
    print(f'❌ Commit failed: {e}')
finally:
    conn.close()
    print('✅ Connection closed.')