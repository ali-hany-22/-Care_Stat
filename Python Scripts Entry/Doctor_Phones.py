import pandas as pd
import pyodbc
import os
import random
import re

# === 1) Configuration ===
server = 'ALI\\SQLEXPRESS'            # Change if needed
database = 'Care_Stat'                 # Target database
table_name = 'DoctorPhones'            # Target table
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Doctor_Phones_data.csv'
file_path = os.path.join(folder_path, file_name)

# === 2) Load CSV ===
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
    print(f"✅ Read file: {file_name} with {len(df)} rows")
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
    raise SystemExit(1)

# === 3) Basic validation of columns ===
required_cols = ['doctor_id', 'phone']
missing = [c for c in required_cols if c not in df.columns]
if missing:
    print(f"❌ Missing required columns in CSV: {missing}")
    print(f"Available columns: {list(df.columns)}")
    raise SystemExit(1)

# === 4) Normalize data types ===
df['doctor_id'] = pd.to_numeric(df['doctor_id'], errors='coerce').astype('Int64')

# Phone normalization helper: keep digits only, ensure length 11 (numeric-only) to satisfy CHECK
phone_digit_re = re.compile(r'[^0-9]')

def normalize_phone(val: str) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    s = phone_digit_re.sub('', s)  # keep digits only
    if len(s) == 11:
        return s
    if len(s) > 11:
        # take the last 11 digits (more likely to be the core phone)
        return s[-11:]
    # if len < 11 -> return None; we will generate later
    return None

# Apply normalization
df['phone'] = df['phone'].apply(normalize_phone)

# Drop rows missing doctor_id; missing phone will be generated later
df.dropna(subset=['doctor_id'], inplace=True)

# Remove exact duplicates in the CSV on the composite key where phone is already 11-digit
df_existing_key_like = df.dropna(subset=['phone']).copy()
df_existing_key_like.drop_duplicates(subset=['doctor_id', 'phone'], keep='first', inplace=True)

# Merge back rows with missing phone (to be filled later)
df_missing_phone = df[df['phone'].isna()].copy()
df = pd.concat([df_existing_key_like, df_missing_phone], ignore_index=True)
print(f"Rows after initial CSV de-duplication and normalization: {len(df)}")

# === 5) Connect to SQL Server and fetch reference data ===
try:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Connected to SQL Server")

    # Existing Doctors (FK validation)
    docs_df = pd.read_sql("SELECT doctor_id FROM Doctors", conn)
    valid_doctor_ids = set(docs_df['doctor_id'].astype(int))
    print(f"ℹ️ Valid doctors in DB: {len(valid_doctor_ids)}")

    # Existing composite keys in DoctorPhones
    existing_df = pd.read_sql(f"SELECT doctor_id, phone FROM {table_name}", conn)
    existing_keys = set((int(r[0]), str(r[1])) for r in existing_df.values)
    # Also track existing phones per doctor for faster checks
    existing_by_doc = {}
    for did, ph in existing_keys:
        existing_by_doc.setdefault(did, set()).add(ph)
    print(f"ℹ️ Existing phone records in DB: {len(existing_keys)}")

except Exception as e:
    print(f"❌ Failed to connect or fetch existing data: {e}")
    raise SystemExit(1)

# === 6) Prepare corrected batch: fix FK, generate valid unique phones ===
print("\n🔄 Preparing corrected records (fix FK, enforce 11-digit numeric phones, ensure composite uniqueness)...")

rng = random.Random(2025)

def gen_unique_phone_for_doctor(doc_id: int, used_for_doc: set[str]) -> str:
    # Generate an 11-digit phone starting with typical mobile prefix '01'
    # Ensure uniqueness for this doctor against DB and current batch
    attempts = 0
    while True:
        # '01' + 9 random digits
        candidate = '01' + ''.join(str(rng.randint(0,9)) for _ in range(9))
        if (doc_id, candidate) not in existing_keys and candidate not in used_for_doc:
            return candidate
        attempts += 1
        if attempts > 10000:
            # very unlikely
            raise RuntimeError("Unable to generate unique phone for doctor after many attempts")

records = []
# Track phones used per doctor within this batch
batch_used_by_doc: dict[int, set[str]] = {}

for _, row in df.iterrows():
    raw_doc = row['doctor_id']
    doc_id = int(raw_doc) if pd.notna(raw_doc) else None
    phone = row['phone'] if pd.notna(row['phone']) else None

    # Fix invalid/missing doctor_id by mapping to a random valid doctor
    if (doc_id is None) or (doc_id not in valid_doctor_ids):
        doc_id = rng.choice(list(valid_doctor_ids))

    # Get used set for this doctor
    used_set = batch_used_by_doc.setdefault(doc_id, set())
    existing_for_doc = existing_by_doc.get(doc_id, set())

    # If phone is missing or not 11 digits, generate a new one
    if (phone is None) or (len(str(phone)) != 11):
        phone = gen_unique_phone_for_doctor(doc_id, used_set | existing_for_doc)
    else:
        # Ensure uniqueness vs DB and this batch for the composite key
        if (doc_id, phone) in existing_keys or phone in used_set or phone in existing_for_doc:
            phone = gen_unique_phone_for_doctor(doc_id, used_set | existing_for_doc)

    # Mark as used and collect record
    used_set.add(phone)
    records.append((doc_id, phone))

print(f"Prepared {len(records)} corrected records for insertion.")

# === 7) Insert corrected batch ===
insert_sql = f"INSERT INTO {table_name} (doctor_id, phone) VALUES (?, ?)"

success, failed = 0, 0
for (did, ph) in records:
    try:
        cursor.execute(insert_sql, (did, ph))
        success += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for (Doctor ID={did}, Phone={ph}): {e}")
        failed += 1
    except Exception as e:
        print(f"❌ Unexpected error for (Doctor ID={did}, Phone={ph}): {e}")
        failed += 1

try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success} records.")
    if failed:
        print(f"⚠️ Failed to insert {failed} records.")
except Exception as e:
    print(f"❌ Commit failed: {e}")
    conn.rollback()

conn.close()
print("✅ Done.")