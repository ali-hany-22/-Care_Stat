import pandas as pd
import pyodbc
import os

# === 1. Configuration ===
server = 'ALI\\SQLEXPRESS'
database = 'Care_Stat'
table_name = 'Doctors'
folder_path = r'E:\instant\Project\EXCEL Care stat'
file_name = 'Doctor_data.csv'  # Corrected file name
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
    print(f"Number of records: {len(df)}")
    print("First 5 rows of data:")
    print(df.head())
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit()

# === 4. Check required columns ===
required_columns = [
    'doctor_id', 'first_name', 'last_name', 'age', 'email', 'gender',
    'specialization', 'graduation_year', 'university_grade',
    'educational_degree', 'hire_year', 'years_of_experience',
    'rating_avg', 'salary'
]
missing_cols = [col for col in required_columns if col not in df.columns]

if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print("Available columns in the file:", list(df.columns))
    exit()

# === 5. Data Cleaning and Validation ===
print("\n🔄 Starting data cleaning and validation process...")

numeric_cols = ['doctor_id', 'age', 'graduation_year', 'hire_year', 'years_of_experience', 'rating_avg', 'salary']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['gender'] = df['gender'].astype(str).str.strip().str.lower()
valid_genders = ['male', 'female']
invalid_gender_rows = df[~df['gender'].isin(valid_genders)]
if not invalid_gender_rows.empty:
    print(f"❌ Invalid values found in 'gender' column: {invalid_gender_rows['gender'].unique()}")
    exit()

df = df.where(pd.notnull(df), None)
print("✅ Data cleaning process completed.")

# === 6. Connect to SQL Server ===
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
except Exception as e:
    print(f"❌ Failed to connect to the server: {e}")
    exit()

# === 7. Insert data row by row ===
insert_query = f"""
INSERT INTO {table_name} (
    doctor_id, first_name, last_name, age, email, gender, specialization,
    graduation_year, university_grade, educational_degree, hire_year,
    years_of_experience, rating_avg, salary
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

success_count = 0
error_count = 0
print("\n🔄 Starting data insertion into the table...")

for index, row in df.iterrows():
    try:
        cursor.execute(insert_query, tuple(row))
        success_count += 1
    except pyodbc.IntegrityError as e:
        print(f"⚠️ Integrity error for record {index + 1} (doctor_id={row['doctor_id']}): {e}")
        error_count += 1
    except Exception as e:
        print(f"❌ Unexpected error inserting record {index + 1} (doctor_id={row['doctor_id']}): {e}")
        error_count += 1

# === 8. Commit changes and close connection ===
try:
    conn.commit()
    print(f"\n✅ Successfully inserted {success_count} records.")
    if error_count > 0:
        print(f"⚠️ Failed to insert {error_count} records.")
except Exception as e:
    print(f"❌ Error committing changes: {e}")
    conn.rollback()

conn.close()
print("✅ Script execution completed and connection closed.")

