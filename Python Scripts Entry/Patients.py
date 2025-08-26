import pandas as pd
import pyodbc
import os

# === Configuration ===
server = 'ALI\\SQLEXPRESS'           # Server name from the image
database = 'Care_Stat'               # Database name
table_name = 'Patients'              # Table name
folder_path = r'E:\instant\Project\EXCEL Care stat'  # Folder containing files
file_name = 'Patient_data.csv'       # CSV file name
file_path = os.path.join(folder_path, file_name)

# Check if the file exists
if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    print("Files in the directory:")
    try:
        print(os.listdir(folder_path))
    except Exception as e:
        print(f"Error reading directory: {e}")
    exit()

# === Read CSV file ===
try:
    df = pd.read_csv(file_path)
    print(f"✅ Successfully read file: {file_name}")
    print(f"Number of records: {len(df)}")
    print("First 5 rows:")
    print(df.head())
except Exception as e:
    print(f"❌ Error reading file: {e}")
    exit()

# === Check required columns ===
required_columns = ['patient_id', 'first_name', 'last_name', 'gender', 'age', 'height_cm', 'weight_kg', 'country', 'city', 'visits_count']
missing_cols = [col for col in required_columns if col not in df.columns]

if missing_cols:
    print(f"❌ Missing required columns: {missing_cols}")
    print("Available columns:", list(df.columns))
    exit()

# === Data cleaning ===
# Convert numeric columns
df['patient_id'] = pd.to_numeric(df['patient_id'], errors='coerce')
df['age'] = pd.to_numeric(df['age'], errors='coerce')
df['height_cm'] = pd.to_numeric(df['height_cm'], errors='coerce')
df['weight_kg'] = pd.to_numeric(df['weight_kg'], errors='coerce')
df['visits_count'] = pd.to_numeric(df['visits_count'], errors='coerce')

# Clean gender column
df['gender'] = df['gender'].astype(str).str.lower().replace({
    'male': 'male', 'female': 'female',
    'm': 'male', 'f': 'female'
})

# Validate gender values
valid_genders = ['male', 'female']
if not df['gender'].isin(valid_genders).all():
    print("❌ Invalid values in 'gender' column (must be 'male' or 'female')")
    print("Invalid values found:", df[~df['gender'].isin(valid_genders)]['gender'].unique())
    exit()

# Replace NaN with None (compatible with SQL)
df = df.where(pd.notnull(df), None)

# === Connect to SQL Server ===
try:
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("✅ Connected to the database successfully.")
except Exception as e:
    print(f"❌ Failed to connect to the server: {e}")
    exit()

# === Insert data row by row ===
insert_query = """
INSERT INTO Patients (
    patient_id, first_name, last_name, gender, age,
    height_cm, weight_kg, country, city, visits_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

success_count = 0
for index, row in df.iterrows():
    try:
        cursor.execute(insert_query, (
            row['patient_id'],
            row['first_name'],
            row['last_name'],
            row['gender'],
            row['age'],
            row['height_cm'],
            row['weight_kg'],
            row['country'],
            row['city'],
            row['visits_count']
        ))
        success_count += 1
    except Exception as e:
        print(f"❌ Error inserting record {index + 1} (patient_id={row['patient_id']}): {str(e)}")

# === Commit changes ===
try:
    conn.commit()
    print(f"✅ Successfully inserted {success_count} out of {len(df)} records into table '{table_name}'.")
except Exception as e:
    print(f"❌ Error committing changes: {e}")
    conn.rollback()

# === Close connection ===
conn.close()
print("✅ Script execution completed.")