import pandas as pd
import os

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10,2)"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "NVARCHAR(255)"  # Default for strings or mixed types

def generate_create_table(csv_path, table_name):
    df = pd.read_csv(csv_path)
    sql = f"CREATE TABLE dbo.{table_name} (\n"
    for col in df.columns:
        sql_type = infer_sql_type(df[col].dtype)
        sql += f"    [{col}] {sql_type},\n"
    sql = sql.rstrip(",\n") + "\n);"
    return sql

# Grab the csv data to be used in the SQL table
csv_file = "C:/Users/fboy/source/repos/HBPizza/data_ingestion/csv_data/bronze/inventory.csv" #TODO: Create dynamic folder option.
table_name = "Inventory"
create_table_sql = generate_create_table(csv_file, table_name)

# Output folder
output_folder = "C:/Users/fboy/source/repos/HBPizza/scaffolding/sql_gens" #TODO: Create dynamic folder option.
os.makedirs(output_folder, exist_ok=True)

# Save to file
output_path = os.path.join(output_folder, f"create_{table_name.lower()}.sql")
with open(output_path, "w") as file:
    file.write(create_table_sql)

print(f"SQL script saved to: {output_path}")
