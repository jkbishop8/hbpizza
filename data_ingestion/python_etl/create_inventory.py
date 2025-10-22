import pandas as pd

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

# Example usage
csv_file = "C:/Users/fboy/source/repos/HBPizza/data_ingestion/csv_data/bronze/inventory.csv"
table_name = "Inventory"
create_table_sql = generate_create_table(csv_file, table_name)

# Print or save to file
print(create_table_sql)
