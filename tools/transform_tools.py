import tools.data_tools as dt
import pandas as pd

def filter_data(column, operator, value):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    if column not in df.columns:
        return f"❌ Column '{column}' not found. Available: {list(df.columns)}", None
    try:
        if pd.api.types.is_numeric_dtype(df[column]):
            value = float(value)
        if operator in [">","gt"]:
            result = df[df[column] > value]
        elif operator in ["<","lt"]:
            result = df[df[column] < value]
        elif operator in ["==","=","eq"]:
            result = df[df[column] == value]
        elif operator in [">=","gte"]:
            result = df[df[column] >= value]
        elif operator in ["<=","lte"]:
            result = df[df[column] <= value]
        elif operator in ["!=","ne"]:
            result = df[df[column] != value]
        else:
            return f"❌ Unknown operator: {operator}", None
        dt.store["df"] = result
        return f"✅ Filtered: {column} {operator} {value} → {len(result)} rows", result
    except Exception as e:
        return f"❌ Filter error: {str(e)}", None

def sort_data(column, order="asc"):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    if column not in df.columns:
        return f"❌ Column '{column}' not found. Available: {list(df.columns)}", None
    ascending = order.lower() in ["asc","ascending"]
    result = df.sort_values(by=column, ascending=ascending)
    dt.store["df"] = result
    return f"✅ Sorted by '{column}' ({order})", result

def compare_columns(col1, col2):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    if col1 not in df.columns or col2 not in df.columns:
        return f"❌ Columns not found. Available: {list(df.columns)}", None
    result = df[[col1, col2]].describe()
    return f"✅ Comparison: '{col1}' vs '{col2}'", result

def rename_column(old_name, new_name):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    if old_name not in df.columns:
        return f"❌ Column '{old_name}' not found. Available: {list(df.columns)}", None
    df = df.rename(columns={old_name: new_name})
    dt.store["df"] = df
    return f"✅ Renamed '{old_name}' → '{new_name}'", df

def drop_column(column):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    if column not in df.columns:
        return f"❌ Column '{column}' not found. Available: {list(df.columns)}", None
    df = df.drop(columns=[column])
    dt.store["df"] = df
    return f"✅ Dropped column '{column}'", df

def add_column(name, expression):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    try:
        df[name] = df.eval(expression)
        dt.store["df"] = df
        return f"✅ Added column '{name}' = {expression}", df
    except Exception as e:
        return f"❌ Error adding column: {str(e)}", None

def convert_type(column, dtype):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    try:
        df[column] = df[column].astype(dtype)
        dt.store["df"] = df
        return f"✅ Converted '{column}' to {dtype}", df
    except Exception as e:
        return f"❌ Conversion error: {str(e)}", None
