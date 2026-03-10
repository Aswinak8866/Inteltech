import tools.data_tools as dt

def remove_nulls(strategy="drop"):
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    before = len(df)
    if strategy == "drop":
        df = df.dropna()
    else:
        df = df.fillna(df.mean(numeric_only=True)).fillna("Unknown")
    dt.store["df"] = df
    return f"✅ Nulls removed ({strategy}). Rows: {before} → {len(df)}", df

def remove_duplicates():
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    before = len(df)
    df = df.drop_duplicates()
    dt.store["df"] = df
    return f"✅ Duplicates removed. Rows: {before} → {len(df)}", df

def find_nulls():
    df = dt.store["df"]
    if df is None:
        return "❌ No data loaded.", None
    null_df = df.isnull().sum().reset_index()
    null_df.columns = ["Column", "Null Count"]
    null_df = null_df[null_df["Null Count"] > 0]
    if null_df.empty:
        return "✅ No null values found!", None
    return f"⚠️ Found nulls in {len(null_df)} columns:", null_df
