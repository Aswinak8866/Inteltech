import pandas as pd
import os
import subprocess

store = {"df": None, "df2": None, "filename": ""}
SAVED_DIR = "/home/vboxuser/de-ai-agent/saved_datasets"
os.makedirs(SAVED_DIR, exist_ok=True)

def load_data(file):
    try:
        name = file.name
        if name.endswith(".csv"):
            df = pd.read_csv(file)
        elif name.endswith((".xlsx",".xls")):
            df = pd.read_excel(file)
        elif name.endswith(".json"):
            df = pd.read_json(file)
        else:
            return "❌ Unsupported format.", None
        store["df"] = df
        store["filename"] = name
        save_path = os.path.join(SAVED_DIR, name.rsplit(".",1)[0] + ".csv")
        df.to_csv(save_path, index=False)
        return f"✅ Loaded & Saved!\n📄 {name}\n📊 {df.shape[0]} rows × {df.shape[1]} cols\n⚠️ Nulls: {df.isnull().sum().sum()}\n🔁 Duplicates: {df.duplicated().sum()}", df
    except Exception as e:
        return f"❌ Error: {str(e)}", None

def list_saved_datasets():
    return [f for f in os.listdir(SAVED_DIR) if f.endswith(".csv")]

def load_saved_dataset(filename):
    path = os.path.join(SAVED_DIR, filename)
    if not os.path.exists(path):
        return f"❌ Not found: {filename}", None
    try:
        df = pd.read_csv(path)
        store["df"] = df
        store["filename"] = filename
        return f"✅ Loaded: {filename}\n📊 {df.shape[0]} rows × {df.shape[1]} cols", df
    except Exception as e:
        return f"❌ Error: {str(e)}", None

def load_from_mysql(host, user, password, database, table):
    try:
        from sqlalchemy import create_engine
        pw = f":{password}" if password else ""
        engine = create_engine(f"mysql+pymysql://{user}{pw}@{host}/{database}")
        df = pd.read_sql_table(table, engine)
        store["df"] = df
        store["filename"] = f"{table}.csv"
        save_path = os.path.join(SAVED_DIR, f"{table}.csv")
        df.to_csv(save_path, index=False)
        return f"✅ MySQL Loaded!\n🗄️ {database}.{table}\n📊 {df.shape[0]} rows × {df.shape[1]} cols", df
    except Exception as e:
        return f"❌ MySQL Error: {str(e)}", None

def push_to_mysql(host, user, password, database, table):
    try:
        from sqlalchemy import create_engine
        df = store["df"]
        if df is None:
            return "❌ No dataset loaded!", None
        pw = f":{password}" if password else ""
        engine = create_engine(f"mysql+pymysql://{user}{pw}@{host}/{database}")
        df.to_sql(table, engine, if_exists="replace", index=False)
        return f"✅ Pushed to MySQL!\n🗄️ {database}.{table}\n📊 {df.shape[0]} rows × {df.shape[1]} cols", df
    except Exception as e:
        return f"❌ MySQL Push Error: {str(e)}", None

def push_mysql_to_hdfs(host, user, password, database, table, hdfs_path):
    try:
        from sqlalchemy import create_engine
        pw = f":{password}" if password else ""
        engine = create_engine(f"mysql+pymysql://{user}{pw}@{host}/{database}")
        df = pd.read_sql_table(table, engine)
        store["df"] = df
        store["filename"] = f"{table}.csv"

        steps = []
        steps.append(f"✅ 1. Loaded {df.shape[0]} rows from MySQL `{database}.{table}`")

        local_csv = os.path.join(SAVED_DIR, f"{table}.csv")
        df.to_csv(local_csv, index=False)
        steps.append(f"✅ 2. Saved locally → {local_csv}")

        hdfs_input = f"{hdfs_path}/input/{table}.csv"
        try:
            subprocess.run(["hdfs","dfs","-mkdir","-p",f"{hdfs_path}/input"], capture_output=True)
            r = subprocess.run(["hdfs","dfs","-put","-f",local_csv,hdfs_input],
                               capture_output=True, text=True, timeout=60)
            if r.returncode == 0:
                steps.append(f"✅ 3. Uploaded to HDFS → {hdfs_input}")
            else:
                steps.append(f"⚠️ 3. HDFS upload warning: {r.stderr[:80]}")
        except Exception as e:
            steps.append(f"⚠️ 3. HDFS skipped: {str(e)[:60]}")

        steps.append(f"\n📦 HDFS path: {hdfs_input}")
        return "\n".join(steps), df
    except Exception as e:
        return f"❌ Error: {str(e)}", None

def export_data(format="csv"):
    if store["df"] is None:
        return "❌ No data loaded.", None, None
    df = store["df"]
    os.makedirs("output", exist_ok=True)
    try:
        if format == "csv":
            path = "output/exported_data.csv"
            df.to_csv(path, index=False)
        elif format in ["xlsx","excel"]:
            path = "output/exported_data.xlsx"
            df.to_excel(path, index=False)
        elif format == "json":
            path = "output/exported_data.json"
            df.to_json(path, orient="records", indent=2)
        else:
            return "❌ Use csv, excel or json.", None, None
        return f"✅ Exported to {path}", path, df
    except Exception as e:
        return f"❌ Export error: {str(e)}", None, None
