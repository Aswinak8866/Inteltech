import streamlit as st
import agent
import tools.data_tools as dt
import tools.clean_tools as ct
import tools.transform_tools as tt
import tools.pipeline_tools as pt
import os

st.set_page_config(page_title="DE AI Agent", page_icon="🤖", layout="wide")

if dt.store["df"] is None and "loaded_filename" in st.session_state:
    _, restored = dt.load_saved_dataset(st.session_state["loaded_filename"])
    if restored is not None:
        dt.store["df"] = restored
        dt.store["filename"] = st.session_state["loaded_filename"]
        st.session_state.data_info = f"Rows:{restored.shape[0]}, Cols:{restored.shape[1]}, Columns:{list(restored.columns)}"

if "messages" not in st.session_state:
    st.session_state.messages = [{"role":"assistant","content":"👋 Hello! I am your **Data Engineering AI Agent**!\n\nSay **'push to hdfs'** for normal Parquet\nSay **'push to hdfs iceberg'** for Iceberg format\nSay **'push to sql'** to push to MySQL\nSay **'run on airflow'** to start the pipeline! 🚀"}]

if "data_info" not in st.session_state:
    st.session_state.data_info = "No data loaded."
if "pipeline_state" not in st.session_state:
    st.session_state.pipeline_state = None
if "pipeline_data" not in st.session_state:
    st.session_state.pipeline_data = {}

with st.sidebar:
    st.title("⚙️ DE AI Agent")
    st.divider()
    st.subheader("📂 Saved Datasets")
    saved = dt.list_saved_datasets()
    if saved:
        selected = st.selectbox("Pick:", ["-- Select --"] + saved)
        if selected != "-- Select --":
            if st.button("📂 Load Dataset"):
                msg, df = dt.load_saved_dataset(selected)
                if df is not None:
                    dt.store["df"] = df
                    dt.store["filename"] = selected
                    st.session_state.data_info = f"Rows:{df.shape[0]}, Cols:{df.shape[1]}, Columns:{list(df.columns)}"
                    st.session_state["loaded_filename"] = selected
                    st.success(f"✅ {selected}")
                    st.rerun()
    else:
        st.caption("No saved datasets yet.")
    st.divider()
    st.subheader("📁 Upload Dataset")
    uploaded = st.file_uploader("CSV / Excel / JSON", type=["csv","xlsx","xls","json"])
    if uploaded:
        msg, df = dt.load_data(uploaded)
        if df is not None:
            st.session_state.data_info = f"Rows:{df.shape[0]}, Cols:{df.shape[1]}, Columns:{list(df.columns)}, Nulls:{df.isnull().sum().sum()}, Duplicates:{df.duplicated().sum()}"
            st.session_state["loaded_filename"] = uploaded.name.rsplit(".",1)[0] + ".csv"
            st.success("✅ Loaded & Saved!")
    if dt.store["df"] is not None:
        df = dt.store["df"]
        st.divider()
        st.subheader("📊 Dataset Info")
        st.metric("Rows", df.shape[0])
        st.metric("Columns", df.shape[1])
        st.metric("Nulls", int(df.isnull().sum().sum()))
        st.metric("Duplicates", int(df.duplicated().sum()))
        st.divider()
        st.subheader("💾 Export")
        c1,c2,c3 = st.columns(3)
        with c1:
            if st.button("CSV"):
                dt.export_data("csv"); st.success("✅")
        with c2:
            if st.button("Excel"):
                dt.export_data("xlsx"); st.success("✅")
        with c3:
            if st.button("JSON"):
                dt.export_data("json"); st.success("✅")
    st.divider()
    st.subheader("💡 Try saying:")
    st.markdown("- *push to hdfs* → Parquet\n- *push to hdfs iceberg* → Iceberg\n- *push to sql* → MySQL\n- *run on airflow*\n- *load mysql table*\n- *clean null values*\n- *filter Age > 30*\n- *sort by Salary desc*\n- *detect schema*")

st.title("🤖 Data Engineering AI Agent")
st.caption("HDFS • Iceberg • Airflow • MySQL")
st.divider()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "dataframe" in msg:
            st.dataframe(msg["dataframe"], use_container_width=True)
        if "filepath" in msg and msg["filepath"] and os.path.exists(msg["filepath"]):
            with open(msg["filepath"]) as f:
                st.download_button(f"⬇️ Download {os.path.basename(msg['filepath'])}", f.read(), file_name=os.path.basename(msg["filepath"]))

if prompt := st.chat_input("Ask me anything..."):
    if dt.store["df"] is None and "loaded_filename" in st.session_state:
        _, restored = dt.load_saved_dataset(st.session_state["loaded_filename"])
        if restored is not None:
            dt.store["df"] = restored
            dt.store["filename"] = st.session_state["loaded_filename"]

    st.session_state.messages.append({"role":"user","content":prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        msg_text = ""
        result_df = None
        result_file = None
        state = st.session_state.pipeline_state
        lower_prompt = prompt.lower()

        if state == "WAIT_HDFS_NORMAL_FOLDER":
            folder = prompt.strip().replace(" ","_").lower()
            st.session_state.pipeline_state = None
            hdfs_path = f"hdfs://localhost:9000/data/{folder}"
            st.markdown(f"✅ Folder: `{folder}`\n\n⚡ Pushing to HDFS as Parquet...")
            with st.spinner("📤 Storing to HDFS as Parquet..."):
                msg_text, result_file = pt.store_to_hdfs_parquet(hdfs_path, folder)
            st.session_state.pipeline_data = {}

        elif state == "WAIT_TABLE_NAME":
            table_name = prompt.strip().replace(".py","").replace(" ","_").replace("-","_").replace(".","_").lower()
            st.session_state.pipeline_data["table_name"] = table_name
            st.session_state.pipeline_state = "WAIT_DAG_NAME"
            msg_text = f"✅ Table name: `{prompt.strip()}`\n\n✈️ **What should the Airflow DAG be named?**"

        elif state == "WAIT_DAG_NAME":
            dag_name = prompt.strip().replace(".py","").replace(" ","_").replace("-","_").replace(".","_")
            st.session_state.pipeline_data["dag_name"] = dag_name
            st.session_state.pipeline_state = None
            st.markdown(f"✅ DAG name: `{prompt.strip()}`\n\n⚡ Running pipeline...")
            with st.spinner("🚀 Running full pipeline..."):
                msg_text, result_file = pt.run_full_pipeline(
                    st.session_state.pipeline_data["hdfs_path"],
                    st.session_state.pipeline_data["table_name"],
                    st.session_state.pipeline_data["dag_name"]
                )
            st.session_state.pipeline_data = {}

        elif state == "WAIT_TABLE_NAME_ONLY":
            table_name = prompt.strip().replace(".py","").replace(" ","_").replace("-","_").replace(".","_").lower()
            st.session_state.pipeline_data["table_name"] = table_name
            st.session_state.pipeline_state = None
            st.markdown(f"✅ Table: `{prompt.strip()}`\n\n⚡ Storing to HDFS as Iceberg...")
            with st.spinner("📦 Storing to HDFS in Iceberg format..."):
                msg_text, result_file = pt.store_to_hdfs_iceberg(
                    st.session_state.pipeline_data["hdfs_path"],
                    st.session_state.pipeline_data["table_name"]
                )
            st.session_state.pipeline_data = {}

        elif state == "WAIT_MYSQL_DB":
            st.session_state.pipeline_data["mysql_db"] = prompt.strip()
            st.session_state.pipeline_state = "WAIT_MYSQL_TABLE"
            msg_text = f"✅ Database: `{prompt.strip()}`\n\n📋 **What is the MySQL table name?**"

        elif state == "WAIT_MYSQL_TABLE":
            st.session_state.pipeline_data["mysql_table"] = prompt.strip()
            st.session_state.pipeline_state = None
            with st.spinner("🔄 Loading from MySQL..."):
                db = st.session_state.pipeline_data["mysql_db"]
                table = prompt.strip()
                msg_text, result_df = dt.load_from_mysql("localhost","root","",db,table)
                if result_df is not None:
                    dt.store["df"] = result_df
                    dt.store["filename"] = f"{table}.csv"
                    st.session_state.data_info = f"Rows:{result_df.shape[0]}, Cols:{result_df.shape[1]}, Columns:{list(result_df.columns)}"
                    st.session_state["loaded_filename"] = f"{table}.csv"
                    msg_text += "\n\n💡 Now say **'push to hdfs'** to push to HDFS!"
            st.session_state.pipeline_data = {}

        elif state == "WAIT_MYSQL_PUSH_TABLE":
            table_name = prompt.strip().replace(" ","_").lower()
            st.session_state.pipeline_state = None
            with st.spinner(f"📤 Pushing to MySQL table `{table_name}`..."):
                msg_text, result_df = dt.push_to_mysql("localhost","root","","de_agent_db",table_name)
            st.session_state.pipeline_data = {}

        else:
            raw = agent.get_response(prompt, st.session_state.data_info)

            if "ACTION:RUN_AIRFLOW" in raw:
                if dt.store["df"] is None:
                    msg_text = "❌ No dataset loaded. Please upload a dataset first!"
                else:
                    st.session_state.pipeline_state = "WAIT_TABLE_NAME"
                    st.session_state.pipeline_data["hdfs_path"] = "hdfs://localhost:9000/data"
                    msg_text = "🚀 **Starting Airflow Pipeline!**\n\n✅ HDFS path: `hdfs://localhost:9000/data` (permanent)\n\n📋 **What is the Iceberg table name?**"

            elif "ACTION:STORE_HDFS" in raw:
                if dt.store["df"] is None:
                    msg_text = "❌ No dataset loaded. Please upload a dataset first!"
                else:
                    if "iceberg" in lower_prompt:
                        st.session_state.pipeline_state = "WAIT_TABLE_NAME_ONLY"
                        st.session_state.pipeline_data["hdfs_path"] = "hdfs://localhost:9000/data"
                        msg_text = "📦 **Store to HDFS as Iceberg**\n\n✅ HDFS path: `hdfs://localhost:9000/data` (permanent)\n\n📋 **What is the Iceberg table name?**"
                    else:
                        st.session_state.pipeline_state = "WAIT_HDFS_NORMAL_FOLDER"
                        msg_text = "📤 **Store to HDFS as Parquet**\n\n✅ HDFS path: `hdfs://localhost:9000/data/` (permanent)\n\n📁 **What folder name should I store it in?**\n_(e.g. gold_prices)_"

            elif "ACTION:PUSH_MYSQL" in raw:
                if dt.store["df"] is None:
                    msg_text = "❌ No dataset loaded. Please upload a dataset first!"
                else:
                    st.session_state.pipeline_state = "WAIT_MYSQL_PUSH_TABLE"
                    msg_text = "🗄️ **Push to MySQL**\n\n📋 **What should the table name be?**\n_(e.g. gold_prices)_"

            elif "ACTION:LOAD_MYSQL" in raw:
                st.session_state.pipeline_state = "WAIT_MYSQL_DB"
                msg_text = "🗄️ **MySQL Load**\n\n📋 **What is the database name?**"

            elif "ACTION:REMOVE_NULLS" in raw:
                strategy = "fill" if "fill" in raw else "drop"
                msg_text, result_df = ct.remove_nulls(strategy)

            elif "ACTION:REMOVE_DUPLICATES" in raw:
                msg_text, result_df = ct.remove_duplicates()

            elif "ACTION:FIND_NULLS" in raw:
                msg_text, result_df = ct.find_nulls()

            elif "ACTION:FILTER" in raw:
                try:
                    parts = raw.split("ACTION:FILTER:")[1].split(":")
                    msg_text, result_df = tt.filter_data(parts[0].strip(),parts[1].strip(),parts[2].strip())
                except:
                    msg_text = "❌ Try: 'filter Price > 1000'"

            elif "ACTION:SORT" in raw:
                try:
                    parts = raw.split("ACTION:SORT:")[1].split(":")
                    msg_text, result_df = tt.sort_data(parts[0].strip(), parts[1].strip() if len(parts)>1 else "asc")
                except:
                    msg_text = "❌ Try: 'sort by Price descending'"

            elif "ACTION:COMPARE" in raw:
                try:
                    parts = raw.split("ACTION:COMPARE:")[1].split(":")
                    msg_text, result_df = tt.compare_columns(parts[0].strip(),parts[1].strip())
                except:
                    msg_text = "❌ Try: 'compare col1 and col2'"

            elif "ACTION:RENAME" in raw:
                try:
                    parts = raw.split("ACTION:RENAME:")[1].split(":")
                    msg_text, result_df = tt.rename_column(parts[0].strip(),parts[1].strip())
                except:
                    msg_text = "❌ Try: 'rename sales to revenue'"

            elif "ACTION:DROP_COLUMN" in raw:
                try:
                    col = raw.split("ACTION:DROP_COLUMN:")[1].strip()
                    msg_text, result_df = tt.drop_column(col)
                except:
                    msg_text = "❌ Try: 'drop column id'"

            elif "ACTION:GENERATE_PYSPARK" in raw:
                try:
                    table = raw.split("ACTION:GENERATE_PYSPARK:")[1].strip()
                    msg_text, result_file = pt.generate_pyspark_code(table)
                except:
                    msg_text, result_file = pt.generate_pyspark_code("my_table")

            elif "ACTION:GENERATE_DAG" in raw:
                try:
                    parts = raw.split("ACTION:GENERATE_DAG:")[1].split(":")
                    msg_text, result_file = pt.generate_dag(parts[0].strip(), parts[1].strip() if len(parts)>1 else "@daily")
                except:
                    msg_text, result_file = pt.generate_dag("my_pipeline")

            elif "ACTION:GENERATE_SQL" in raw:
                try:
                    table = raw.split("ACTION:GENERATE_SQL:")[1].strip()
                    msg_text, result_file = pt.generate_sql(table)
                except:
                    msg_text, result_file = pt.generate_sql("my_table")

            elif "ACTION:DETECT_SCHEMA" in raw:
                msg_text, result_file = pt.detect_schema()

            elif "ACTION:BUILD_PIPELINE" in raw:
                try:
                    name = raw.split("ACTION:BUILD_PIPELINE:")[1].strip()
                    msg_text, result_file = pt.build_pipeline(name)
                except:
                    msg_text = "❌ Try: 'build pipeline my_pipeline'"
            else:
                msg_text = raw

        st.markdown(msg_text)
        entry = {"role":"assistant","content":msg_text,"filepath":result_file}

        if result_df is not None:
            st.dataframe(result_df, use_container_width=True)
            entry["dataframe"] = result_df
            if dt.store["df"] is not None:
                d = dt.store["df"]
                st.session_state.data_info = f"Rows:{d.shape[0]}, Cols:{d.shape[1]}, Columns:{list(d.columns)}"

        if result_file and os.path.exists(result_file):
            with open(result_file) as f:
                st.download_button(
                    f"⬇️ Download {os.path.basename(result_file)}",
                    f.read(), file_name=os.path.basename(result_file),
                    key=f"dl_{os.path.basename(result_file)}_{len(st.session_state.messages)}"
                )

        st.session_state.messages.append(entry)
