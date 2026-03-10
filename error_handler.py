import os
from groq import Groq

def fix_error_with_gpt(error_message, context=""):
    try:
        env = open(os.path.expanduser("~/de-ai-agent/.env")).read()
        api_key = [line.split("=",1)[1] for line in env.strip().split("\n") if line.startswith("GROQ_API_KEY")][0]
        client = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a data engineering expert. When given an error from HDFS, Iceberg, PySpark, Airflow or MySQL, give a short clear fix in 2-3 lines only."},
                {"role": "user", "content": f"Error:\n{error_message}\n\nContext:\n{context}\n\nHow to fix this?"}
            ],
            max_tokens=300
        )
        return "🤖 AI Fix:\n\n" + response.choices[0].message.content
    except Exception as e:
        return f"❌ AI could not help: {str(e)}"
