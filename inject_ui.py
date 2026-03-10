with open('/home/vboxuser/de-ai-agent/app.py', 'r') as f:
    content = f.read()

css = '''
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background-color: #1a1a1a !important; color: #e8e3dc !important; font-family: 'Sora', sans-serif !important; }
#MainMenu, footer, header, [data-testid="stToolbar"] { display: none !important; }
[data-testid="stSidebar"] { background-color: #111111 !important; border-right: 1px solid #2a2a2a !important; }
[data-testid="stSidebar"] * { color: #c9c3bb !important; }
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3 { color: #e8e3dc !important; font-size: 0.85rem !important; font-weight: 600 !important; text-transform: uppercase !important; letter-spacing: 0.08em !important; }
[data-testid="stSidebar"] .stButton > button { background-color: #2a2a2a !important; color: #e8e3dc !important; border: 1px solid #383838 !important; border-radius: 8px !important; width: 100% !important; }
[data-testid="stSidebar"] .stButton > button:hover { border-color: #d4975a !important; color: #d4975a !important; }
[data-testid="stSidebar"] [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace !important; color: #d4975a !important; }
[data-testid="stMainBlockContainer"] { max-width: 780px !important; margin: 0 auto !important; padding: 2rem 1.5rem 6rem !important; }
[data-testid="stMainBlockContainer"] h1 { font-size: 1.6rem !important; font-weight: 600 !important; color: #e8e3dc !important; letter-spacing: -0.02em !important; }
hr { border-color: #2a2a2a !important; }
[data-testid="stChatInput"] textarea { background-color: #222 !important; border: 1px solid #333 !important; border-radius: 12px !important; color: #e8e3dc !important; }
[data-testid="stChatInput"] textarea:focus { border-color: #d4975a !important; }
[data-testid="stChatInput"] textarea::placeholder { color: #555 !important; }
[data-testid="stChatInput"] button { background-color: #d4975a !important; border-radius: 8px !important; border: none !important; color: #111 !important; }
[data-testid="chatAvatarIcon-user"] { background-color: #d4975a !important; color: #111 !important; }
[data-testid="chatAvatarIcon-assistant"] { background-color: #2a2a2a !important; border: 1px solid #383838 !important; }
[data-testid="stDownloadButton"] > button { background-color: #222 !important; color: #d4975a !important; border: 1px solid #d4975a !important; border-radius: 8px !important; }
[data-testid="stDownloadButton"] > button:hover { background-color: #d4975a !important; color: #111 !important; }
code { background: #222 !important; color: #d4975a !important; border-radius: 4px !important; font-family: 'JetBrains Mono', monospace !important; }
::-webkit-scrollbar { width: 5px; } ::-webkit-scrollbar-track { background: #1a1a1a; } ::-webkit-scrollbar-thumb { background: #333; border-radius: 10px; }
</style>
""", unsafe_allow_html=True)
'''

content = content.replace(
    'st.set_page_config(page_title="DE AI Agent", page_icon="🤖", layout="wide")',
    'st.set_page_config(page_title="DE AI Agent", page_icon="⚙️", layout="wide")\n' + css
)
content = content.replace('st.title("🤖 Data Engineering AI Agent")', 'st.title("⚙️ Data Engineering AI Agent")')

with open('/home/vboxuser/de-ai-agent/app.py', 'w') as f:
    f.write(content)

print("Done!")
