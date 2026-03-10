import os
from groq import Groq

def fix_script_with_ai(script_code, error_message):
    try:
        env = open(os.path.expanduser("~/de-ai-agent/.env")).read()
        api_key = [line.split("=",1)[1] for line in env.strip().split("\n") if line.startswith("GROQ_API_KEY")][0]
        client = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": """You are a PySpark and Python expert. 
When given a script with an error, return ONLY the fixed script with no explanation, 
no markdown, no backticks. Just the pure fixed Python code."""},
                {"role": "user", "content": f"""This script has an error:

ERROR:
{error_message}

SCRIPT:
{script_code}

Return ONLY the fixed script:"""}
            ],
            max_tokens=2000
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return None

def run_script_with_autofix(script_path, run_command):
    """Run a script, if it fails AI fixes it and retries automatically"""
    import subprocess
    
    # First attempt
    r = subprocess.run(run_command, capture_output=True, text=True, timeout=120)
    
    if r.returncode == 0:
        return True, "✅ Script ran successfully!", None
    
    # Script failed - send to AI for fix
    error = r.stderr[-500:] if r.stderr else r.stdout[-500:]
    
    try:
        original_script = open(script_path).read()
    except:
        return False, f"❌ Script failed: {error}", None
    
    # Get AI fix
    fixed_script = fix_script_with_ai(original_script, error)
    
    if not fixed_script:
        return False, f"❌ Script failed and AI could not fix: {error}", None
    
    # Save fixed script
    fixed_path = script_path.replace('.py', '_fixed.py')
    open(fixed_path, 'w').write(fixed_script)
    
    # Retry with fixed script
    fixed_command = [c.replace(script_path, fixed_path) for c in run_command]
    r2 = subprocess.run(fixed_command, capture_output=True, text=True, timeout=120)
    
    if r2.returncode == 0:
        # Replace original with fixed
        open(script_path, 'w').write(fixed_script)
        return True, "✅ AI fixed the script and ran successfully!", fixed_script
    else:
        return False, f"❌ AI tried to fix but still failed: {r2.stderr[-200:]}", fixed_script

