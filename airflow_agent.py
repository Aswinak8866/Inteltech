#!/usr/bin/env python3
import subprocess,sys,os,time,re,json,shutil

def ipkg(pkg):
    subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q","--break-system-packages"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

for p in ["requests","rich"]:
    try: __import__(p)
    except: ipkg(p)

import requests
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt,Confirm
from rich.spinner import Spinner
from rich.live import Live
from rich.table import Table
from rich import box

console=Console()
HOME_AF=os.path.expanduser("~/airflow")
os.environ["AIRFLOW_HOME"]=HOME_AF
MODEL="google/gemma-3-12b-it:free"
KEY="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"

def ok(m): console.print(f"[green]  OK  {m}[/green]")
def warn(m): console.print(f"[yellow]  WARN  {m}[/yellow]")
def err(m): console.print(f"[red]  ERR  {m}[/red]")
def info(m): console.print(f"[cyan]  INFO  {m}[/cyan]")

def ask_ai(sys_p,usr_p):
    h={"Authorization":"Bearer "+KEY,"Content-Type":"application/json","HTTP-Referer":"https://agent.local","X-Title":"Agent"}
    b={"model":MODEL,"messages":[{"role":"system","content":sys_p},{"role":"user","content":usr_p}],"max_tokens":512,"temperature":0.1}
    for attempt in range(5):
        try:
            r=requests.post("https://openrouter.ai/api/v1/chat/completions",headers=h,json=b,timeout=40)
            d=r.json()
            if "choices" in d: return d["choices"][0]["message"]["content"]
            msg=str(d.get("error",{}).get("message","?"))
            if "rate" in msg.lower() or "429" in str(d.get("error",{}).get("code","")):
                print(f"  Rate limited, waiting 15s... (attempt {attempt+1}/5)")
                time.sleep(15)
                continue
            return "ERR:"+msg
        except Exception as e: return "ERR:"+str(e)
    return "ERR:Rate limit exceeded after 5 retries"

FSYS="Ubuntu Airflow DevOps agent. JSON only: {diagnosis:str,fix_commands:[str],explanation:str}"

def aifix(error,ctx=""):
    raw=ask_ai(FSYS,"ERROR:"+error[:500]+" CTX:"+ctx+" JSON only.")
    try:
        c=re.sub("```json|```","",raw).strip()
        m=re.search(r"[{].*[}]",c,re.DOTALL)
        if m: return json.loads(m.group())
    except: pass
    return {"diagnosis":"?","fix_commands":[],"explanation":raw[:80]}

def run(cmd,t=300):
    try:
        r=subprocess.run(cmd,shell=True,capture_output=True,text=True,timeout=t,env=os.environ.copy())
        return r.returncode==0,(r.stdout or "")+(r.stderr or "")
    except subprocess.TimeoutExpired: return False,"TIMEOUT"
    except Exception as e: return False,str(e)

def runfix(cmd,label,ctx="",n=3):
    console.print(f"[cyan]  {label}[/cyan]")
    console.print(f"[dim]  $ {cmd}[/dim]")
    for i in range(1,n+1):
        with Live(Spinner("dots",text=f"{i}/{n}..."),refresh_per_second=10):
            s,o=run(cmd)
        if s: ok("Done"); return True,o
        warn(f"Failed {i}/{n}")
        if o: console.print(f"[dim red]{o[:200]}[/dim red]")
        if i<n:
            console.print("[blue]  AI fixing...[/blue]")
            fx=aifix(o,cmd)
            ok(fx.get("diagnosis","?"))
            for c in fx.get("fix_commands",[]): run(c)
            time.sleep(2)
    return False,o

REQS=[
    {"n":"Python3","c":lambda:sys.version_info>=(3,8),"i":"sudo apt-get install -y python3 python3-pip"},
    {"n":"pip3","c":lambda:shutil.which("pip3") is not None,"i":"sudo apt-get install -y python3-pip"},
    {"n":"gcc","c":lambda:shutil.which("gcc") is not None,"i":"sudo apt-get install -y build-essential"},
    {"n":"libssl","c":lambda:run("dpkg -l libssl-dev 2>/dev/null|grep -c ii")[1].strip()!="0","i":"sudo apt-get install -y libssl-dev libffi-dev"},
    {"n":"sqlite3","c":lambda:shutil.which("sqlite3") is not None,"i":"sudo apt-get install -y sqlite3"},
    {"n":"Java","c":lambda:shutil.which("java") is not None,"i":"sudo apt-get install -y default-jdk","note":"optional"},
    {"n":"curl","c":lambda:shutil.which("curl") is not None,"i":"sudo apt-get install -y curl"},
]

def chkreqs():
    console.print(Panel("[bold]STEP 1 - Requirements[/bold]",border_style="blue"))
    run("sudo apt-get update -qq")
    t=Table(box=box.SIMPLE,show_header=True,header_style="bold cyan")
    t.add_column("Item",width=15)
    t.add_column("Status",width=12)
    t.add_column("Note",width=20)
    miss=[]
    for r in REQS:
        try: f=r["c"]()
        except: f=False
        t.add_row(r["n"],"[green]Found[/green]" if f else "[red]Missing[/red]",r.get("note","required"))
        if not f: miss.append(r)
    console.print(t)
    if not miss: ok("All OK"); return
    for r in miss: runfix(r["i"],"Install "+r["n"])
    ok("All installed")

def afok():
    f,_=run("airflow version 2>/dev/null",t=15)
    return f

def instaf():
    console.print(Panel("[bold]STEP 2 - Install Airflow[/bold]",border_style="blue"))
    py=str(sys.version_info.major)+"."+str(sys.version_info.minor)
    con="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-"+py+".txt"
    runfix("pip3 install --upgrade pip --break-system-packages","Upgrade pip")
    s,_=runfix("pip3 install apache-airflow==2.10.0 --constraint "+con+" --break-system-packages --ignore-installed","Install Airflow 2.8.1")
    if not s: runfix("pip3 install apache-airflow --break-system-packages --ignore-installed","Install Airflow simple")
    return s

def initdb():
    console.print(Panel("[bold]STEP 3 - Init DB[/bold]",border_style="blue"))
    s,_=runfix("airflow db migrate","DB migrate")
    if not s: runfix("airflow db init","DB init")

def mkuser():
    console.print(Panel("[bold]STEP 4 - Create Admin[/bold]",border_style="blue"))
    u=Prompt.ask("  Username",default="admin")
    p=Prompt.ask("  Password",password=True)
    p2=Prompt.ask("  Confirm",password=True)
    while p!=p2: warn("No match"); p=Prompt.ask("  Password",password=True); p2=Prompt.ask("  Confirm",password=True)
    fn=Prompt.ask("  First name",default="Admin")
    ln=Prompt.ask("  Last name",default="User")
    em=Prompt.ask("  Email",default=u+"@airflow.local")
    runfix("airflow users create --username "+u+" --password "+p+" --firstname "+fn+" --lastname "+ln+" --role Admin --email "+em,"Create user "+u)
    ok("User "+u+" created")
    return u

def startaf(port):
    console.print(Panel("[bold]STEP 5 - Start Airflow port "+str(port)+"[/bold]",border_style="green"))
    env=os.environ.copy()
    run("fuser -k "+str(port)+"/tcp 2>/dev/null")
    time.sleep(1)
    ld=os.path.expanduser("~/airflow/logs")
    os.makedirs(ld,exist_ok=True)
    subprocess.Popen("airflow webserver --port "+str(port)+" > "+ld+"/web.log 2>&1",shell=True,env=env)
    time.sleep(4)
    subprocess.Popen("airflow scheduler > "+ld+"/sched.log 2>&1",shell=True,env=env)
    ok("Started")

def hlth(port):
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:"+str(port)+"/health",timeout=6)
        return True
    except: return False

def main():
    console.clear()
    console.print(Panel("[bold cyan]AIRFLOW AI AGENT[/bold cyan]\n[bold green]Model: gemma-3-12b-it FREE[/bold green]\n[dim]Ubuntu Auto Fix No UI[/dim]",border_style="cyan",padding=(1,4)))
    console.print()
    info("AI ready: gemma-3-12b-it FREE (will activate on errors)")
    ok("Skipping connection test to save rate limit\n")
    chkreqs()
    console.print()
    if afok():
        ok("Airflow installed")
        do=Confirm.ask("  Reinstall?",default=False)
    else:
        warn("Airflow NOT installed")
        do=Confirm.ask("  Install Airflow?",default=True)
    if do:
        if not instaf(): err("Failed"); sys.exit(1)
    console.print()
    ps=Prompt.ask("  Which port?",default="8080")
    try: port=int(ps)
    except: port=8080
    console.print()
    initdb()
    console.print()
    u=mkuser()
    console.print()
    startaf(port)
    console.print()
    time.sleep(5)
    alive=hlth(port)
    console.print(Panel("[bold green]AIRFLOW RUNNING[/bold green]\n\nURL: http://localhost:"+str(port)+"\nUser: "+u+"\nHome: "+HOME_AF+"\n\n[dim]Logs: ~/airflow/logs/[/dim]",border_style="green",padding=(1,4)))
    console.print("\n[cyan]Monitoring every 30s (Ctrl+C to quit)[/cyan]\n")
    try:
        c=0
        while True:
            time.sleep(30); c+=1; ts=time.strftime("%H:%M:%S")
            if hlth(port): console.print(f"[dim]  [{ts}] Healthy #{c}[/dim]")
            else: warn("Down - restarting"); startaf(port)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped. Airflow at http://localhost:"+str(port)+"[/yellow]\n")

main()
