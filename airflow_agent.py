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
AIRFLOW_VERSION="2.10.5"
MODEL="google/gemma-3-12b-it:free"
KEY="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"

def ok(m):   console.print(f"[bold green]  OK  {m}[/bold green]")
def warn(m): console.print(f"[bold yellow]  WARN  {m}[/bold yellow]")
def err(m):  console.print(f"[bold red]  ERR  {m}[/bold red]")
def info(m): console.print(f"[bold cyan]  INFO  {m}[/bold cyan]")

FSYS="""You are Ubuntu/Airflow expert. JSON only: {"diagnosis":"...","fix_commands":["cmd"],"explanation":"..."}
Use --break-system-packages for pip. Use sudo for apt. Target: airflow==2.10.5, Python 3.12, PostgreSQL 15."""

def ask_ai(p):
    h={"Authorization":"Bearer "+KEY,"Content-Type":"application/json","HTTP-Referer":"https://agent.local","X-Title":"AirflowAgent"}
    b={"model":MODEL,"messages":[{"role":"system","content":FSYS},{"role":"user","content":p}],"max_tokens":600,"temperature":0.1}
    for i in range(5):
        try:
            r=requests.post("https://openrouter.ai/api/v1/chat/completions",headers=h,json=b,timeout=40)
            d=r.json()
            if "choices" in d: return d["choices"][0]["message"]["content"]
            if "rate" in str(d.get("error",{})): time.sleep(15); continue
        except Exception as e: return "ERR:"+str(e)
    return "ERR:failed"

def aifix(error,cmd):
    raw=ask_ai(f"CMD:{cmd}\nERROR:{error[:500]}\nJSON only.")
    try:
        m=re.search(r"\{.*\}",re.sub("```json|```","",raw).strip(),re.DOTALL)
        if m: return json.loads(m.group())
    except: pass
    return {"diagnosis":"?","fix_commands":[],"explanation":raw[:80]}

def run(cmd,t=600):
    try:
        r=subprocess.run(cmd,shell=True,capture_output=True,text=True,timeout=t,env=os.environ.copy())
        return r.returncode==0,(r.stdout or "")+(r.stderr or "")
    except subprocess.TimeoutExpired: return False,"TIMEOUT"
    except Exception as e: return False,str(e)

def runfix(cmd,label,n=3):
    console.print(f"[cyan]  {label}[/cyan]")
    console.print(f"[dim]  $ {cmd}[/dim]")
    for i in range(1,n+1):
        with Live(Spinner("dots",text=f"{i}/{n}..."),refresh_per_second=10):
            s,o=run(cmd)
        if s: ok("Done"); return True,o
        warn(f"Failed {i}/{n}")
        if o: console.print(f"[dim red]{o[:300]}[/dim red]")
        if i<n:
            console.print("[blue]  AI fixing...[/blue]")
            fx=aifix(o,cmd)
            console.print(f"[yellow]  {fx.get('diagnosis','?')}[/yellow]")
            for c in fx.get("fix_commands",[]): run(c)
            time.sleep(3)
    return False,o

PKGS=[
    ("python3-pip","pip3","sudo apt-get install -y python3-pip"),
    ("build-essential","gcc","sudo apt-get install -y build-essential"),
    ("curl","curl","sudo apt-get install -y curl"),
    ("libssl-dev",None,"sudo apt-get install -y libssl-dev libffi-dev"),
    ("libsasl2-dev",None,"sudo apt-get install -y libsasl2-dev libsasl2-modules"),
    ("libxmlsec1-dev",None,"sudo apt-get install -y libxmlsec1 libxmlsec1-dev pkg-config"),
    ("unixodbc-dev",None,"sudo apt-get install -y unixodbc unixodbc-dev"),
    ("freetds-dev",None,"sudo apt-get install -y freetds-bin freetds-dev"),
    ("libldap2-dev",None,"sudo apt-get install -y ldap-utils libldap2-dev"),
    ("sqlite3","sqlite3","sudo apt-get install -y sqlite3"),
    ("postgresql","psql","sudo apt-get install -y postgresql postgresql-contrib"),
    ("libpq-dev",None,"sudo apt-get install -y libpq-dev"),
]

def step1():
    console.print(Panel("[bold]STEP 1 - System Requirements + PostgreSQL[/bold]",border_style="blue"))
    run("sudo apt-get update -qq")
    t=Table(box=box.SIMPLE,show_header=True,header_style="bold cyan")
    t.add_column("Package",width=18); t.add_column("Status",width=10)
    miss=[]
    for pkg,bn,cmd in PKGS:
        import shutil as sh
        f=sh.which(bn) is not None if bn else run(f"dpkg -l {pkg} 2>/dev/null|grep -c '^ii'")[1].strip()!="0"
        t.add_row(pkg,"[green]Found[/green]" if f else "[red]Missing[/red]")
        if not f: miss.append((pkg,cmd))
    console.print(t)
    for pkg,cmd in miss: runfix(cmd,f"Install {pkg}")
    run("sudo service postgresql start"); time.sleep(2)
    ok("PostgreSQL started")

def step2(db,user,pw):
    console.print(Panel("[bold]STEP 2 - PostgreSQL Setup[/bold]",border_style="blue"))
    run(f"sudo -u postgres psql -c \"CREATE USER {user} WITH PASSWORD \'{pw}\';\" 2>/dev/null || true")
    run(f"sudo -u postgres psql -c \"CREATE DATABASE {db} OWNER {user};\" 2>/dev/null || true")
    run(f"sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {db} TO {user};\"")
    conn=f"postgresql+psycopg2://{user}:{pw}@localhost:5432/{db}"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]=conn
    os.environ["AIRFLOW__CORE__EXECUTOR"]="LocalExecutor"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"]="False"
    ok(f"DB ready: {db} / {user}")

def step3():
    console.print(Panel(f"[bold]STEP 3 - Install Airflow {AIRFLOW_VERSION}[/bold]",border_style="blue"))
    py=f"{sys.version_info.major}.{sys.version_info.minor}"
    info(f"Python {py}")
    runfix("pip3 install --upgrade pip setuptools wheel --break-system-packages","Upgrade pip")
    run("pip3 install "alembic<2.0" --break-system-packages")
    con=f"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{py}.txt"
    try:
        use=requests.head(con,timeout=10).status_code==200
    except: use=False
    if use:
        cmd=f'pip3 install "apache-airflow[postgres]=={AIRFLOW_VERSION}" --constraint "{con}" --break-system-packages'
    else:
        cmd=f'pip3 install "apache-airflow[postgres]=={AIRFLOW_VERSION}" --break-system-packages'
    s,o=runfix(cmd,f"Install Airflow {AIRFLOW_VERSION}",n=3)
    if not s: s,o=runfix(f'pip3 install "apache-airflow[postgres]=={AIRFLOW_VERSION}" --break-system-packages',"Fallback",n=2)
    runfix("pip3 install psycopg2-binary --break-system-packages","psycopg2-binary")
    return s

def step4():
    console.print(Panel("[bold]STEP 4 - Init Database[/bold]",border_style="blue"))
    s,_=runfix("airflow db migrate","DB migrate")
    if not s: runfix("airflow db init","DB init")

def step5():
    console.print(Panel("[bold]STEP 5 - Create Admin[/bold]",border_style="blue"))
    u=Prompt.ask("  Username",default="admin")
    p=Prompt.ask("  Password",password=True)
    p2=Prompt.ask("  Confirm",password=True)
    while p!=p2: warn("No match"); p=Prompt.ask("  Password",password=True); p2=Prompt.ask("  Confirm",password=True)
    fn="Admin"
    ln="User"
    em=f"{u}@airflow.local"
    runfix(f"airflow users create --username {u} --password {p} --firstname {fn} --lastname {ln} --role Admin --email {em}",f"Create {u}")
    ok(f"User {u} created"); return u

def step6(port):
    console.print(Panel(f"[bold]STEP 6 - Start Airflow port {port}[/bold]",border_style="green"))
    env=os.environ.copy()
    run(f"fuser -k {port}/tcp 2>/dev/null"); time.sleep(1)
    ld=os.path.join(HOME_AF,"logs"); os.makedirs(ld,exist_ok=True)
    subprocess.Popen(f"airflow webserver --port {port} > {ld}/webserver.log 2>&1",shell=True,env=env)
    time.sleep(5)
    subprocess.Popen(f"airflow scheduler > {ld}/scheduler.log 2>&1",shell=True,env=env)
    ok("Started!")

def hlth(port):
    try:
        import urllib.request; urllib.request.urlopen(f"http://localhost:{port}/health",timeout=6); return True
    except: return False

def main():
    console.clear()
    console.print(Panel(
        f"[bold cyan]AIRFLOW AI AGENT[/bold cyan]\n\n"
        f"  Airflow  : [green]{AIRFLOW_VERSION}[/green]\n"
        f"  Database : [green]PostgreSQL 15[/green]\n"
        f"  Python   : [green]3.12[/green]\n"
        f"  Ubuntu   : [green]24.04[/green]",
        border_style="cyan",padding=(1,4)))
    console.print()
    db="airflow"
    user="airflow"
    pw="airflow123"
    port=8080
    s,o=run("airflow version 2>/dev/null",t=15)
    if s:
        ver=re.search(r"[\d]+\.[\d]+\.[\d]+",o)
        ok(f"Airflow {ver.group() if ver else '?'} found")
        do=Confirm.ask("  Reinstall?",default=False)
    else:
        warn("Not installed"); do=True
    step1(); console.print()
    step2(db,user,pw); console.print()
    if do:
        if not step3(): err("Failed!"); sys.exit(1)
    console.print()
    step4(); console.print()
    u=step5(); console.print()
    step6(port); console.print()
    time.sleep(6)
    alive=hlth(port)
    console.print(Panel(
        f"[bold green]AIRFLOW RUNNING![/bold green]\n\n"
        f"  URL  : [cyan]http://localhost:{port}[/cyan]\n"
        f"  User : [cyan]{u}[/cyan]\n"
        f"  DB   : [cyan]PostgreSQL → {db}[/cyan]",
        border_style="green",padding=(1,4)))
    console.print("\n[cyan]Monitoring every 30s - Ctrl+C to stop[/cyan]\n")
    try:
        c=0
        while True:
            time.sleep(30); c+=1; ts=time.strftime("%H:%M:%S")
            if hlth(port): console.print(f"[dim]  [{ts}] Healthy #{c}[/dim]")
            else: warn("Down - restarting"); step6(port)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped. http://localhost:{port}[/yellow]\n")

main()
