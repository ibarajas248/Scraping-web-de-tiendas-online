#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Borra cronjobs de SABADO (DOW incluye 6/sat) del crontab remoto o local.

Uso (si ya estás en el VPS):
  python3 borrar_jobs_sabado.py --dry-run
  python3 borrar_jobs_sabado.py

Uso por SSH (desde tu PC u otro host):
  python3 borrar_jobs_sabado.py --use-ssh --ask-pass --dry-run
  python3 borrar_jobs_sabado.py --use-ssh --ask-pass

Opciones:
  --include-star   también borra jobs con DOW="*" (todos los días, incluye sábado)
"""

import re
import sys
import argparse
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Set

# =========================
# CONFIG (EDITA SI QUIERES)
# =========================
USE_SSH_DEFAULT = False

SSH_HOST = "179.61.219.207"
SSH_PORT = 22
SSH_USER = "intelligenceblue-scrap"
SSH_PASS = ""  # <-- si usas SSH y NO quieres --ask-pass, pon el password aquí

# =========================
# CRON PARSER
# =========================
CRON_RE = re.compile(
    r"^\s*"
    r"(?P<m>\*|[\d\/,\-]+)\s+"
    r"(?P<h>\*|[\d\/,\-]+)\s+"
    r"(?P<dom>\*|[\d\/,\-]+)\s+"
    r"(?P<mon>\*|[\d\/,\-]+)\s+"
    r"(?P<dow>\*|[\w\/,\-]+)\s+"
    r"(?P<cmd>.+)$"
)

DOW_NAME_TO_NUM = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

def is_env_or_comment(line: str) -> bool:
    ls = line.strip()
    if not ls:
        return True
    if ls.startswith("#"):
        return True
    first = ls.split()[0]
    return "=" in first  # VAR=...

def is_spec_line(line: str) -> bool:
    return line.strip().startswith("@")  # @reboot, @daily, etc.

def parse_5field(line: str) -> Optional[Dict[str, str]]:
    if is_env_or_comment(line) or is_spec_line(line):
        return None
    m = CRON_RE.match(line)
    if not m:
        return None
    d = m.groupdict()
    return {"m": d["m"], "h": d["h"], "dom": d["dom"], "mon": d["mon"], "dow": d["dow"], "cmd": d["cmd"]}

def validate_crontab_lines(lines: List[str]) -> str:
    cleaned = []
    for i, ln in enumerate(lines):
        if ln is None:
            continue
        ln = ln.replace("\r", "")
        if not ln.strip():
            cleaned.append("")
            continue
        if is_env_or_comment(ln) or is_spec_line(ln) or CRON_RE.match(ln):
            cleaned.append(ln)
        else:
            raise RuntimeError(f"Línea inválida en crontab (idx {i}): {ln}")
    return "\n".join(cleaned).rstrip("\n") + "\n"

# =========================
# DOW utils
# =========================
def _normalize_dow_token(tok: str) -> Optional[int]:
    t = tok.strip().lower()
    if not t:
        return None
    if t.isdigit():
        n = int(t)
        if n == 7:
            n = 0
        return n if 0 <= n <= 6 else None
    t3 = t[:3]
    return DOW_NAME_TO_NUM.get(t3)

def dow_expr_values(expr: str) -> Optional[Set[int]]:
    e = expr.strip().lower()
    if e == "*":
        return None  # todos los días

    domain = list(range(0, 7))
    values: Set[int] = set()
    parts = [p.strip() for p in e.split(",") if p.strip()]

    for part in parts:
        step = None
        base = part
        if "/" in part:
            base, step_s = part.split("/", 1)
            base = base.strip()
            step = int(step_s.strip()) if step_s.strip().isdigit() else None

        if base == "*" or base == "":
            base_vals = domain[:]
        elif "-" in base:
            a_s, b_s = base.split("-", 1)
            a = _normalize_dow_token(a_s)
            b = _normalize_dow_token(b_s)
            if a is None or b is None:
                continue
            if a <= b:
                base_vals = list(range(a, b + 1))
            else:
                base_vals = list(range(a, 7)) + list(range(0, b + 1))
        else:
            n = _normalize_dow_token(base)
            if n is None:
                continue
            base_vals = [n]

        if step and step > 0:
            base_vals = base_vals[0::step]

        values.update(base_vals)

    return values

def includes_saturday(dow_expr: str) -> bool:
    vals = dow_expr_values(dow_expr)
    if vals is None:
        return True  # '*' incluye sábado
    return 6 in vals

# =========================
# LOCAL CRONTAB
# =========================
def get_crontab_lines_local() -> List[str]:
    import subprocess
    p = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    txt = (p.stdout or "") + (p.stderr or "")
    if "no crontab for" in txt.lower():
        return []
    return (p.stdout or "").splitlines()

def set_crontab_lines_local(lines: List[str]) -> None:
    import subprocess
    content = validate_crontab_lines(lines)
    p = subprocess.run(["crontab", "-"], input=content, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "Error escribiendo crontab").strip())

# =========================
# SSH CRONTAB
# =========================
def run_ssh_command(host: str, port: int, user: str, password: str, command: str) -> Tuple[str, str, int]:
    import paramiko
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, port=port, username=user, password=password, timeout=20)
    stdin, stdout, stderr = c.exec_command(command)
    out = stdout.read().decode(errors="ignore")
    err = stderr.read().decode(errors="ignore")
    rc = stdout.channel.recv_exit_status()
    c.close()
    return out, err, rc

def run_ssh_stdin(host: str, port: int, user: str, password: str, command: str, input_data: str) -> Tuple[str, str, int]:
    import paramiko
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, port=port, username=user, password=password, timeout=20)
    stdin, stdout, stderr = c.exec_command(command)
    stdin.write(input_data)
    stdin.flush()
    stdin.channel.shutdown_write()
    out = stdout.read().decode(errors="ignore")
    err = stderr.read().decode(errors="ignore")
    rc = stdout.channel.recv_exit_status()
    c.close()
    return out, err, rc

def get_crontab_lines_ssh(host: str, port: int, user: str, password: str) -> List[str]:
    out, err, rc = run_ssh_command(host, port, user, password, "crontab -l 2>/dev/null || true")
    txt = (out or "") + (err or "")
    if "no crontab for" in txt.lower():
        return []
    return (out or "").splitlines()

def set_crontab_lines_ssh(host: str, port: int, user: str, password: str, lines: List[str]) -> None:
    content = validate_crontab_lines(lines)
    out, err, rc = run_ssh_stdin(host, port, user, password, "env LANG=C LC_ALL=C crontab -", content)
    if rc != 0:
        raise RuntimeError((err or out or "Error actualizando crontab").strip())

# =========================
# MAIN LOGIC
# =========================
def remove_saturday_jobs(lines: List[str], include_star: bool) -> Tuple[List[str], List[str]]:
    """
    Retorna (new_lines, removed_lines)
    - Borra líneas 5-campos cuyo DOW incluye sábado.
    - Por defecto NO borra DOW="*" a menos que include_star=True.
    - También borra el comentario anterior si coincide con el patrón que usamos al duplicar.
    """
    new_lines: List[str] = []
    removed: List[str] = []

    dup_comment_re = re.compile(r"^\s*#\s*duplicado\s+viernes->sabado\b", re.IGNORECASE)

    i = 0
    while i < len(lines):
        ln = lines[i]
        p = parse_5field(ln)

        if p:
            dow = p["dow"].strip().lower()
            is_star = (dow == "*")

            if includes_saturday(dow) and (include_star or not is_star):
                # Si la línea anterior fue el comentario de duplicado, también lo borramos
                if new_lines and dup_comment_re.match(new_lines[-1] or ""):
                    removed.append(new_lines.pop())

                removed.append(ln)
                i += 1
                continue

        new_lines.append(ln)
        i += 1

    return new_lines, removed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--use-ssh", action="store_true", help="Usar SSH en vez de crontab local.")
    ap.add_argument("--ask-pass", action="store_true", help="Pedir password por consola (si usas SSH).")
    ap.add_argument("--dry-run", action="store_true", help="No aplica cambios; solo muestra lo que borraría.")
    ap.add_argument("--include-star", action="store_true", help='También borrar jobs con DOW="*" (todos los días).')
    # overrides opcionales
    ap.add_argument("--host", default=SSH_HOST)
    ap.add_argument("--port", type=int, default=SSH_PORT)
    ap.add_argument("--user", default=SSH_USER)
    ap.add_argument("--password", default=SSH_PASS)
    args = ap.parse_args()

    use_ssh = args.use_ssh or USE_SSH_DEFAULT

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"crontab_backup_{ts}.txt"

    if use_ssh:
        if args.ask_pass:
            from getpass import getpass
            pwd = getpass("SSH Password: ")
        else:
            pwd = args.password

        if not pwd:
            print("Error: falta password para SSH. Pon SSH_PASS en el script o usa --ask-pass.", file=sys.stderr)
            sys.exit(2)

        lines = get_crontab_lines_ssh(args.host, args.port, args.user, pwd)
        with open(backup_name, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))
        print(f"Backup guardado en: {backup_name}")

        new_lines, removed = remove_saturday_jobs(lines, include_star=args.include_star)

        print(f"Jobs/líneas borradas: {len(removed)}")
        if removed:
            print("\n=== Líneas que se borrarían ===")
            for ln in removed:
                print(ln)

        if args.dry_run or not removed:
            print("\n(dry-run) No se aplicaron cambios." if args.dry_run else "\nNo había nada que borrar.")
            return

        set_crontab_lines_ssh(args.host, args.port, args.user, pwd, new_lines)
        print("\n✅ Crontab actualizado (SSH).")

    else:
        lines = get_crontab_lines_local()
        with open(backup_name, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))
        print(f"Backup guardado en: {backup_name}")

        new_lines, removed = remove_saturday_jobs(lines, include_star=args.include_star)

        print(f"Jobs/líneas borradas: {len(removed)}")
        if removed:
            print("\n=== Líneas que se borrarían ===")
            for ln in removed:
                print(ln)

        if args.dry_run or not removed:
            print("\n(dry-run) No se aplicaron cambios." if args.dry_run else "\nNo había nada que borrar.")
            return

        set_crontab_lines_local(new_lines)
        print("\n✅ Crontab actualizado (local).")

if __name__ == "__main__":
    main()
