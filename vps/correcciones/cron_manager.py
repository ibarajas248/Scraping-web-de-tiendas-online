#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def cron_manager():
    import paramiko
    import streamlit as st
    import pandas as pd
    import re
    import os  # para manejar basenames
    from typing import List, Tuple, Optional
    import shlex
    from pathlib import PurePosixPath
    import hashlib
    from datetime import datetime

    # =========================
    # CONFIG POR DEFECTO (EDITABLE EN LA UI)
    # =========================
    DEFAULT_SSH_HOST = "179.61.219.207"
    DEFAULT_SSH_PORT = 22
    DEFAULT_SSH_USER = "intelligenceblue-scrap"
    DEFAULT_SSH_PASS = "WLlMf047NTAskTjijHju"

    BASE_DIR = "/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/scrap_tiendas"
    DEFAULT_PYTHON_BIN = "/home/intelligenceblue-scrap/.venvs/scrap/bin/python"

    # =========================
    # SSH HELPERS
    # =========================
    def _ssh_client(host: str, port: int, user: str, password: str) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, port, user, password, timeout=15)
        return client

    def run_ssh_command(host: str, port: int, user: str, password: str, command: str) -> Tuple[str, str, int]:
        client = _ssh_client(host, port, user, password)
        stdin, stdout, stderr = client.exec_command(command)
        out = stdout.read().decode(errors="ignore")
        err = stderr.read().decode(errors="ignore")
        rc = stdout.channel.recv_exit_status()
        client.close()
        return out, err, rc

    def run_ssh_stdin(host: str, port: int, user: str, password: str, command: str, input_data: str) -> Tuple[str, str, int]:
        client = _ssh_client(host, port, user, password)
        chan = client.get_transport().open_session()
        chan.exec_command(command)
        chan.send(input_data.encode())
        chan.shutdown_write()
        out = chan.recv(65535).decode(errors="ignore")
        err = ""
        rc = chan.recv_exit_status()
        client.close()
        return out, err, rc

    # =========================
    # CRONTAB HELPERS
    # =========================
    _CRON_RE = re.compile(
        r"^\s*"
        r"(?P<m>\*|[\d\/,\-]+)\s+"
        r"(?P<h>\*|[\d\/,\-]+)\s+"
        r"(?P<dom>\*|[\d\/,\-]+)\s+"
        r"(?P<mon>\*|[\d\/,\-]+)\s+"
        r"(?P<dow>\*|[\d\/,\-]+)\s+"
        r"(?P<cmd>.+)$"
    )

    def get_crontab_lines(host: str, port: int, user: str, password: str) -> List[str]:
        out, err, rc = run_ssh_command(host, port, user, password, "crontab -l || true")
        # Si no hay crontab, devolvemos lista vacía
        if ("no crontab for" in (out + err).lower()) or (not out.strip() and not err.strip()):
            return []
        return out.splitlines()

    def set_crontab_lines(host: str, port: int, user: str, password: str, lines: List[str]) -> None:
        content = "\n".join(lines).rstrip() + "\n"
        _, err, rc = run_ssh_stdin(host, port, user, password, "crontab -", content)
        if rc != 0:
            raise RuntimeError(f"Error actualizando crontab: {err or 'desconocido'}")

    def is_env_or_comment(line: str) -> bool:
        ls = line.strip()
        return (not ls) or ls.startswith("#") or ("=" in ls.split()[0] if ls else False)

    def is_spec_line(line: str) -> bool:
        # @reboot, @yearly, etc.
        return line.strip().startswith("@")

    def parse_cron_line(line: str) -> Optional[dict]:
        if is_env_or_comment(line):
            return None
        if is_spec_line(line):
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                return {"spec": parts[0], "cmd": parts[1]}
            else:
                return {"spec": parts[0], "cmd": ""}
        m = _CRON_RE.match(line)
        if not m:
            return None
        d = m.groupdict()
        return {"m": d["m"], "h": d["h"], "dom": d["dom"], "mon": d["mon"], "dow": d["dow"], "cmd": d["cmd"]}

    def format_cron_line(m: str, h: str, dom: str, mon: str, dow: str, cmd: str) -> str:
        return f"{m} {h} {dom} {mon} {dow} {cmd}".strip()

    def format_spec_line(spec: str, cmd: str) -> str:
        return f"{spec} {cmd}".strip()

    # =========================
    # LOCK + TIMEOUT HELPERS
    # =========================
    def _extract_script_for_lock(cmd: str) -> str:
        """
        Intenta hallar el .py del comando para generar un lock estable.
        Si no hay .py, usa todo el comando. Quita redirecciones y desempaqueta flock/timeout.
        """
        # quitar redirección
        cmd_no_redirect = re.split(r"\s+>\s*/dev/null.*$", cmd)[0].strip()

        # detectar flock -c '...'
        m = re.search(r"\bflock\s+-n\s+[^\s]+\s+-c\s+(.+)$", cmd_no_redirect)
        if m:
            inner = m.group(1).strip()
            if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
                inner = inner[1:-1]
            # quitar timeout inicial si existe
            t = re.match(r"timeout\s+\S+\s+(.+)$", inner)
            if t:
                cmd_no_redirect = t.group(1)
            else:
                cmd_no_redirect = inner

        try:
            tokens = shlex.split(cmd_no_redirect)
        except ValueError:
            tokens = cmd_no_redirect.split()

        for tkn in tokens:
            if tkn.endswith(".py"):
                return tkn

        return cmd_no_redirect

    def _safe_lockfile_from_cmd(cmd: str) -> str:
        """
        Genera /tmp/cronlock-<hash>-<basename>.lock
        - hash asegura unicidad, basename ayuda a identificar a simple vista.
        """
        base = os.path.basename(_extract_script_for_lock(cmd)) or "cmd"
        h = hashlib.sha1(cmd.encode("utf-8", errors="ignore")).hexdigest()[:10]
        base_sane = re.sub(r"[^A-Za-z0-9._-]+", "-", base)
        return f"/tmp/cronlock-{h}-{base_sane}.lock"

    def ensure_locked_timeout(cmd: str, timeout_hours: int = 10) -> str:
        """
        Si el comando NO está envuelto con flock/timeout, lo envuelve así:
          flock -n /tmp/cronlock-<...>.lock -c '<timeout 10h ...>'
        Si ya está con flock, se respeta y no se doble-envuelve.
        """
        if re.search(r"\bflock\b.*\s-c\s", cmd):
            return cmd  # ya tiene flock

        lockfile = _safe_lockfile_from_cmd(cmd)
        sub = f"timeout {timeout_hours}h {cmd}"
        sub_quoted = shlex.quote(sub)
        return f"flock -n {lockfile} -c {sub_quoted}"

    # =========================
    # FILE EXPLORER (REMOTE)
    # =========================
    def list_dirs(host: str, port: int, user: str, password: str, path: str) -> List[str]:
        out, _, _ = run_ssh_command(host, port, user, password, f"ls -1 -d {path}/*/ 2>/dev/null || true")
        lines = [l.strip().rstrip("/") for l in out.splitlines() if l.strip()]
        return lines

    def list_py_files(host: str, port: int, user: str, password: str, path: str) -> List[str]:
        out, _, _ = run_ssh_command(host, port, user, password, f"ls -1 {path}/*.py 2>/dev/null || true")
        lines = [l.strip() for l in out.splitlines() if l.strip()]
        return lines

    # =========================
    # UI HELPERS
    # =========================
    def minutes_options():
        common = ["*", "*/5", "*/10", "*/15", "*/30"]
        nums = [str(i) for i in range(60)]
        return common + nums

    def hours_options():
        return ["*"] + [str(i) for i in range(24)]

    def day_options():
        return ["*"] + [str(i) for i in range(1, 32)]

    def month_options():
        return ["*"] + [str(i) for i in range(1, 13)]

    def weekday_options():
        labels = [
            ("*", "* (Cualquiera)"),
            ("0", "0 (Dom)"),
            ("1", "1 (Lun)"),
            ("2", "2 (Mar)"),
            ("3", "3 (Mié)"),
            ("4", "4 (Jue)"),
            ("5", "5 (Vie)"),
            ("6", "6 (Sáb)"),
        ]
        return labels

    def pretty_cmd(cmd: str, base_dir: str) -> str:
        """
        Muestra una vista corta del .py aunque esté envuelto con flock/timeout.
        """
        cmd_no_redirect = re.split(r"\s+>\s*/dev/null.*$", cmd)[0].strip()

        m = re.search(r"\bflock\b\s+-n\s+[^\s]+\s+-c\s+(.+)$", cmd_no_redirect)
        if m:
            inner = m.group(1).strip()
            if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
                inner = inner[1:-1]
            t = re.match(r"timeout\s+\S+\s+(.+)$", inner)
            if t:
                cmd_no_redirect = t.group(1).strip()
            else:
                cmd_no_redirect = inner

        try:
            tokens = shlex.split(cmd_no_redirect)
        except ValueError:
            tokens = cmd_no_redirect.split()

        script_path = None
        for t in tokens:
            if t.endswith(".py"):
                script_path = t
                break

        if not script_path:
            m = re.search(r"(/[^ \t'\"<>|&]+\.(?:py|sh|pl|rb))", cmd_no_redirect)
            if m:
                script_path = m.group(1)

        if script_path:
            p = PurePosixPath(script_path)
            try:
                short = str(p.relative_to(base_dir).as_posix())
            except Exception:
                parts = p.parts
                short = "/".join(parts[-2:]) if len(parts) >= 2 else p.name
            return short

        m2 = re.search(r"/([^/\s]+/[^/\s]+)$", cmd_no_redirect)
        if m2:
            return m2.group(1)

        return cmd_no_redirect[:80]

    # =========================
    # STREAMLIT APP
    # =========================
    st.set_page_config(page_title="Panel CronJobs VPS", layout="wide")
    st.title(" Panel  de CronJobs ")

    with st.sidebar:
        st.header("🔐 Conexión SSH")
        host = st.text_input("Host", value=DEFAULT_SSH_HOST)
        port = st.number_input("Puerto", value=DEFAULT_SSH_PORT, step=1)
        user = st.text_input("Usuario", value=DEFAULT_SSH_USER)
        password = st.text_input("Password", value=DEFAULT_SSH_PASS, type="password")

        st.header("🐍 Python")
        python_bin = st.text_input("Ruta intérprete Python", value=DEFAULT_PYTHON_BIN)

        st.header("📁 Base de scripts")
        st.text_input("Directorio base", value=BASE_DIR, disabled=True)

    # ======== CARGA CRONTAB ========
    try:
        lines = get_crontab_lines(host, port, user, password)
    except Exception as e:
        st.error(f"No se pudo leer el crontab remoto: {e}")
        st.stop()

    # Construir tabla de jobs (manteniendo índices reales de 'lines')
    rows = []
    for idx, line in enumerate(lines):
        parsed = parse_cron_line(line)
        if not parsed:
            continue
        if "spec" in parsed:
            rows.append({
                "line_idx": idx, "Tipo": "spec", "Spec/Min": parsed["spec"],
                "Hora": "", "Día": "", "Mes": "", "DíaSem": "", "Comando": parsed["cmd"]
            })
        else:
            rows.append({
                "line_idx": idx, "Tipo": "5campos", "Spec/Min": parsed["m"],
                "Hora": parsed["h"], "Día": parsed["dom"], "Mes": parsed["mon"],
                "DíaSem": parsed["dow"], "Comando": parsed["cmd"]
            })

    st.subheader("🕒 Cronjobs actuales")
    if rows:
        df = pd.DataFrame(rows)
        df_display = df.drop(columns=["line_idx"]).copy()
        df_display["Comando"] = df_display["Comando"].apply(lambda c: pretty_cmd(c, BASE_DIR))
        st.dataframe(df_display, use_container_width=True)
    else:
        st.info("No hay cronjobs cargados para este usuario.")

    # ======== ELIMINAR JOB ========
    with st.expander("🗑️ Eliminar un job", expanded=False):
        if rows:
            labels = []
            opt_map = {}  # label -> line_idx
            for r in rows:
                short = pretty_cmd(r["Comando"], BASE_DIR)
                if r["Tipo"] == "5campos":
                    label = (f"[{r['Tipo']}] {r['Spec/Min']} {r['Hora']} {r['Día']} "
                             f"{r['Mes']} {r['DíaSem']}  →  {short}  [#{r['line_idx']}]")
                else:  # @spec
                    label = f"[{r['Tipo']}] {r['Spec/Min']}  →  {short}  [#{r['line_idx']}]"
                labels.append(label)
                opt_map[label] = r["line_idx"]

            to_del_label = st.selectbox("Selecciona el job a eliminar", ["(ninguno)"] + labels)
            if to_del_label != "(ninguno)" and st.button("Eliminar seleccionado"):
                del_idx = opt_map[to_del_label]
                new_lines = [ln for i, ln in enumerate(lines) if i != del_idx]
                try:
                    set_crontab_lines(host, port, user, password, new_lines)
                    st.success("Job eliminado ✅. Recarga la página para ver cambios.")
                except Exception as e:
                    st.error(f"Error al eliminar job: {e}")
        else:
            st.info("No hay jobs para eliminar.")

    # ======== EXPLORADOR DE SCRIPTS ========
    st.subheader("📂 Explorar scripts en scrap_tiendas")
    colX, colY = st.columns(2)

    with colX:
        try:
            dirs = list_dirs(host, port, user, password, BASE_DIR)
        except Exception as e:
            dirs = []
            st.error(f"No se pudo listar directorios: {e}")

        folder_names = [d.split("/")[-1] for d in dirs]
        folder = st.selectbox("Carpeta", options=folder_names if folder_names else ["(sin carpetas)"])

    with colY:
        py_file = None
        if folder and folder != "(sin carpetas)":
            full = f"{BASE_DIR}/{folder}"
            try:
                py_files = list_py_files(host, port, user, password, full)
            except Exception as e:
                py_files = []
                st.error(f"No se pudo listar scripts: {e}")
            if py_files:
                name_to_path = {os.path.basename(p): p for p in py_files}
                selected_name = st.selectbox("Script .py", options=list(name_to_path.keys()))
                py_file = name_to_path[selected_name]
            else:
                st.warning("No hay .py en esta carpeta.")
    if py_file:
        st.success(f"Script seleccionado: `{os.path.basename(py_file)}`")

    # ======== AGREGAR / EDITAR ========
    st.subheader("✍️ Agregar / Editar job")

    mode = st.radio("Modo", ["Agregar nuevo", "Editar existente"], horizontal=True)

    if mode == "Editar existente" and rows:
        def _label_for_row(r):
            if r["Tipo"] == "5campos":
                return f"[{r['Tipo']}] {r['Spec/Min']} {r['Hora']} {r['Día']} {r['Mes']} {r['DíaSem']}  →  {r['Comando']}"
            else:
                return f"[{r['Tipo']}] {r['Spec/Min']}  →  {r['Comando']}"

        opt_edit_map = { _label_for_row(r): r for r in rows }
        to_edit_label = st.selectbox("Selecciona el job a editar", ["(ninguno)"] + list(opt_edit_map.keys()))
        selected_row = opt_edit_map.get(to_edit_label)
    else:
        selected_row = None

    if selected_row and mode == "Editar existente":
        if selected_row["Tipo"] == "5campos":
            st.info("Se cargaron los valores actuales. Ajusta y guarda.")
            st.code(
                format_cron_line(
                    selected_row["Spec/Min"], selected_row["Hora"], selected_row["Día"],
                    selected_row["Mes"], selected_row["DíaSem"], selected_row["Comando"]
                )
            )
        else:
            st.warning("Este job usa sintaxis @spec (p. ej. @reboot). La edición visual aquí es para formato de 5 campos.")

    # opciones para selects (evita recomputar)
    minutes_opts = minutes_options()
    hours_opts = hours_options()
    days_opts = day_options()
    months_opts = month_options()
    weekday_opts = weekday_options()

    with st.form(key="cron_form"):
        col1, col2, col3, col4, col5 = st.columns(5)
        minute = col1.selectbox("Minuto", options=minutes_opts, index=minutes_opts.index("*/15") if "*/15" in minutes_opts else 0)
        hour = col2.selectbox("Hora", options=hours_opts, index=0)
        day = col3.selectbox("Día del mes", options=days_opts, index=0)
        month = col4.selectbox("Mes", options=months_opts, index=0)
        weekday_label = col5.selectbox("Día de semana", options=[lbl for _, lbl in weekday_opts], index=0)
        weekday_val = [v for v, lbl in weekday_opts if lbl == weekday_label][0]

        # Comando sugerido
        default_cmd = ""
        if py_file:
            default_cmd = f"{DEFAULT_PYTHON_BIN} {py_file}" if not DEFAULT_PYTHON_BIN else f"{DEFAULT_PYTHON_BIN} {py_file}"
        if selected_row and mode == "Editar existente" and (not py_file):
            if selected_row.get("Comando"):
                default_cmd = selected_row["Comando"]

        cmd = st.text_input("Comando a ejecutar", value=default_cmd, key="cmd_input")

        st.markdown("**Redirección de salida (fijada a /dev/null para no guardar logs):**")
        st.caption("El panel envuelve con `flock -n` + `timeout 10h` y redirige a `/dev/null 2>&1` automáticamente.")

        def apply_redirect(command: str) -> str:
            # Envolver con flock + timeout 10h
            wrapped = ensure_locked_timeout(command.strip(), timeout_hours=10)
            # Redirigir salida (sin logs)
            return f"{wrapped} > /dev/null 2>&1"

        cA, cB = st.columns(2)
        submit_add = cA.form_submit_button("➕ Agregar job")
        submit_save = cB.form_submit_button("💾 Guardar cambios en job seleccionado")

    # --- Procesamiento tras el submit ---
    if mode == "Agregar nuevo" and submit_add:
        if not cmd.strip():
            st.error("Debes especificar un comando.")
        else:
            new_cmd = apply_redirect(cmd.strip())
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            comment = f"# añadido por panel {timestamp}"
            new_line = format_cron_line(minute, hour, day, month, weekday_val, new_cmd)
            new_lines = lines[:] + [comment, new_line]
            try:
                set_crontab_lines(host, port, user, password, new_lines)
                st.success(f"Agregado ✅: {new_line}")
            except Exception as e:
                st.error(f"Error agregando job: {e}")

    if mode == "Editar existente" and selected_row and submit_save:
        if selected_row["Tipo"] != "5campos":
            st.warning("Solo se puede editar aquí el formato de 5 campos. Para @reboot, edítalo como texto fuera de este panel.")
        elif not cmd.strip():
            st.error("Debes especificar un comando.")
        else:
            new_cmd = apply_redirect(cmd.strip())
            updated = format_cron_line(minute, hour, day, month, weekday_val, new_cmd)
            new_lines = lines[:]
            try:
                new_lines[selected_row["line_idx"]] = updated
                set_crontab_lines(host, port, user, password, new_lines)
                st.success(f"Actualizado ✅: {updated}")
            except Exception as e:
                st.error(f"Error actualizando job: {e}")


# Para ejecutarlo como app de Streamlit:
if __name__ == "__main__":
    import streamlit as st
    cron_manager()
