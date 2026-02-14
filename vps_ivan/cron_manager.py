#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def cron_manager():

    import paramiko
    import streamlit as st
    import pandas as pd
    import re
    import os  # <- para basenames
    from typing import List, Tuple, Optional
    import shlex
    from pathlib import PurePosixPath

    # =========================
    # CONFIG POR DEFECTO (EDITABLE EN LA UI)
    # =========================
    DEFAULT_SSH_HOST = "187.77.10.108"   # <- cambia si aplica
    DEFAULT_SSH_PORT = 22
    DEFAULT_SSH_USER = "root"            # <- cambia si aplica
    DEFAULT_SSH_PASS = "&X(gr2'4(UMVS80bKs3E"                # <- por seguridad, ponlo en la UI (no hardcode)

    BASE_DIR = "/root/streamlit_test/scraptiendas"
    DEFAULT_PYTHON_BIN = "/root/streamlit_test/.venv/bin/python"

    # =========================
    # SSH HELPERS
    # =========================

    def pretty_cmd(cmd: str, base_dir: str) -> str:
        """
        Devuelve una vista corta del comando, priorizando la ruta del .py
        como 'carpeta/script.py'. Si no encuentra .py, intenta algo razonable.
        """
        cmd_no_redirect = re.split(r"\s+>\s*/dev/null.*$", cmd)[0].strip()

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
        if ("no crontab for" in err.lower()) or (not out.strip() and not err.strip()):
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
        return {
            "m": d["m"],
            "h": d["h"],
            "dom": d["dom"],
            "mon": d["mon"],
            "dow": d["dow"],
            "cmd": d["cmd"],
        }

    def format_cron_line(m: str, h: str, dom: str, mon: str, dow: str, cmd: str) -> str:
        return f"{m} {h} {dom} {mon} {dow} {cmd}".strip()

    def format_spec_line(spec: str, cmd: str) -> str:
        return f"{spec} {cmd}".strip()

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
    # UI: SELECTORS
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

        st.header(" Python")
        python_bin = st.text_input("Ruta intérprete Python", value=DEFAULT_PYTHON_BIN)

        st.header(" Base de scripts")
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

    st.subheader(" Cronjobs actuales")
    if rows:
        df = pd.DataFrame(rows)
        df_display = df.drop(columns=["line_idx"]).copy()
        df_display["Comando"] = df_display["Comando"].apply(lambda c: pretty_cmd(c, BASE_DIR))
        st.dataframe(df_display, use_container_width=True)
    else:
        st.info("No hay cronjobs cargados para este usuario.")

    # ======== ELIMINAR JOB ========
    with st.expander(" Eliminar un job", expanded=False):
        if rows:
            labels = []
            opt_map = {}  # label -> line_idx
            for r in rows:
                short = pretty_cmd(r["Comando"], BASE_DIR)
                if r["Tipo"] == "5campos":
                    label = (f"[{r['Tipo']}] {r['Spec/Min']} {r['Hora']} {r['Día']} "
                             f"{r['Mes']} {r['DíaSem']}  →  {short}  [#{r['line_idx']}]")
                else:
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
    # ✅ aquí está lo que pediste: al cambiar carpeta/archivo, se muestra ruta final
    st.subheader(" Explorar scripts en scrap_tiendas")
    colX, colY = st.columns(2)

    full = None
    py_file = None

    with colX:
        try:
            dirs = list_dirs(host, port, user, password, BASE_DIR)
        except Exception as e:
            dirs = []
            st.error(f"No se pudo listar directorios: {e}")

        folder_names = [d.split("/")[-1] for d in dirs]
        folder = st.selectbox("Carpeta", options=folder_names if folder_names else ["(sin carpetas)"])

        if folder and folder != "(sin carpetas)":
            full = f"{BASE_DIR}/{folder}"
            st.caption(f"📁 Ruta carpeta: `{full}`")

    with colY:
        if full:
            try:
                py_files = list_py_files(host, port, user, password, full)
            except Exception as e:
                py_files = []
                st.error(f"No se pudo listar scripts: {e}")

            if py_files:
                name_to_path = {os.path.basename(p): p for p in py_files}
                selected_name = st.selectbox("Script .py", options=list(name_to_path.keys()))
                py_file = name_to_path[selected_name]
                st.caption(f"📄 Ruta script: `{py_file}`")
            else:
                st.warning("No hay .py en esta carpeta.")

    if py_file:
        st.success(f"Script seleccionado: `{os.path.basename(py_file)}`")

    # ======== FORMULARIO AGREGAR / EDITAR ========
    st.subheader(" Agregar / Editar job")

    mode = st.radio("Modo", ["Agregar nuevo", "Editar existente"], horizontal=True)

    if mode == "Editar existente" and rows:
        opt_edit_map = {
            f"[{r['Tipo']}] {r['Spec/Min']} {r['Hora']} {r['Día']} {r['Mes']} {r['DíaSem']}  →  {r['Comando']}": r
            for r in rows
        }
        to_edit_label = st.selectbox("Selecciona el job a editar", ["(ninguno)"] + list(opt_edit_map.keys()))
        selected_row = opt_edit_map.get(to_edit_label)
    else:
        selected_row = None

    if selected_row and mode == "Editar existente":
        if selected_row["Tipo"] == "5campos":
            st.info("Se cargaron los valores actuales. Ajusta y guarda.")
            st.code(format_cron_line(
                selected_row["Spec/Min"], selected_row["Hora"], selected_row["Día"],
                selected_row["Mes"], selected_row["DíaSem"], selected_row["Comando"]
            ))
        else:
            st.warning("Este job usa sintaxis @spec (por ejemplo @reboot). La edición visual aquí está pensada para formato de 5 campos.")

    # ---------- A PARTIR DE AQUÍ: SIN RERUN HASTA SUBMIT ----------
    with st.form(key="cron_form"):
        col1, col2, col3, col4, col5 = st.columns(5)

        minute = col1.selectbox(
            "Minuto",
            options=minutes_options(),
            index=minutes_options().index("*/15") if "*/15" in minutes_options() else 0
        )
        hour = col2.selectbox("Hora", options=hours_options(), index=hours_options().index("*"))
        day = col3.selectbox("Día del mes", options=day_options(), index=day_options().index("*"))
        month = col4.selectbox("Mes", options=month_options(), index=month_options().index("*"))
        weekday_label = col5.selectbox(
            "Día de semana",
            options=[lbl for _, lbl in weekday_options()],
            index=0
        )
        weekday_val = [v for v, lbl in weekday_options() if lbl == weekday_label][0]

        # Comando: se arma con python_bin + script elegido
        default_cmd = ""
        if py_file:
            default_cmd = f"{python_bin} {py_file}"

        # sugerir comando actual si estás editando y no hay py_file seleccionado
        if selected_row and mode == "Editar existente" and (not py_file):
            if selected_row.get("Comando"):
                default_cmd = selected_row["Comando"]

        cmd = st.text_input("Comando a ejecutar", value=default_cmd, key="cmd_input")

        # Redirección de salida (mantengo tu comportamiento)
        st.markdown("**Redirección de salida (opcional):**")
        log_choice = st.radio("Logs", ["Enviar a /dev/null"], horizontal=True, key="log_choice")
        log_path = ""

        def apply_redirect(command: str) -> str:
            if log_choice == "Enviar a /dev/null":
                return f"{command} > /dev/null 2>&1"
            # Mantengo tu bloque comentado original (no lo implemento aquí)
            return command

        cA, cB = st.columns(2)
        submit_add = cA.form_submit_button("➕ Agregar job")
        submit_save = cB.form_submit_button("💾 Guardar cambios en job seleccionado")

    # --- Procesamiento tras el submit ---
    if mode == "Agregar nuevo" and submit_add:
        if not cmd.strip():
            st.error("Debes especificar un comando.")
        else:
            new_cmd = apply_redirect(cmd.strip())
            new_line = format_cron_line(minute, hour, day, month, weekday_val, new_cmd)
