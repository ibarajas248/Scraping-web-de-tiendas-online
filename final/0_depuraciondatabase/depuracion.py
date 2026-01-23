#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# python3 depurar_historico_uno_por_dia.py --dry-run --from "2026-01-01" --to "2026-02-01"
# python3 depurar_historico_uno_por_dia.py --apply   --from "2026-01-01" --to "2026-02-01"




'''

para correr, sin apply (simulacion)

/home/intelligenceblue-scrap/.venvs/scrap/bin/python -u \
/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/scrap_tiendas/pruebas/depuracion.py \
--dry-run \
--from "2026-01-01" \
--to "2026-02-01"




/home/intelligenceblue-scrap/.venvs/scrap/bin/python -u \
/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/scrap_tiendas/pruebas/depuracion.py \
--apply \
--from "2025-08-01" \
--to "2025-12-31"


con apply






'''
import os
import sys
import argparse
from datetime import datetime, timedelta

# Para importar tu get_conn desde /../..
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL


def parse_date(s: str) -> datetime:
    # Acepta YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS o YYYY-MM-DD HH:MM:SS
    s = s.strip().replace("T", " ")
    if len(s) == 10:
        return datetime.strptime(s, "%Y-%m-%d")
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def main():
    ap = argparse.ArgumentParser(
        description="Depura historico_precios para que por día exista solo 1 fila por (tienda_id, producto_tienda_id): la última del día."
    )
    ap.add_argument("--dry-run", action="store_true", help="Solo muestra conteos; NO borra.")
    ap.add_argument("--apply", action="store_true", help="Aplica borrado.")
    ap.add_argument("--batch", type=int, default=50000, help="Tamaño del batch de borrado (default 50000).")
    ap.add_argument("--from", dest="from_dt", default=None, help="Inicio (YYYY-MM-DD o YYYY-MM-DD HH:MM:SS).")
    ap.add_argument("--to", dest="to_dt", default=None, help="Fin EXCLUSIVO (YYYY-MM-DD o YYYY-MM-DD HH:MM:SS).")
    ap.add_argument("--show-samples", type=int, default=20, help="Muestra N ids ejemplo a borrar (default 20).")
    args = ap.parse_args()

    if not args.apply and not args.dry_run:
        # Por seguridad: si no especifica nada, asumimos dry-run
        args.dry_run = True

    if args.apply and args.dry_run:
        print("Usa solo uno: --dry-run o --apply")
        sys.exit(2)

    from_dt = parse_date(args.from_dt) if args.from_dt else None
    to_dt = parse_date(args.to_dt) if args.to_dt else None

    # Conexión
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    # Detectar versión MySQL (para window functions)
    cur.execute("SELECT VERSION()")
    ver = (cur.fetchone() or [""])[0]
    major = int(ver.split(".")[0]) if ver and ver[0].isdigit() else 0
    has_window = major >= 8

    if not has_window:
        print(f"⚠️ MySQL version: {ver}. Este script está optimizado para MySQL 8+ (window functions).")
        print("   Si estás en MySQL 5.7, te lo adapto con una estrategia alternativa (más lenta).")
        sys.exit(1)

    # Filtro por rango de fechas (opcional)
    where = []
    params = []
    if from_dt:
        where.append("hp.capturado_en >= %s")
        params.append(from_dt.strftime("%Y-%m-%d %H:%M:%S"))
    if to_dt:
        where.append("hp.capturado_en < %s")
        params.append(to_dt.strftime("%Y-%m-%d %H:%M:%S"))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # 1) Contar grupos duplicados (por día)
    q_groups = f"""
        SELECT COUNT(*) FROM (
          SELECT 1
          FROM historico_precios hp
          {where_sql}
          GROUP BY hp.tienda_id, hp.producto_tienda_id, DATE(hp.capturado_en)
          HAVING COUNT(*) > 1
        ) x
    """
    cur.execute(q_groups, params)
    dup_groups = cur.fetchone()[0]

    # 2) Contar filas "sobrantes" (las que se borrarían)
    q_dupes = f"""
        SELECT COUNT(*) FROM (
          SELECT hp.id,
                 ROW_NUMBER() OVER (
                   PARTITION BY hp.tienda_id, hp.producto_tienda_id, DATE(hp.capturado_en)
                   ORDER BY hp.capturado_en DESC, hp.id DESC
                 ) AS rn
          FROM historico_precios hp
          {where_sql}
        ) t
        WHERE t.rn > 1
    """
    cur.execute(q_dupes, params)
    to_delete = cur.fetchone()[0]

    print("=== Diagnóstico ===")
    print(f"MySQL: {ver} (window functions: {'OK' if has_window else 'NO'})")
    print(f"Grupos duplicados (tienda_id, producto_tienda_id, día): {dup_groups}")
    print(f"Filas sobrantes a borrar (dejando solo la última por día): {to_delete}")

    if to_delete == 0:
        print("✅ No hay nada que depurar.")
        conn.rollback()
        cur.close()
        conn.close()
        return

    # Mostrar ejemplos
    if args.show_samples and args.show_samples > 0:
        q_samples = f"""
            SELECT t.id, t.tienda_id, t.producto_tienda_id, t.capturado_en
            FROM (
              SELECT hp.*,
                     ROW_NUMBER() OVER (
                       PARTITION BY hp.tienda_id, hp.producto_tienda_id, DATE(hp.capturado_en)
                       ORDER BY hp.capturado_en DESC, hp.id DESC
                     ) AS rn
              FROM historico_precios hp
              {where_sql}
            ) t
            WHERE t.rn > 1
            ORDER BY t.id
            LIMIT {int(args.show_samples)}
        """
        cur.execute(q_samples, params)
        rows = cur.fetchall()
        print("\n=== Ejemplos de filas a borrar (id, tienda_id, producto_tienda_id, capturado_en) ===")
        for r in rows:
            print(r)

    if args.dry_run:
        print("\n(dry-run) No se aplicaron cambios.")
        conn.rollback()
        cur.close()
        conn.close()
        return

    # APPLY: borrado por batches usando tabla temporal
    print("\n=== Aplicando depuración (batches) ===")
    cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_hp_del (
          id BIGINT UNSIGNED NOT NULL PRIMARY KEY
        ) ENGINE=InnoDB
    """)
    conn.commit()

    total_deleted = 0
    batch = max(1000, int(args.batch))

    while True:
        cur.execute("TRUNCATE TABLE tmp_hp_del")
        conn.commit()

        # Insertar ids a borrar en batch
        q_fill = f"""
            INSERT INTO tmp_hp_del (id)
            SELECT id FROM (
              SELECT hp.id,
                     ROW_NUMBER() OVER (
                       PARTITION BY hp.tienda_id, hp.producto_tienda_id, DATE(hp.capturado_en)
                       ORDER BY hp.capturado_en DESC, hp.id DESC
                     ) AS rn
              FROM historico_precios hp
              {where_sql}
            ) t
            WHERE t.rn > 1
            LIMIT {batch}
        """
        cur.execute(q_fill, params)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM tmp_hp_del")
        n = cur.fetchone()[0]
        if n == 0:
            break

        # Borrar
        cur.execute("""
            DELETE hp
            FROM historico_precios hp
            JOIN tmp_hp_del d ON d.id = hp.id
        """)
        deleted_now = cur.rowcount
        conn.commit()

        total_deleted += max(0, deleted_now)
        print(f"Batch borrado: {deleted_now} | Total borrado: {total_deleted}")

    print(f"\n✅ Depuración terminada. Total borrado: {total_deleted}")

    # Re-chequeo final rápido (solo grupos)
    cur.execute(q_groups, params)
    dup_groups_after = cur.fetchone()[0]
    print(f"Grupos duplicados restantes: {dup_groups_after}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
