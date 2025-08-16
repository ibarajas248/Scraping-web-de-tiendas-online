#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import unicodedata
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
from mysql.connector import Error as MySQLError
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from base_datos import get_conn  # host=localhost, port=3310, db=analisis_retail

st.set_page_config(page_title="Vincular SKUs a productos con EAN", layout="wide")

# ==========================
# Normalización y parsing
# ==========================
_STOP = {"de","la","el","los","las","con","para","por","sin","y","o","u","en","x","lt","l","ml","kg","gr","g","cc"}

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).lower().strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    toks = [t for t in s.split() if t and t not in _STOP]
    return " ".join(toks)

def combo_string(nombre: str, marca: Optional[str]) -> str:
    m = _norm(marca) if marca else ""
    return (m + " " + _norm(nombre)).strip()

_pack_re = re.compile(r"(?:^|\s)(?:x\s*|(\d+)\s*[xX])(\d+)(?=\D|$)")
_units_re = re.compile(r"(?:^|\s)x\s*(\d+)(?=\D|$)")
_size_re = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(ml|cc|l|lt|litro?s?|g|gr|gramos?|kg|kilos?)",
    re.I
)

def parse_size_pack(text: str) -> Tuple[Optional[float], Optional[float], int]:
    """
    Devuelve (ml_total, g_total, unidades). Convierte L→ml, kg→g.
    Si hay pack (xN) intenta multiplicar.
    """
    if not text:
        return None, None, 1
    t = text.lower()
    # unidades/packs
    units = 1
    m_u = _units_re.search(t)
    if m_u:
        try:
            units = int(m_u.group(1))
        except Exception:
            units = 1

    # tamaños
    ml_total = None
    g_total = None
    sizes = _size_re.findall(t)
    # Si hay varios tamaños, tomamos el mayor (p. ej. “pack x6 500 ml” → 500 ml * 6)
    best_ml = 0.0
    best_g = 0.0
    for num, unit in sizes:
        try:
            val = float(num.replace(",", "."))
        except Exception:
            continue
        unit = unit.lower()
        if unit in ("l","lt","litro","litros"):
            val_ml = val * 1000.0
            best_ml = max(best_ml, val_ml)
        elif unit in ("ml","cc"):
            val_ml = val * 1.0
            best_ml = max(best_ml, val_ml)
        elif unit in ("kg","kilo","kilos"):
            val_g = val * 1000.0
            best_g = max(best_g, val_g)
        elif unit in ("g","gr","gramo","gramos"):
            val_g = val * 1.0
            best_g = max(best_g, val_g)
    if best_ml > 0:
        ml_total = best_ml * units
    if best_g > 0:
        g_total = best_g * units
    return ml_total, g_total, units

def size_bonus(base_size: Tuple[Optional[float], Optional[float], int],
               dest_size: Tuple[Optional[float], Optional[float], int]) -> float:
    b_ml, b_g, _ = base_size
    d_ml, d_g, _ = dest_size
    # mismo tipo
    if b_ml and d_ml:
        diff = abs(b_ml - d_ml) / max(b_ml, d_ml)
        if diff <= 0.2: return 0.10
        if diff <= 0.35: return 0.05
        return -0.08
    if b_g and d_g:
        diff = abs(b_g - d_g) / max(b_g, d_g)
        if diff <= 0.2: return 0.10
        if diff <= 0.35: return 0.05
        return -0.08
    # tipos distintos (ml vs g) penaliza
    if (b_ml and d_g) or (b_g and d_ml):
        return -0.10
    return 0.0

def brand_bonus(b_brand: str, d_brand: str) -> float:
    b = _norm(b_brand)
    d = _norm(d_brand)
    if not b or not d:
        return 0.0
    if b == d:
        return 0.08
    # marca incluida (ej submarca)
    if b in d or d in b:
        return 0.04
    return -0.03

# ==========================
# DB helpers
# ==========================
def run_select_df(sql: str, params: Tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        cur.close()
        conn.close()

def run_exec(sql: str, params: Tuple = (), many: bool = False):
    conn = get_conn()
    conn.start_transaction()
    try:
        cur = conn.cursor()
        if many:
            cur.executemany(sql, params)
        else:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ==========================
# Queries
# ==========================
@st.cache_data(ttl=120)
def get_filtros_base():
    tiendas = run_select_df("SELECT id, nombre FROM tiendas ORDER BY nombre")
    marcas = run_select_df("SELECT DISTINCT marca FROM productos WHERE marca IS NOT NULL AND marca<>'' ORDER BY marca")
    categorias = run_select_df("SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL AND categoria<>'' ORDER BY categoria")
    return tiendas, marcas, categorias

@st.cache_data(ttl=60, show_spinner=False)
def get_pt_sin_ean(tienda_id: Optional[int], marca: Optional[str], categoria: Optional[str],
                   q_base: Optional[str], limit: int = 2000) -> pd.DataFrame:
    sql = """
    SELECT
      pt.id            AS pt_id,
      pt.tienda_id,
      t.nombre         AS tienda,
      pt.sku_tienda,
      pt.nombre_tienda,
      p.id             AS producto_id,
      p.nombre         AS producto,
      p.marca,
      p.categoria,
      p.subcategoria
    FROM producto_tienda pt
    JOIN productos p   ON p.id = pt.producto_id
    JOIN tiendas   t   ON t.id = pt.tienda_id
    WHERE p.ean IS NULL
    """
    params: list = []
    if tienda_id:
        sql += " AND pt.tienda_id=%s"
        params.append(tienda_id)
    if marca:
        sql += " AND p.marca=%s"
        params.append(marca)
    if categoria:
        sql += " AND p.categoria=%s"
        params.append(categoria)
    if q_base and q_base.strip():
        like = f"%{q_base.strip()}%"
        sql += """
          AND (
                p.nombre LIKE %s
             OR pt.nombre_tienda LIKE %s
             OR pt.sku_tienda LIKE %s
             OR p.marca LIKE %s
          )
        """
        params.extend([like, like, like, like])
    sql += " ORDER BY t.nombre, pt.nombre_tienda LIMIT %s"
    params.append(int(limit))
    return run_select_df(sql, tuple(params))

@st.cache_data(ttl=60, show_spinner=False)
def get_resumen_productos_sin_ean() -> pd.DataFrame:
    sql = """
    SELECT
      p.id AS producto_id,
      p.nombre,
      p.marca,
      p.categoria,
      COUNT(pt.id) AS skus_asociados
    FROM productos p
    LEFT JOIN producto_tienda pt ON pt.producto_id = p.id
    WHERE p.ean IS NULL
    GROUP BY p.id, p.nombre, p.marca, p.categoria
    HAVING skus_asociados > 0
    ORDER BY skus_asociados DESC, p.marca, p.nombre
    """
    return run_select_df(sql)

@st.cache_data(ttl=60, show_spinner=False)
def buscar_destinos_con_ean(q: str, limit: int = 200) -> pd.DataFrame:
    q_like = f"%{q}%"
    sql = f"""
    SELECT
      p.id   AS destino_producto_id,
      p.ean,
      p.nombre,
      p.marca,
      p.categoria,
      p.subcategoria,
      COUNT(pt.id) AS vinculaciones
    FROM productos p
    LEFT JOIN producto_tienda pt ON pt.producto_id = p.id
    WHERE p.ean IS NOT NULL
      AND (
            p.ean = %s
         OR p.nombre LIKE %s
         OR p.marca  LIKE %s
         OR p.categoria LIKE %s
      )
    GROUP BY p.id, p.ean, p.nombre, p.marca, p.categoria, p.subcategoria
    ORDER BY
      CASE WHEN p.ean = %s THEN 0 ELSE 1 END,
      vinculaciones DESC, p.nombre
    LIMIT {int(limit)}
    """
    return run_select_df(sql, (q, q_like, q_like, q_like, q))

@st.cache_data(ttl=120, show_spinner=False)
def pool_destinos_con_ean_acotado(marcas: List[str], categorias: List[str], limit: int = 40000) -> pd.DataFrame:
    sql = """
    SELECT
      p.id   AS destino_producto_id,
      p.ean,
      p.nombre,
      p.marca,
      p.categoria,
      p.subcategoria
    FROM productos p
    WHERE p.ean IS NOT NULL
    """
    params: list = []
    conds = []
    if marcas:
        conds.append("p.marca IN (" + ",".join(["%s"] * len(marcas)) + ")")
        params.extend(marcas)
    if categorias:
        conds.append("p.categoria IN (" + ",".join(["%s"] * len(categorias)) + ")")
        params.extend(categorias)
    if conds:
        sql += " AND (" + " OR ".join(conds) + ")"
    sql += " LIMIT %s"
    params.append(int(limit))
    return run_select_df(sql, tuple(params))

def reassign_producto_ids(pt_ids: List[int], new_producto_id: int) -> int:
    if not pt_ids:
        return 0
    placeholders = ",".join(["%s"] * len(pt_ids))
    sql = f"UPDATE producto_tienda SET producto_id=%s WHERE id IN ({placeholders})"
    params = tuple([new_producto_id] + pt_ids)
    conn = get_conn()
    conn.start_transaction()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        updated = cur.rowcount
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def delete_orphan_products(product_ids: List[int]) -> int:
    if not product_ids:
        return 0
    placeholders = ",".join(["%s"] * len(product_ids))
    sql = f"""
    DELETE FROM productos
    WHERE id IN ({placeholders})
      AND ean IS NULL
      AND NOT EXISTS (SELECT 1 FROM producto_tienda pt WHERE pt.producto_id = productos.id)
    """
    conn = get_conn()
    conn.start_transaction()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(product_ids))
        deleted = cur.rowcount
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ==========================
# UI
# ==========================
st.title("🔗 Vincular SKUs a productos con EAN")
st.caption("Incluye sugerencias con ranker híbrido (TF-IDF + Fuzzy + Marca/Tamaño).")

with st.sidebar:
    st.header("Filtros")
    tiendas_df, marcas_df, categorias_df = get_filtros_base()

    tienda_opt = st.selectbox(
        "Tienda",
        options=[("","— Todas —")] + [(int(r.id), r.nombre) for _, r in tiendas_df.iterrows()],
        format_func=lambda x: x[1] if isinstance(x, tuple) else x
    )
    tienda_id = tienda_opt[0] if isinstance(tienda_opt, tuple) and tienda_opt[0] != "" else None

    marca = st.selectbox("Marca", options=[""] + list(marcas_df["marca"].astype(str)), index=0) or None
    categoria = st.selectbox("Categoría", options=[""] + list(categorias_df["categoria"].astype(str)), index=0) or None

    q_base = st.text_input("Buscar en base (nombre/SKU/marca)", placeholder="Ej: 'aceite', '7790...', o parte del nombre")
    base_limit = st.slider("Máx. filas base", 200, 10000, 2000, step=200)

    st.markdown("---")
    st.subheader("Destino (producto con EAN)")
    q = st.text_input("Buscar destino por EAN, nombre o marca", placeholder="Ej: 7791234567890 o 'aceite natura'")
    limit = st.slider("Máx. resultados destino", 50, 1000, 200, step=50)

tab1, tab2, tab3 = st.tabs(["Vincular por SKU", "Resumen productos sin EAN", "Sugerencias (IA)"])

# --------------------------
# Tab 1: Vincular por SKU
# --------------------------
with tab1:
    st.subheader("1) Selecciona SKUs cuyo producto no tiene EAN")
    pt_df = get_pt_sin_ean(tienda_id, marca, categoria, q_base, limit=base_limit)

    if pt_df.empty:
        st.info("No hay SKUs pendientes con producto sin EAN según los filtros/búsqueda.")
    else:
        show_cols = ["pt_id", "tienda", "sku_tienda", "nombre_tienda", "marca", "categoria", "producto_id", "producto"]
        for c in show_cols:
            if c not in pt_df.columns:
                pt_df[c] = None
        st.dataframe(pt_df[show_cols], use_container_width=True, hide_index=True)

        selected_ids = st.multiselect("Selecciona uno o varios pt_id a vincular", options=list(pt_df["pt_id"]))
        orig_prod_ids = sorted(set(pt_df.loc[pt_df["pt_id"].isin(selected_ids), "producto_id"].tolist())) if selected_ids else []

        st.subheader("2) Elige el producto destino (con EAN)")
        destinos_df = buscar_destinos_con_ean(q.strip(), limit=limit) if q.strip() else pd.DataFrame()
        if not q.strip():
            st.info("Escribe arriba un término de búsqueda para listar posibles destinos con EAN.")
        elif destinos_df.empty:
            st.warning("No se encontraron productos con EAN para esa búsqueda.")
        else:
            st.dataframe(destinos_df, use_container_width=True, hide_index=True)
            destino_id = st.number_input("ID del producto destino (destino_producto_id)", min_value=1, step=1)

            colA, colB = st.columns(2)
            with colA:
                do_cleanup = st.checkbox("Eliminar productos huérfanos sin EAN después de vincular", value=True,
                                         help="Solo eliminará productos sin EAN que queden sin ningún SKU asociado.")
            with colB:
                st.caption("Recomendado: mantener el catálogo limpio y sin duplicados.")

            btn = st.button("🔗 Vincular seleccionados → producto destino", type="primary",
                            disabled=(not selected_ids or not destino_id))

            if btn:
                try:
                    updated = reassign_producto_ids(selected_ids, int(destino_id))
                    st.success(f"Vinculación completada: {updated} SKU(s) → producto_id={int(destino_id)}")
                    if do_cleanup and orig_prod_ids:
                        removed = delete_orphan_products(orig_prod_ids)
                        st.info(f"Productos huérfanos eliminados: {removed}")
                    get_pt_sin_ean.clear(); get_resumen_productos_sin_ean.clear()
                    st.rerun()
                except MySQLError as e:
                    st.error(f"Error MySQL: {e}")
                except Exception as e:
                    st.error(f"Ocurrió un error: {e}")

# --------------------------
# Tab 2: Resumen
# --------------------------
with tab2:
    st.subheader("Productos sin EAN (agrupados)")
    grp = get_resumen_productos_sin_ean()
    if grp.empty:
        st.info("No hay productos sin EAN con SKUs asociados.")
    else:
        st.dataframe(grp, use_container_width=True, hide_index=True)

# --------------------------
# Tab 3: Sugerencias (IA) mejoradas
# --------------------------
with tab3:
    st.subheader("Sugerencias automáticas (ranker híbrido)")
    pt_df = get_pt_sin_ean(tienda_id, marca, categoria, q_base, limit=base_limit)
    if pt_df.empty:
        st.info("No hay SKUs sin EAN para sugerir.")
    else:
        # Controles de precisión
        colf1, colf2, colf3, colf4 = st.columns(4)
        with colf1:
            n_base = st.slider("Máx. SKUs base", 20, 1000, min(200, len(pt_df)), step=20)
        with colf2:
            topk = st.slider("Top-k por SKU", 1, 10, 5, step=1)
        with colf3:
            same_brand_only = st.checkbox("Sólo misma marca", value=False)
        with colf4:
            threshold = st.slider("Umbral mínimo (0–1)", 0.0, 1.0, 0.55, step=0.05)

        base_sample = pt_df.head(n_base).copy()
        # Pool por marca/categoría si se filtra; sino, traer amplio
        marcas_pool = [marca] if marca else []
        categorias_pool = [categoria] if categoria else []
        pool_df = pool_destinos_con_ean_acotado(marcas_pool, categorias_pool, limit=40000)
        if pool_df.empty:
            st.warning("Pool de productos con EAN vacío. Verifica que existan productos con EAN.")
        else:
            # Prepara campos comparables
            base_sample["cmp"] = base_sample.apply(lambda r: combo_string(r["producto"] or r["nombre_tienda"], r["marca"]), axis=1)
            pool_df = pool_df.copy()
            pool_df["cmp"] = pool_df.apply(lambda r: combo_string(r["nombre"], r["marca"]), axis=1)

            # TF-IDF de n-gramas de caracteres (3–5) → muy robusto a variaciones
            vect = TfidfVectorizer(analyzer="char_wb", ngram_range=(3,5), min_df=1)
            X_pool = vect.fit_transform(pool_df["cmp"])
            X_base = vect.transform(base_sample["cmp"])
            cos = cosine_similarity(X_base, X_pool)  # (n_base, n_pool)

            # Precalcular sizes y marcas
            base_sizes = [parse_size_pack((r["producto"] or r["nombre_tienda"]) or "") for _, r in base_sample.iterrows()]
            pool_sizes = [parse_size_pack(nm) for nm in pool_df["nombre"].fillna("").tolist()]

            # Ranking por fila
            suggestions = []
            pool_cmp = pool_df["cmp"].tolist()
            for i, (_, brow) in enumerate(base_sample.iterrows()):
                # opción de filtrar pool por misma marca
                if same_brand_only and brow["marca"]:
                    mask = pool_df["marca"].fillna("").str.lower().str.strip() == str(brow["marca"]).lower().strip()
                    idxs = np.where(mask.values)[0]
                    if len(idxs) == 0:
                        continue
                    cos_row = cos[i, idxs]
                    cand_idx = idxs
                else:
                    cos_row = cos[i, :]
                    cand_idx = np.arange(cos.shape[1])

                # Fuzzy rápida sobre Top-N por cosine preliminar
                prelim = np.argpartition(-cos_row, kth=min(len(cand_idx)-1, topk*10))[:topk*10]
                for j_local in prelim:
                    j = cand_idx[j_local]
                    drow = pool_df.iloc[j]
                    # fuzzy
                    fz = fuzz.token_set_ratio(base_sample.iloc[i]["cmp"], pool_cmp[j]) / 100.0
                    # bonuses
                    bb = brand_bonus(str(brow.get("marca","")), str(drow.get("marca","")))
                    sb = size_bonus(base_sizes[i], pool_sizes[j])
                    score = 0.60*float(cos_row[j_local]) + 0.25*float(fz) + bb + sb
                    score = max(0.0, min(1.0, score))
                    if score < threshold:
                        continue
                    suggestions.append({
                        "score": round(score, 3),
                        "pt_id": int(brow["pt_id"]),
                        "sku_tienda": brow.get("sku_tienda"),
                        "nombre_tienda": brow.get("nombre_tienda"),
                        "producto_base": brow.get("producto"),
                        "marca_base": brow.get("marca"),
                        "destino_producto_id": int(drow["destino_producto_id"]),
                        "ean_destino": drow.get("ean"),
                        "nombre_destino": drow.get("nombre"),
                        "marca_destino": drow.get("marca"),
                        "categoria_destino": drow.get("categoria"),
                    })

            if not suggestions:
                st.info("No hay sugerencias que superen el umbral. Afloja el ‘Umbral mínimo’ o desactiva ‘Sólo misma marca’.")
            else:
                sug_df = pd.DataFrame(suggestions).sort_values(["pt_id","score"], ascending=[True, False])
                # top-k finales por pt_id
                sug_df["rank"] = sug_df.groupby("pt_id")["score"].rank(method="first", ascending=False)
                sug_df = sug_df[sug_df["rank"] <= topk].drop(columns=["rank"])

                st.dataframe(sug_df, use_container_width=True, hide_index=True)

                st.markdown("### Aplicar sugerencia (selección manual)")
                c1, c2 = st.columns(2)
                with c1:
                    sug_pt_id = st.number_input("pt_id", min_value=1, step=1)
                with c2:
                    sug_destino_id = st.number_input("destino_producto_id", min_value=1, step=1)
                do_cleanup2 = st.checkbox("Eliminar productos huérfanos sin EAN", value=True)
                if st.button("✅ Vincular esta sugerencia"):
                    try:
                        # producto original para limpieza
                        orig_df = pt_df.loc[pt_df["pt_id"] == int(sug_pt_id), ["producto_id"]]
                        orig_ids = [int(orig_df.iloc[0]["producto_id"])] if not orig_df.empty else []
                        updated = reassign_producto_ids([int(sug_pt_id)], int(sug_destino_id))
                        st.success(f"Vinculado pt_id={int(sug_pt_id)} → producto_id={int(sug_destino_id)} ({updated} fila/s)")
                        if do_cleanup2 and orig_ids:
                            removed = delete_orphan_products(orig_ids)
                            st.info(f"Productos huérfanos eliminados: {removed}")
                        get_pt_sin_ean.clear(); get_resumen_productos_sin_ean.clear(); pool_destinos_con_ean_acotado.clear()
                        st.rerun()
                    except MySQLError as e:
                        st.error(f"Error MySQL: {e}")
                    except Exception as e:
                        st.error(f"Ocurrió un error: {e}")
