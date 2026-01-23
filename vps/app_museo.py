# app.py
import os
import re
import gc
import heapq
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import pandas as pd
import streamlit as st

# Optional deps (recommended). The app will still run with reduced features if missing.
try:
    import plotly.express as px
    import plotly.graph_objects as go
except Exception:  # pragma: no cover
    px = None
    go = None

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except Exception:  # pragma: no cover
    TfidfVectorizer = None
    cosine_similarity = None

# Optional deps (ML visión: CLIP para "buscar objetos" en imágenes)
from io import BytesIO

try:
    import torch
    from PIL import Image
    import requests
    from transformers import CLIPModel, CLIPProcessor
except Exception:  # pragma: no cover
    torch = None
    Image = None
    requests = None
    CLIPModel = None
    CLIPProcessor = None


# -----------------------------
# Config (SIN st.secrets)
# -----------------------------
st.set_page_config(
    page_title="MuseoLab · Humanidades Digitales",
    page_icon="🏛️",
    layout="wide",
)

# >>>> CONEXION AQUI MISMO (edita si aplica) <<<<
MYSQL_HOST = "khushiconfecciones.com"
MYSQL_PORT = 3306
MYSQL_USER = "u506324710_artcom"
# Usa variable de entorno para password (recomendado)
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "PEGA_AQUI_TU_PASSWORD")
MYSQL_DATABASE = "u506324710_museos"
MYSQL_POOL_SIZE = 5


# -----------------------------
# DB connection
# -----------------------------
@dataclass
class MySQLCfg:
    host: str
    port: int
    user: str
    password: str
    database: str


def _load_cfg() -> MySQLCfg:
    # Si quieres, puedes permitir override por env vars (opcional)
    return MySQLCfg(
        host=os.getenv("MYSQL_HOST", MYSQL_HOST),
        port=int(os.getenv("MYSQL_PORT", str(MYSQL_PORT))),
        user=os.getenv("MYSQL_USER", MYSQL_USER),
        password=os.getenv("MYSQL_PASSWORD", MYSQL_PASSWORD),
        database=os.getenv("MYSQL_DATABASE", MYSQL_DATABASE),
    )


@st.cache_resource(show_spinner=False)
def _get_pool():
    # mysql-connector-python pool
    try:
        from mysql.connector import pooling
    except Exception as e:
        raise RuntimeError("Falta dependencia MySQL. Instala: pip install mysql-connector-python") from e

    cfg = _load_cfg()
    pool = pooling.MySQLConnectionPool(
        pool_name="museolab_pool",
        pool_size=int(os.getenv("MYSQL_POOL_SIZE", str(MYSQL_POOL_SIZE))),
        pool_reset_session=True,
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=True,
    )
    return pool


def run_query(sql: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
    pool = _get_pool()
    conn = pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def safe_int(x: Any) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=300)
def get_year_bounds() -> Tuple[int, int]:
    """Obtiene límites de años del dataset (cacheado)."""
    df = run_query("SELECT MIN(year_start) AS mn, MAX(year_end) AS mx FROM obras_arte")
    mn = safe_int(df.iloc[0].get("mn")) if not df.empty else None
    mx = safe_int(df.iloc[0].get("mx")) if not df.empty else None
    mn = mn or 1200
    mx = mx or 2026
    if mn > mx:
        mn, mx = mx, mn
    return int(mn), int(mx)


# -----------------------------
# Helpers
# -----------------------------
ROMAN = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}


def roman_to_int(s: str) -> Optional[int]:
    s = (s or "").upper().strip()
    if not s or any(ch not in ROMAN for ch in s):
        return None
    total, prev = 0, 0
    for ch in reversed(s):
        val = ROMAN[ch]
        if val < prev:
            total -= val
        else:
            total += val
        prev = val
    return total


def infer_year_range(date_text: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Heurística simple para inferir año_inicio/año_fin desde date_text.
    Devuelve (year_start, year_end, note)
    """
    if not date_text:
        return None, None, None
    t = date_text.strip().lower()

    # 1889-1890 / 1889–1890
    m = re.search(r"(1\d{3}|20\d{2})\s*[\-–—]\s*(1\d{3}|20\d{2})", t)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return min(a, b), max(a, b), "rango"

    # c. 1889 / ca 1889 / circa 1889
    m = re.search(r"(?:c\.|ca\.?|circa)\s*(1\d{3}|20\d{2})", t)
    if m:
        y = int(m.group(1))
        return y, y, "circa"

    # 1889
    m = re.search(r"\b(1\d{3}|20\d{2})\b", t)
    if m:
        y = int(m.group(1))
        return y, y, "año"

    # siglo XVII / s. XVII / siglo xvi
    m = re.search(r"(?:siglo|s\.)\s*([ivxlcdm]+)", t)
    if m:
        c = roman_to_int(m.group(1))
        if c:
            start = (c - 1) * 100
            end = start + 99
            return start, end, f"siglo {c}"

    return None, None, "sin_parse"


def clean_artist(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\.,;:()\[\]{}'\"“”‘’]", "", s)
    s = (
        s.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    # normalizaciones puntuales (ajusta a tu caso)
    s = s.replace("van gogh", "vangogh")
    return s


# -----------------------------
# ML Visión: búsqueda visual por texto (CLIP)
# -----------------------------
@st.cache_resource(show_spinner=False)
def _get_clip_bundle():
    if CLIPModel is None or CLIPProcessor is None or torch is None:
        raise RuntimeError("Faltan dependencias de ML/visión (torch/transformers/PIL/requests).")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model_name = os.getenv("CLIP_MODEL_NAME", "openai/clip-vit-base-patch32")
    model = CLIPModel.from_pretrained(model_name)
    processor = CLIPProcessor.from_pretrained(model_name)
    model.to(device)
    model.eval()
    return device, model, processor, model_name


# SIN cache: evita llenar memoria cuando hay miles de imágenes
def _download_image_bytes(url: str) -> Optional[bytes]:
    if not url or requests is None:
        return None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        r = requests.get(url, headers=headers, timeout=12, stream=True)
        r.raise_for_status()
        content = r.content
        if len(content) > 12_000_000:  # evita imágenes gigantes
            return None
        return content
    except Exception:
        return None


def _bytes_to_pil(b: bytes):
    if Image is None or not b:
        return None
    try:
        return Image.open(BytesIO(b)).convert("RGB")
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=120)
def get_image_candidates(selected_sources: Tuple[str, ...], ys: int, ye: int, limit_imgs: int) -> pd.DataFrame:
    where_sql, where_params = build_where(selected_sources, ys, ye)

    # Intento 1: si existe image_full_url
    sql1 = f"""
    SELECT id, source, title, artist, year_start, year_end, technique,
           COALESCE(image_full_url, image_url) AS img,
           source_url
    FROM obras_arte
    {where_sql}
      AND (image_full_url IS NOT NULL OR image_url IS NOT NULL)
    ORDER BY updated_at DESC
    LIMIT %s
    """
    try:
        return run_query(sql1, where_params + (limit_imgs,))
    except Exception:
        # Fallback: solo image_url
        sql2 = f"""
        SELECT id, source, title, artist, year_start, year_end, technique,
               image_url AS img,
               source_url
        FROM obras_arte
        {where_sql}
          AND image_url IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT %s
        """
        return run_query(sql2, where_params + (limit_imgs,))


def clip_text_image_search(
    candidates: pd.DataFrame,
    query_text: str,
    topk: int = 12,
    batch_size: int = 8,  # se mantiene por compatibilidad, pero NO se usa (1x1)
    prompt_mode: str = "Direct",
    prompt_en: Optional[str] = None,
) -> pd.DataFrame:
    """Ranquea imágenes por similitud CLIP con un texto (ej: 'manzana').
    Versión memory-safe: evalúa 1x1 y solo guarda el TopK.
    """
    device, model, processor, model_name = _get_clip_bundle()

    query_text = (query_text or "").strip()
    if not query_text or candidates is None or candidates.empty:
        return pd.DataFrame()

    if prompt_mode == "English template":
        prompt = (prompt_en or "").strip() or f"a photo of {query_text}"
    else:
        prompt = query_text

    # Tokeniza el texto UNA sola vez (ahorra RAM/CPU)
    text_tok = processor(text=[prompt], return_tensors="pt", padding=True)
    text_tok = {k: v.to(device) for k, v in text_tok.items()}

    # Min-heap para guardar solo TopK: (score, meta_dict)
    heap: List[Tuple[float, dict]] = []
    n_seen = 0

    for _, r in candidates.iterrows():
        n_seen += 1

        url = r.get("img")
        if not url:
            continue

        b = _download_image_bytes(str(url))
        if not b:
            continue

        img = _bytes_to_pil(b)
        if img is None:
            continue

        # Reduce tamaño en memoria (opcional)
        try:
            img.thumbnail((1024, 1024))
        except Exception:
            pass

        try:
            # Procesa UNA imagen
            img_inputs = processor(images=img, return_tensors="pt")
            img_inputs = {k: v.to(device) for k, v in img_inputs.items()}

            inputs = {}
            inputs.update(text_tok)      # input_ids, attention_mask
            inputs.update(img_inputs)    # pixel_values

            with torch.inference_mode():
                logits = model(**inputs).logits_per_image  # (1,1)
                score = float(logits[0, 0].detach().cpu().item())

            meta = {k: r.get(k) for k in candidates.columns}
            meta["clip_score"] = score
            meta["clip_prompt"] = prompt
            meta["clip_model"] = model_name

            if len(heap) < int(topk):
                heapq.heappush(heap, (score, meta))
            else:
                if score > heap[0][0]:
                    heapq.heapreplace(heap, (score, meta))

        except Exception:
            pass
        finally:
            # Libera memoria SIEMPRE
            try:
                img.close()
            except Exception:
                pass

            # elimina referencias grandes
            try:
                del b, img
            except Exception:
                pass
            try:
                del img_inputs, inputs, logits
            except Exception:
                pass

            # limpieza periódica
            if n_seen % 25 == 0:
                gc.collect()
                if torch is not None and device == "cuda":
                    try:
                        torch.cuda.empty_cache()
                    except Exception:
                        pass

    if not heap:
        return pd.DataFrame()

    # Ordena por score desc
    best = sorted(heap, key=lambda x: x[0], reverse=True)
    out_rows = [m for _, m in best]
    return pd.DataFrame(out_rows)


# -----------------------------
# UI: Sidebar Filters
# -----------------------------
st.title("🏛️ MuseoLab · Ciencia de Datos + Humanidades Digitales")
st.caption(
    "Explora tu tabla `obras_arte` con búsqueda full-text, facetas, cronologías, redes y similitud semántica (TF-IDF)."
)

with st.sidebar:
    st.header("Filtros")

    # Sources
    try:
        src_df = run_query("SELECT DISTINCT source FROM obras_arte ORDER BY source")
        all_sources = src_df["source"].dropna().tolist() if not src_df.empty else []
    except Exception:
        all_sources = []
        st.warning("No pude leer fuentes (columna `source`). Revisa conexión / schema.")

    sources = st.multiselect(
        "Museo / Fuente",
        options=all_sources,
        default=all_sources[:1] if all_sources else [],
    )
    sources_t = tuple(sources)  # hashable para cache

    q = st.text_input(
        "Búsqueda (Google-ish)",
        placeholder="ej: 'vangogh' técnica óleo dormitorio",
        help="Usa FULLTEXT en (title, artist, description, technique, info_extra).",
    )
    ft_mode = st.selectbox(
        "Modo de búsqueda",
        ["Natural", "Boolean"],
        index=0,
        help="Boolean permite + - * y comillas en MySQL.",
    )

    yr_min, yr_max = get_year_bounds()
    year_range = st.slider(
        "Rango de años",
        min_value=int(yr_min),
        max_value=int(yr_max),
        value=(int(yr_min), int(yr_max)),
    )
    ys, ye = int(year_range[0]), int(year_range[1])

    limit = st.slider("Máx. resultados", min_value=50, max_value=2000, value=500, step=50)

    st.divider()
    st.header("Humanidades Digitales")
    top_n = st.slider("Top N (gráficas)", 5, 100, 25)


# -----------------------------
# Query builder
# -----------------------------
def build_where(selected_sources: Tuple[str, ...], ys: int, ye: int) -> Tuple[str, Tuple[Any, ...]]:
    where = []
    params: List[Any] = []

    if selected_sources:
        where.append("source IN (%s)" % (", ".join(["%s"] * len(selected_sources))))
        params.extend(list(selected_sources))

    # year overlap
    where.append("(COALESCE(year_start, -32768) <= %s AND COALESCE(year_end, 32768) >= %s)")
    params.extend([ye, ys])

    return (" WHERE " + " AND ".join(where)) if where else "", tuple(params)


@st.cache_data(show_spinner=False, ttl=60)
def search_obras(
    q: str,
    ft_mode: str,
    selected_sources: Tuple[str, ...],
    ys: int,
    ye: int,
    limit: int,
) -> pd.DataFrame:
    where_sql, where_params = build_where(selected_sources, ys, ye)

    if q.strip():
        mode_sql = "IN NATURAL LANGUAGE MODE" if ft_mode == "Natural" else "IN BOOLEAN MODE"
        sql = f"""
        SELECT
          id, source, title, artist, date_text, year_start, year_end,
          technique, medium, dimensions,
          inventory_no, collection, room, location,
          image_url, source_url,
          MATCH(title, artist, description, technique, info_extra) AGAINST (%s {mode_sql}) AS score,
          scraped_at, updated_at
        FROM obras_arte
        {where_sql}
          AND MATCH(title, artist, description, technique, info_extra) AGAINST (%s {mode_sql})
        ORDER BY score DESC, updated_at DESC
        LIMIT %s
        """
        params = (q,) + where_params + (q, limit)
    else:
        sql = f"""
        SELECT
          id, source, title, artist, date_text, year_start, year_end,
          technique, medium, dimensions,
          inventory_no, collection, room, location,
          image_url, source_url,
          NULL AS score,
          scraped_at, updated_at
        FROM obras_arte
        {where_sql}
        ORDER BY updated_at DESC
        LIMIT %s
        """
        params = where_params + (limit,)

    return run_query(sql, params)


def get_obras_df() -> pd.DataFrame:
    return search_obras(q, ft_mode, sources_t, ys, ye, limit)


# -----------------------------
# Tabs
# -----------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🔎 Explorador", "📈 Cronología", "🕸️ Redes", "🧠 Similitud", "🧹 Calidad + Exportar"]
)


# -----------------------------
# TAB 1: Explorador
# -----------------------------
with tab1:
    df = get_obras_df()

    colA, colB, colC, colD = st.columns(4)
    with colA:
        st.metric("Obras (muestra)", f"{len(df):,}")
    with colB:
        st.metric("Fuentes", f"{df['source'].nunique() if not df.empty and 'source' in df.columns else 0}")
    with colC:
        st.metric("Artistas (muestra)", f"{df['artist'].nunique() if not df.empty and 'artist' in df.columns else 0}")
    with colD:
        if not df.empty and {"year_start", "year_end"}.issubset(df.columns):
            yrs = df[["year_start", "year_end"]].dropna(how="all")
            if not yrs.empty:
                st.metric("Años cubiertos", f"{int(yrs.min().min())}–{int(yrs.max().max())}")
            else:
                st.metric("Años cubiertos", "—")
        else:
            st.metric("Años cubiertos", "—")

    st.subheader("Resultados")
    show_cols = [
        "id",
        "source",
        "title",
        "artist",
        "year_start",
        "year_end",
        "technique",
        "inventory_no",
        "collection",
        "room",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    st.dataframe(df[show_cols] if show_cols else df, use_container_width=True, height=420)

    st.markdown("### Detalle")
    selected_id = None
    if not df.empty and "id" in df.columns:
        ids = df["id"].astype(int).tolist()[:2000]
        selected_id = st.selectbox("Selecciona una obra por ID", options=ids)

    if selected_id is not None:
        det = run_query("SELECT * FROM obras_arte WHERE id = %s", (int(selected_id),))
        if det.empty:
            st.info("No se encontró la obra.")
        else:
            r = det.iloc[0].to_dict()
            left, right = st.columns([1, 1])
            with left:
                st.markdown(f"#### {r.get('title') or '(sin título)'}")
                st.write(
                    {
                        "artista": r.get("artist"),
                        "fecha": r.get("date_text"),
                        "años": f"{r.get('year_start') or '—'}–{r.get('year_end') or '—'}",
                        "técnica": r.get("technique"),
                        "medium": r.get("medium"),
                        "dimensiones": r.get("dimensions"),
                        "inventario": r.get("inventory_no"),
                        "colección": r.get("collection"),
                        "sala": r.get("room"),
                        "ubicación": r.get("location"),
                        "fuente": r.get("source"),
                    }
                )
                if r.get("source_url"):
                    st.link_button("Abrir ficha fuente", r.get("source_url"))

            with right:
                img = r.get("image_full_url") or r.get("image_url")
                if img:
                    st.image(img, use_container_width=True)
                else:
                    st.info("Sin imagen en esta obra.")

            st.markdown("#### Texto curatorial / descripción")
            st.write(r.get("description") or "(vacío)")

            with st.expander("Ver metadata extendida"):
                st.write(
                    {
                        "info_extra": r.get("info_extra"),
                        "bibliography": r.get("bibliography"),
                        "exhibition_hist": r.get("exhibition_hist"),
                        "scraped_at": str(r.get("scraped_at")),
                        "updated_at": str(r.get("updated_at")),
                    }
                )


# -----------------------------
# TAB 2: Cronología
# -----------------------------
with tab2:
    df = get_obras_df()
    if df.empty:
        st.info("No hay datos para graficar con los filtros actuales.")
    else:
        tmp = df.copy()
        if {"year_start", "year_end"}.issubset(tmp.columns):
            tmp["year_mid"] = tmp[["year_start", "year_end"]].mean(axis=1)
            tmp = tmp.dropna(subset=["year_mid"])
        else:
            tmp = pd.DataFrame()

        st.subheader("Distribución temporal")
        if tmp.empty:
            st.info("No hay años (year_start/year_end) para graficar con los filtros actuales.")
        elif px is None:
            st.warning("Instala plotly para ver gráficas interactivas: pip install plotly")
        else:
            fig = px.histogram(tmp, x="year_mid", nbins=60, hover_data=["source"], title="Obras por año (aprox.)")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top artistas por recuento")
        if "artist" in df.columns:
            top_art = (
                df.assign(artist_clean=df["artist"].fillna("").map(clean_artist))
                .query("artist_clean != ''")
                .groupby("artist_clean", as_index=False)
                .size()
                .sort_values("size", ascending=False)
                .head(top_n)
            )
            st.dataframe(
                top_art.rename(columns={"artist_clean": "artista_norm", "size": "obras"}),
                use_container_width=True
            )
        else:
            st.info("No existe la columna `artist` en el resultado.")


# -----------------------------
# TAB 3: Redes (Sankey)
# -----------------------------
with tab3:
    df = get_obras_df()
    st.subheader("Redes de relaciones (lectura humanística)")
    st.caption("Ejemplos: Artista → Fuente, Artista → Técnica.")

    if go is None:
        st.warning("Instala plotly para visualizar redes (Sankey): pip install plotly")
    elif df.empty:
        st.info("No hay datos para graficar con los filtros actuales.")
    else:
        kind = st.radio("Tipo de red", ["Artista → Fuente", "Artista → Técnica"], horizontal=True)

        tmp = df.copy()
        if "artist" not in tmp.columns:
            st.info("No existe la columna `artist` en el resultado.")
        else:
            tmp["artist_clean"] = tmp["artist"].fillna("").map(clean_artist)
            tmp = tmp.query("artist_clean != ''")

            if kind == "Artista → Fuente":
                tmp["right"] = tmp.get("source", pd.Series(["(sin fuente)"] * len(tmp))).fillna("(sin fuente)")
            else:
                tech = tmp.get("technique", pd.Series(["(sin técnica)"] * len(tmp)))
                tmp["right"] = tech.fillna("(sin técnica)").astype(str).str.lower().str.strip()

            top_left = tmp.groupby("artist_clean").size().sort_values(ascending=False).head(top_n).index.tolist()
            top_right = tmp.groupby("right").size().sort_values(ascending=False).head(top_n).index.tolist()

            g = tmp[tmp["artist_clean"].isin(top_left) & tmp["right"].isin(top_right)]
            if g.empty:
                st.info("No hay suficientes relaciones con el TOP seleccionado.")
            else:
                edges = g.groupby(["artist_clean", "right"]).size().reset_index(name="count")
                nodes = pd.Index(edges["artist_clean"].tolist() + edges["right"].tolist()).unique().tolist()
                node_idx = {n: i for i, n in enumerate(nodes)}

                sankey = go.Figure(
                    data=[
                        go.Sankey(
                            node=dict(label=nodes, pad=12, thickness=14),
                            link=dict(
                                source=[node_idx[a] for a in edges["artist_clean"]],
                                target=[node_idx[b] for b in edges["right"]],
                                value=edges["count"].tolist(),
                            ),
                        )
                    ]
                )
                sankey.update_layout(height=520, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(sankey, use_container_width=True)

            st.markdown("### Lectura rápida")
            st.write(
                "- Si un artista aparece concentrado en una sola **fuente**, puede indicar sesgo de scraping (una web) o una colección dominante.\n"
                "- Si una **técnica** domina por artista, puede ayudarte a separar series, periodos o atribuciones dudosas."
            )


# -----------------------------
# TAB 4: Similitud (TF-IDF) · 'Obras parecidas' + ML Visión (CLIP)
# -----------------------------
with tab4:
    st.subheader("Similitud semántica (TF-IDF) · 'Obras parecidas'")
    st.caption("Vectoriza texto (título + técnica + descripción + extra) y calcula similitud coseno.")

    if TfidfVectorizer is None:
        st.warning("Instala scikit-learn para similitud: pip install scikit-learn")
    else:
        df = get_obras_df()
        if df.empty:
            st.info("No hay datos con los filtros actuales.")
        else:
            max_corpus = st.slider("Tamaño del corpus (más = mejor, pero más pesado)", 200, 5000, 1500, step=100)

            @st.cache_data(show_spinner=True, ttl=120)
            def build_tfidf(selected_sources: Tuple[str, ...], ys: int, ye: int, max_corpus: int):
                where_sql, where_params = build_where(selected_sources, ys, ye)
                sql = f"""
                SELECT id, CONCAT_WS(' ', title, artist, technique, description, info_extra) AS txt
                FROM obras_arte
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT %s
                """
                corpus = run_query(sql, where_params + (max_corpus,))
                corpus["txt"] = corpus.get("txt", pd.Series([""] * len(corpus))).fillna("")

                vec = TfidfVectorizer(
                    max_features=40000,
                    ngram_range=(1, 2),
                    min_df=2,
                    stop_words=None,
                )
                X = vec.fit_transform(corpus["txt"].tolist())
                return corpus[["id"]], vec, X

            ids_df, vec, X = build_tfidf(sources_t, ys, ye, max_corpus)
            id_list = ids_df["id"].astype(int).tolist()

            pick_id = st.selectbox("Selecciona una obra del corpus", options=id_list)
            if pick_id is not None:
                i = id_list.index(int(pick_id))
                sims = cosine_similarity(X[i], X).flatten()

                topk_sim = 10
                idx = sims.argsort()[::-1]
                idx = [j for j in idx if j != i][:topk_sim]

                sim_ids = [int(id_list[j]) for j in idx]
                sim_scores = [float(sims[j]) for j in idx]

                sims_df = run_query(
                    f"""
                    SELECT id, source, title, artist, year_start, year_end, technique, image_url, source_url
                    FROM obras_arte
                    WHERE id IN ({', '.join(['%s'] * len(sim_ids))})
                    """,
                    tuple(sim_ids),
                )

                order = {oid: k for k, oid in enumerate(sim_ids)}
                sims_df["score"] = sims_df["id"].map(lambda x: sim_scores[order[int(x)]])
                sims_df = sims_df.sort_values("score", ascending=False)

                st.markdown("### Top similares")
                st.dataframe(
                    sims_df[["id", "score", "source", "title", "artist", "year_start", "year_end", "technique"]],
                    use_container_width=True,
                    height=360,
                )

                with st.expander("Vista rápida con imágenes"):
                    for _, row in sims_df.head(6).iterrows():
                        c1, c2 = st.columns([1, 3])
                        with c1:
                            if row.get("image_url"):
                                st.image(row.get("image_url"), use_container_width=True)
                        with c2:
                            st.markdown(f"**{row.get('title') or '(sin título)'}**")
                            st.write(
                                {
                                    "id": int(row["id"]),
                                    "artista": row.get("artist"),
                                    "fuente": row.get("source"),
                                    "años": f"{row.get('year_start') or '—'}–{row.get('year_end') or '—'}",
                                    "técnica": row.get("technique"),
                                }
                            )
                            if row.get("source_url"):
                                st.link_button("Abrir fuente", row.get("source_url"))

    st.divider()
    st.subheader("🖼️ Machine Learning (Visión): encontrar objetos en imágenes (CLIP)")
    st.caption("Escribe una palabra (ej: manzana) y el modelo ordena imágenes por probabilidad de que aparezca ese concepto.")

    if CLIPModel is None or torch is None or requests is None:
        st.warning(
            "Para activar este módulo instala dependencias:\n\n"
            "pip install torch transformers pillow requests\n\n"
            "Luego reinicia Streamlit."
        )
    else:
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            vision_query = st.text_input("Palabra / concepto a buscar en la imagen", value="manzana", key="vision_query")
        with c2:
            prompt_mode = st.selectbox("Prompt", ["Direct", "English template"], index=0, key="vision_prompt_mode")
        with c3:
            topk = st.slider("Top resultados", 3, 30, 12, key="vision_topk")

        max_imgs = st.slider("Imágenes a evaluar (más = más lento)", 20, 600, 160, step=20, key="vision_max_imgs")

        prompt_en = None
        if prompt_mode == "English template":
            prompt_en = st.text_input("Prompt en inglés (opcional)", value="", key="vision_prompt_en")

        run_vis = st.button("Buscar en imágenes", key="vision_run")

        if run_vis:
            cand = get_image_candidates(sources_t, ys, ye, int(max_imgs))
            if cand.empty:
                st.info("No hay imágenes disponibles con los filtros actuales.")
            else:
                with st.spinner("Descargando imágenes y calculando similitud (CLIP) 1x1..."):
                    res = clip_text_image_search(
                        cand,
                        vision_query,
                        topk=int(topk),
                        batch_size=1,
                        prompt_mode=prompt_mode,
                        prompt_en=prompt_en,
                    )

                if res.empty:
                    st.info("No pude procesar imágenes (URLs caídas, bloqueadas o sin dependencias).")
                else:
                    show = ["id", "clip_score", "source", "title", "artist", "year_start", "year_end", "technique", "img", "source_url"]
                    show = [c for c in show if c in res.columns]
                    st.dataframe(res[show], use_container_width=True, height=340)

                    with st.expander("Ver top con imágenes"):
                        cols = st.columns(3)
                        for i, (_, row) in enumerate(res.iterrows()):
                            col = cols[i % 3]
                            with col:
                                if row.get("img"):
                                    st.image(row.get("img"), use_container_width=True)
                                st.markdown(f"**{row.get('title') or '(sin título)'}**")
                                st.write(
                                    {
                                        "id": int(row["id"]) if row.get("id") is not None else None,
                                        "score": float(row.get("clip_score", 0.0)),
                                        "artista": row.get("artist"),
                                        "fuente": row.get("source"),
                                    }
                                )
                                if row.get("source_url"):
                                    st.link_button("Abrir fuente", row.get("source_url"))


# -----------------------------
# TAB 5: Calidad + Exportar
# -----------------------------
with tab5:
    st.subheader("Calidad de datos + Exportación")

    df = get_obras_df()
    if df.empty:
        st.info("No hay datos con los filtros actuales.")
    else:
        cols = ["title", "artist", "date_text", "year_start", "year_end", "technique", "description", "image_url"]
        cols = [c for c in cols if c in df.columns]

        if cols:
            miss = (
                df[cols]
                .isna()
                .mean()
                .sort_values(ascending=False)
                .mul(100)
                .round(1)
                .reset_index()
                .rename(columns={"index": "campo", 0: "%_vacío"})
            )
            st.markdown("### % de campos vacíos (muestra)")
            st.dataframe(miss, use_container_width=True)
        else:
            st.info("No están las columnas esperadas para reporte de vacíos.")

        st.markdown("### Inferencia de años (preview)")
        st.caption("Inferencia rápida (sin escribir en DB).")

        sample_n = st.slider("Filas a inferir", 50, 1000, 200, step=50)
        base_cols = [c for c in ["id", "date_text", "year_start", "year_end"] if c in df.columns]
        sample = df[base_cols].head(sample_n).copy() if base_cols else pd.DataFrame()

        if not sample.empty and "date_text" in sample.columns:
            inferred = sample["date_text"].map(lambda t: infer_year_range(t))
            sample["infer_year_start"] = [x[0] for x in inferred]
            sample["infer_year_end"] = [x[1] for x in inferred]
            sample["infer_note"] = [x[2] for x in inferred]
            st.dataframe(sample, use_container_width=True, height=360)
        else:
            st.info("No hay `date_text` para inferir.")

        st.markdown("### Exportar")
        c1, c2 = st.columns(2)
        with c1:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Descargar CSV (muestra filtrada)",
                data=csv,
                file_name="obras_filtradas.csv",
                mime="text/csv",
            )
        with c2:
            jsonl = (df.to_json(orient="records", lines=True, force_ascii=False) + "\n").encode("utf-8")
            st.download_button(
                "Descargar JSONL",
                data=jsonl,
                file_name="obras_filtradas.jsonl",
                mime="application/json",
            )


st.divider()
with st.expander("🔧 Notas técnicas"):
    st.markdown(
        """
- Esta app asume MySQL 8 y tabla `obras_arte`.
- Búsqueda usa `FULLTEXT` con `MATCH ... AGAINST` sobre: (title, artist, description, technique, info_extra).
- Para mantenerlo rápido: limita resultados y cachea consultas (TTL corto).

**Tips de performance**
- Si el corpus crece mucho, evita cargar texto completo en `Similitud`; crea un pipeline offline y guarda embeddings/keywords.
- Para búsquedas por artista con variaciones ("vangogh" vs "v. vang"), considera un campo `artist_canon` (ETL) + índice.
"""
    )
