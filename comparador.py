from io import BytesIO

import numpy as np
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Comparador de bases", layout="wide")

if "instituciones" not in st.session_state:
    st.session_state.instituciones = []
if "datasets_institucion" not in st.session_state:
    st.session_state.datasets_institucion = []
if "recursos_dataset" not in st.session_state:
    st.session_state.recursos_dataset = {}
if "df_ckan" not in st.session_state:
    st.session_state.df_ckan = None
if "df_local" not in st.session_state:
    st.session_state.df_local = None


def obtener_instituciones(base_url):
    """Obtiene la lista de instituciones activas del CKAN."""
    try:
        url = f"{base_url}/api/3/action/organization_list"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json().get("result", [])
    except Exception as e:
        st.warning(f"No se pudieron obtener instituciones: {e}")
        return []


def obtener_datasets_institucion(base_url, org_id):
    """Obtiene datasets de una institución."""
    try:
        url = (
            f"{base_url}/api/3/action/package_search?fq=organization:{org_id}&rows=1000"
        )
        # url = f"{base_url}/api/3/action/organization_show?id={org_id}&include_datasets=True"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("packages", [])
    except Exception as e:
        st.warning(f"No se pudieron obtener datasets: {e}")
        return []


def obtener_recursos_dataset(base_url, dataset_id):
    """Obtiene recursos de un dataset específico."""
    try:
        url = f"{base_url}/api/3/action/package_show?id={dataset_id}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json().get("result", {}).get("resources", [])
    except Exception as e:
        st.warning(f"No se pudieron obtener recursos: {e}")
        return []


def obtener_tamano_legible(size):
    """Convierte bytes a MB o indica tamaño desconocido."""
    if size and str(size).isdigit():
        mb = int(size) / (1024 * 1024)
        return f"{mb:.2f} MB"
    return "Tamaño desconocido"


def obtener_tamano_recurso_ckan(resource_id, base_url):
    """Devuelve el tamaño en bytes del recurso CKAN."""
    try:
        url = f"{base_url}/api/3/action/resource_show?id={resource_id}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        size = data.get("result", {}).get("size")
        if size:
            return int(size)
        else:
            # Si CKAN no da size, intentamos HEAD
            recurso_url = data.get("result", {}).get("url")
            if recurso_url:
                r2 = requests.head(recurso_url, allow_redirects=True, timeout=10)
                return int(r2.headers.get("Content-Length", 0))
    except Exception as e:
        st.warning(f"No se pudo obtener el tamaño del recurso: {e}")
    return None


def descargar_recurso_ckan(resource_id, base_url):
    """Descarga el recurso CKAN y devuelve un DataFrame."""
    api_url = f"{base_url}/api/3/action/resource_show?id={resource_id}"
    try:
        r = requests.get(api_url, timeout=10)
        r.raise_for_status()
        data = r.json()
        url_csv = data["result"]["url"]
        st.info(f"Descargando recurso desde {url_csv} ...")
        resp = requests.get(url_csv, timeout=60)
        resp.raise_for_status()
        return pd.read_csv(BytesIO(resp.content))
    except Exception as e:
        st.error(f"Error al descargar o leer el recurso CKAN: {e}")
        return None


def leer_csv_local(archivo):
    try:
        return pd.read_csv(archivo)
    except Exception as e:
        st.error(f"No se pudo leer el archivo: {e}")
        return None


def resumen_general(df):
    data = {
        "Filas": [len(df)],
        "Columnas": [len(df.columns)],
        "Nulos totales": [int(df.isna().sum().sum())],
    }
    return pd.DataFrame(data)


def resumen_por_tipo(df):
    """Devuelve tres DataFrames: numéricas, texto repetido, texto no repetido."""
    resumen_numericas = []
    resumen_texto_repetido = []
    resumen_texto_unico = []

    for col in df.columns:
        serie = df[col]
        nulos = serie.isna().sum()
        no_nulos = len(serie) - nulos

        if pd.api.types.is_numeric_dtype(serie):
            resumen_numericas.append(
                {
                    "Columna": col,
                    "count": int(serie.count()),
                    "mean": float(serie.mean()) if not serie.empty else np.nan,
                    "std": float(serie.std()) if not serie.empty else np.nan,
                    "min": float(serie.min()) if not serie.empty else np.nan,
                    "median": float(serie.median()) if not serie.empty else np.nan,
                    "max": float(serie.max()) if not serie.empty else np.nan,
                }
            )

        elif pd.api.types.is_string_dtype(serie):
            valores_unicos = serie.dropna().unique()
            n_unicos = len(valores_unicos)
            n_total = len(serie)

            if n_unicos < (n_total * 0.8):  # criterio: valores repetidos
                top_values = serie.value_counts().head(5)
                top_str = ", ".join(
                    [f"{v} ({c})" for v, c in zip(top_values.index, top_values.values)]
                )
                resumen_texto_repetido.append(
                    {
                        "Columna": col,
                        "Categorías únicas": n_unicos,
                        "Top 5 valores": top_str,
                    }
                )
            else:
                resumen_texto_unico.append(
                    {
                        "Columna": col,
                        "Valores no nulos": no_nulos,
                        "Nulos": nulos,
                        "Valores únicos": n_unicos,
                    }
                )

    df_num = pd.DataFrame(resumen_numericas)
    df_text_rep = pd.DataFrame(resumen_texto_repetido)
    df_text_uni = pd.DataFrame(resumen_texto_unico)

    return df_num, df_text_rep, df_text_uni


base_url = "https://www.datos.gob.mx/"

st.title("Comparador de bases de datos")

st.caption("Selecciona un recurso en la PNDA")

st.subheader("Paso 1: Cargar archivo CSV local")
archivo_local = st.file_uploader("Subir archivo CSV local", type=["csv"])
if archivo_local:
    st.session_state.df_local = leer_csv_local(archivo_local)
    if st.session_state.df_local is not None:
        st.success("Archivo local cargado correctamente.")

if st.session_state.df_local is not None:
    st.divider()
    st.subheader("Paso 2: Seleccionar recurso en la PNDA")

    if not st.session_state.instituciones:
        with st.spinner("Cargando instituciones activas..."):
            st.session_state.instituciones = obtener_instituciones(base_url)

    institucion_sel = st.selectbox(
        "Selecciona una institución",
        options=["-- Selecciona --"] + st.session_state.instituciones,
        index=0,
    )

    if institucion_sel != "-- Selecciona --":
        if (
            not st.session_state.datasets_institucion
            or st.session_state.datasets_institucion[0]
            .get("organization", {})
            .get("name")
            != institucion_sel
        ):
            with st.spinner("Cargando conjuntos de la institución..."):
                st.session_state.datasets_institucion = obtener_datasets_institucion(
                    base_url, institucion_sel
                )

        for dataset in st.session_state.datasets_institucion:
            with st.expander(f"📦 {dataset.get('title', dataset.get('name'))}"):
                dataset_id = dataset.get("id")
                if dataset_id not in st.session_state.recursos_dataset:
                    with st.spinner("Cargando recursos del conjunto..."):
                        st.session_state.recursos_dataset[dataset_id] = (
                            obtener_recursos_dataset(base_url, dataset_id)
                        )

                recursos = st.session_state.recursos_dataset[dataset_id]
                if not recursos:
                    st.write("No hay recursos disponibles.")
                else:
                    for recurso in recursos:
                        tamano = recurso.get("size")
                        if tamano:
                            tamano_mb = int(tamano) / (1024 * 1024)
                            tamano_texto = f"{tamano_mb:.2f} MB"
                        else:
                            tamano_texto = "Tamaño desconocido"

                        cols = st.columns([3, 1])
                        with cols[0]:
                            st.write(f"**{recurso.get('name', 'Recurso sin nombre')}**")
                            st.caption(
                                f"{recurso.get('format', '').upper()} — {tamano_texto}"
                            )
                        with cols[1]:
                            if tamano and tamano_mb > 200:
                                st.warning(
                                    f"Archivo demasiado grande ({tamano_mb:.2f} MB). "
                                    f"[Abrir en la plataforma]({recurso.get('url', '#')})"
                                )
                            else:
                                if st.button("Cargar desde PNDA", key=recurso["id"]):
                                    df = descargar_recurso_ckan(recurso["id"], base_url)
                                    if df is not None:
                                        st.session_state.df_ckan = df
                                        st.success(
                                            f"Recurso '{recurso.get('name')}' cargado correctamente."
                                        )


if (st.session_state.df_ckan is not None) and (st.session_state.df_local is not None):
    st.divider()
    st.subheader("Paso 3: Comparativa")

    col1, col2 = st.columns(2)

    for nombre, df in [
        ("PNDA", st.session_state.df_ckan),
        ("Local", st.session_state.df_local),
    ]:
        with col1 if nombre == "PNDA" else col2:
            st.subheader(f"Vista previa — {nombre}")
            st.dataframe(df)
            st.markdown("**Resumen general**")
            st.table(resumen_general(df))

            st.markdown("**Análisis por tipo de columna**")
            df_num, df_text_rep, df_text_uni = resumen_por_tipo(df)

            if not df_num.empty:
                st.markdown("🔢 **Columnas numéricas**")
                st.dataframe(df_num)
            if not df_text_rep.empty:
                st.markdown(
                    "🔤 **Columnas de texto con valores repetidos (categóricas)**"
                )
                st.dataframe(df_text_rep)
            if not df_text_uni.empty:
                st.markdown("🧩 **Columnas de texto sin valores repetidos**")
                st.dataframe(df_text_uni)
