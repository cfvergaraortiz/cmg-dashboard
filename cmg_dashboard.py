"""
Dashboard CMg Real vs Programado - Coordinador Eléctrico Nacional
=================================================================
Requisitos:
    pip install streamlit plotly pandas requests streamlit-autorefresh

Para correr:
    streamlit run cmg_dashboard.py
"""

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# =============================================================================
# 1. CONFIGURACIÓN
# =============================================================================

st.set_page_config(
    page_title="CMg Real vs Programado",
    page_icon="⚡",
    layout="wide",
)

BASE_URL = "https://sipub.api.coordinador.cl:443"

# El token se lee desde Streamlit Cloud Secrets (o desde .streamlit/secrets.toml localmente)
# En Streamlit Cloud ve a Settings → Secrets y agrega:
#   CEN_TOKEN = "tu_token_aqui"
DEFAULT_TOKEN = st.secrets.get("CEN_TOKEN", "")

# Diccionario de homologación de barras
# Clave: nombre de barra Online (barra_transf)
# Valor: nombre de barra Programado (llave_cmg)
BARRAS = {
    "P.MONTT_______220": "PMontt220",
    "A.JAHUEL______220": "AJahuel220",
    "POLPAICO______220": "Polpaico220",
    "P.AZUCAR______220": "PAzucar220",
    "CARDONES______220": "Cardones220",
    "QUILLOTA______220": "Quillota220",
    "CRUCERO_______220": "Crucero220",
    "CHARRUA_______220": "Charrua220",
}

# Nombre "limpio" para mostrar en la UI
# Quitamos guiones bajos y la tensión para que sea más legible
def nombre_display(key_online: str) -> str:
    nombre = key_online.split("_")[0].replace(".", " ").strip().title()
    tension = key_online.strip("_").split("_")[-1]  # "220"
    return f"{nombre} {tension} kV"

BARRAS_DISPLAY = {nombre_display(k): k for k in BARRAS}


# =============================================================================
# 2. FUNCIONES API
# =============================================================================

def fetch_all_pages(url: str, user_key: str, params: dict, page_size: int = 100) -> list:
    """
    Paginación automática. Auth via user_key como query param.
    Usamos page_size=100 para no sobrecargar la API del CEN.
    """
    all_records = []
    page = 1
    MAX_RETRIES = 3

    while True:
        params_page = {**params, "user_key": user_key, "page": page, "limit": page_size}

        # Reintentos ante errores transitorios del servidor
        for intento in range(MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    params=params_page,
                    headers={"accept": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()
                break  # éxito → salir del loop de reintentos
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else "?"
                if status == 500 and intento < MAX_RETRIES - 1:
                    import time; time.sleep(2)   # esperar antes de reintentar
                    continue
                st.error(f"Error {status} en la API del CEN (página {page}): {e}")
                return all_records
            except requests.exceptions.RequestException as e:
                st.error(f"Error de conexión: {e}")
                return all_records

        try:
            data = response.json()
        except Exception:
            st.error("La API devolvió una respuesta que no es JSON válido.")
            return all_records

        records = data.get("data", [])
        if not records:
            break

        all_records.extend(records)

        if len(records) < page_size:
            break

        page += 1

    return all_records


@st.cache_data(ttl=3600)
def fetch_cmg_online(user_key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    CMg Online — resolución 15 minutos.
    Trae todas las barras del diccionario BARRAS en una sola llamada (sin filtro
    de barra) y luego filtra localmente. Así minimizamos llamadas a la API.
    """
    url = f"{BASE_URL}/costo-marginal-online/v4/findByDate"
    params = {"startDate": start_date, "endDate": end_date}

    records = fetch_all_pages(url, user_key, params)
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Construir datetime desde fecha + hra + min
    df["datetime"] = (
        pd.to_datetime(df["fecha"])
        + pd.to_timedelta(df["hra"].astype(int), unit="h")
        + pd.to_timedelta(df["min"].astype(int), unit="m")
    )

    df = df.rename(columns={
        "barra_info":   "nombre_barra",
        "barra_transf": "barra_online",
        "cmg_usd_mwh_": "cmg_real",
        "cmg_clp_kwh_": "cmg_real_clp",
    })

    # Quedarse solo con las barras del diccionario
    df = df[df["barra_online"].isin(BARRAS.keys())].copy()

    cols = ["datetime", "barra_online", "nombre_barra", "cmg_real", "cmg_real_clp", "version"]
    df = df[[c for c in cols if c in df.columns]]
    return df.sort_values(["barra_online", "datetime"]).reset_index(drop=True)


@st.cache_data(ttl=3600)
def fetch_cmg_programado(user_key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    CMg Programado PID — resolución horaria.
    """
    url = f"{BASE_URL}/cmg-programado-pid/v4/findByDate"
    params = {"startDate": start_date, "endDate": end_date}

    records = fetch_all_pages(url, user_key, params)
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["fecha_hora"])

    df = df.rename(columns={
        "nmb_barra_info": "nombre_barra",
        "llave_cmg":      "barra_prog",
        "cmg_usd_mwh":    "cmg_programado",
    })

    # Quedarse solo con barras del diccionario
    barras_prog = set(BARRAS.values())
    df = df[df["barra_prog"].isin(barras_prog)].copy()

    cols = ["datetime", "barra_prog", "nombre_barra", "cmg_programado",
            "zona", "region", "fecha_programa", "hora_programa"]
    df = df[[c for c in cols if c in df.columns]]
    return df.sort_values(["barra_prog", "datetime"]).reset_index(drop=True)


# =============================================================================
# 3. PROCESAMIENTO
# =============================================================================

def preparar_comparacion(df_real: pd.DataFrame, df_prog: pd.DataFrame,
                          barra_online: str) -> pd.DataFrame:
    """
    Para una barra dada:
      1. Filtra el CMg Online por barra_online
      2. Resamplea de 15 min → horario (promedio)
      3. Filtra el CMg Programado con la clave homologada
      4. Hace merge y calcula diferencias
    """
    barra_programado = BARRAS[barra_online]

    # --- Online: resampleo 15 min → 1 hora ---
    real = (
        df_real[df_real["barra_online"] == barra_online]
        .set_index("datetime")[["cmg_real"]]
        .resample("1h")
        .mean()                   # promedio de los 4 bloques de 15 min
        .reset_index()
    )
    real["datetime"] = real["datetime"].dt.floor("1h")

    # --- Programado: ya es horario ---
    prog = (
        df_prog[df_prog["barra_prog"] == barra_programado][["datetime", "cmg_programado"]]
        .copy()
    )
    prog["datetime"] = prog["datetime"].dt.floor("1h")

    # --- Merge ---
    merged = pd.merge(real, prog, on="datetime", how="inner")
    merged["diferencia"]     = merged["cmg_real"] - merged["cmg_programado"]
    merged["diferencia_pct"] = (
        merged["diferencia"] / merged["cmg_programado"].replace(0, float("nan")) * 100
    )
    return merged


# =============================================================================
# 4. DASHBOARD
# =============================================================================

# Auto-refresh horario (requiere: pip install streamlit-autorefresh)
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=3600 * 1000, key="auto_refresh")
except ImportError:
    pass

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚡ CMg Dashboard")
    st.markdown("---")

    user_key = st.text_input(
        "🔑 User Key (API CEN)",
        value=DEFAULT_TOKEN,
        type="password",
        help="Tu user_key de SIPUB",
    )

    st.markdown("### 📅 Rango de fechas")
    hoy  = datetime.today().date()
    ayer = hoy - timedelta(days=1)
    fecha_inicio = st.date_input("Desde", value=ayer, max_value=hoy)
    fecha_fin    = st.date_input("Hasta", value=hoy,  max_value=hoy)

    if st.button("🔄 Forzar actualización", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    st.caption("Cache 1h | auto-refresh con streamlit-autorefresh")

# ── Main ─────────────────────────────────────────────────────────────────────
st.title("⚡ CMg Real vs Programado — SEN Chile")
st.caption(
    "CMg Online (15 min → promedio horario) vs CMg Programado PID  |  "
    "Fuente: Coordinador Eléctrico Nacional"
)

if not user_key:
    st.warning("👈 Ingresa tu user_key en el panel izquierdo para comenzar.")
    st.stop()

if fecha_inicio > fecha_fin:
    st.error("La fecha de inicio debe ser anterior o igual a la fecha fin.")
    st.stop()

start_str = fecha_inicio.strftime("%Y-%m-%d")
end_str   = fecha_fin.strftime("%Y-%m-%d")

# ── Carga de datos ────────────────────────────────────────────────────────────
with st.spinner("Consultando API del CEN..."):
    df_real = fetch_cmg_online(user_key, start_str, end_str)
    df_prog = fetch_cmg_programado(user_key, start_str, end_str)

if df_real.empty and df_prog.empty:
    st.error("Sin datos. Verifica el user_key o las fechas.")
    st.stop()

# ── Sección 1: Selector de barra ──────────────────────────────────────────────
st.markdown("---")

col_sel, col_info = st.columns([1, 2])

with col_sel:
    st.subheader("🗼 Selecciona una barra")
    nombre_sel  = st.radio("", list(BARRAS_DISPLAY.keys()), label_visibility="collapsed")
    barra_online = BARRAS_DISPLAY[nombre_sel]
    barra_prog   = BARRAS[barra_online]
    st.caption(f"Online: `{barra_online}`")
    st.caption(f"Programado: `{barra_prog}`")

with col_info:
    # Verificar disponibilidad de datos para la barra seleccionada
    tiene_real = not df_real.empty and barra_online in df_real["barra_online"].values
    tiene_prog = not df_prog.empty and barra_prog  in df_prog["barra_prog"].values

    st.subheader("📋 Disponibilidad de datos")
    c1, c2 = st.columns(2)
    c1.metric(
        "CMg Online",
        "✅ Disponible" if tiene_real else "❌ Sin datos",
        delta=f"{df_real[df_real['barra_online']==barra_online].shape[0]} registros (15min)" if tiene_real else None,
    )
    c2.metric(
        "CMg Programado",
        "✅ Disponible" if tiene_prog else "❌ Sin datos",
        delta=f"{df_prog[df_prog['barra_prog']==barra_prog].shape[0]} registros (horario)" if tiene_prog else None,
    )

# ── Sección 2: Comparación ───────────────────────────────────────────────────
st.markdown("---")
st.subheader(f"📊 Análisis — {nombre_sel}")

if not tiene_real or not tiene_prog:
    st.warning("No hay datos suficientes para comparar esta barra en el período seleccionado.")
else:
    df_merged = preparar_comparacion(df_real, df_prog, barra_online)

    if df_merged.empty:
        st.warning("No hay horas comunes entre el CMg Online y el Programado para esta barra.")
    else:
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        diff_media = df_merged["diferencia"].mean()
        mae        = df_merged["diferencia"].abs().mean()
        max_diff   = df_merged["diferencia"].abs().max()
        pct_mayor  = (df_merged["diferencia"] > 0).mean() * 100

        k1.metric("Diferencia media",     f"{diff_media:+.1f} USD/MWh",
                  help="Promedio de (Real − Programado)")
        k2.metric("MAE",                   f"{mae:.1f} USD/MWh",
                  help="Error absoluto medio por hora")
        k3.metric("Máx. desviación abs.", f"{max_diff:.1f} USD/MWh")
        k4.metric("% horas Real > Prog.", f"{pct_mayor:.1f}%")

        # Gráfico serie temporal + diferencia
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.62, 0.38],
            subplot_titles=(
                "CMg Real (prom. horario) vs CMg Programado PID  [USD/MWh]",
                "Diferencia  Real − Programado  [USD/MWh]",
            ),
            vertical_spacing=0.08,
        )

        fig.add_trace(go.Scatter(
            x=df_merged["datetime"], y=df_merged["cmg_real"],
            name="CMg Real", mode="lines+markers",
            line=dict(color="#2196F3", width=2), marker=dict(size=4),
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=df_merged["datetime"], y=df_merged["cmg_programado"],
            name="CMg Programado PID", mode="lines+markers",
            line=dict(color="#FF9800", width=2, dash="dash"), marker=dict(size=4),
        ), row=1, col=1)

        colores = ["#e53935" if v > 0 else "#43a047" for v in df_merged["diferencia"]]
        fig.add_trace(go.Bar(
            x=df_merged["datetime"], y=df_merged["diferencia"],
            name="Diferencia (Real − Prog.)", marker_color=colores,
        ), row=2, col=1)
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)

        fig.update_layout(
            height=600, template="plotly_white",
            legend=dict(orientation="h", y=1.02, x=0),
            hovermode="x unified", margin=dict(t=80),
        )
        fig.update_yaxes(title_text="USD/MWh", row=1, col=1)
        fig.update_yaxes(title_text="USD/MWh", row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)

        # Histograma de diferencias
        st.markdown("#### Distribución de diferencias (Real − Programado)")
        fig_h = go.Figure()
        fig_h.add_trace(go.Histogram(
            x=df_merged["diferencia"], nbinsx=35,
            marker_color="#5C6BC0", opacity=0.85,
        ))
        fig_h.add_vline(x=0, line_dash="dash", line_color="red",
                        annotation_text="Sin diferencia", annotation_position="top right")
        fig_h.add_vline(x=diff_media, line_dash="dot", line_color="#FF9800",
                        annotation_text=f"Media {diff_media:+.1f}", annotation_position="top left")
        fig_h.update_layout(
            template="plotly_white", height=300,
            xaxis_title="USD/MWh", yaxis_title="Frecuencia (horas)", showlegend=False,
        )
        st.plotly_chart(fig_h, use_container_width=True)

        # Tabla de datos
        with st.expander("📋 Ver tabla de datos"):
            st.dataframe(
                df_merged.rename(columns={
                    "datetime":       "Fecha/Hora",
                    "cmg_real":       "CMg Real (USD/MWh)",
                    "cmg_programado": "CMg Prog. (USD/MWh)",
                    "diferencia":     "Diferencia (USD/MWh)",
                    "diferencia_pct": "Diferencia (%)",
                }).round(3),
                use_container_width=True,
            )

# ── Sección 3: CMg Online en 15 min ──────────────────────────────────────────
st.markdown("---")
st.subheader("🕐 CMg Online en resolución original (15 min)")

if tiene_real:
    df_15 = df_real[df_real["barra_online"] == barra_online].sort_values("datetime")
    fig_15 = go.Figure(go.Scatter(
        x=df_15["datetime"], y=df_15["cmg_real"],
        mode="lines", line=dict(color="#2196F3", width=1.5), name="CMg Online (15 min)",
    ))
    fig_15.update_layout(
        template="plotly_white", height=320,
        xaxis_title="Fecha/Hora", yaxis_title="USD/MWh",
        hovermode="x unified", margin=dict(t=20),
    )
    st.plotly_chart(fig_15, use_container_width=True)
else:
    st.info("Sin datos Online para esta barra en el período seleccionado.")

# ── Raw data ──────────────────────────────────────────────────────────────────
with st.expander("🔬 Datos crudos (debugging)"):
    t1, t2 = st.tabs(["CMg Online", "CMg Programado PID"])
    with t1:
        if not df_real.empty:
            st.write(f"**{len(df_real):,} registros** | columnas: {list(df_real.columns)}")
            st.dataframe(df_real[df_real["barra_online"] == barra_online].head(100),
                         use_container_width=True)
    with t2:
        if not df_prog.empty:
            st.write(f"**{len(df_prog):,} registros** | columnas: {list(df_prog.columns)}")
            st.dataframe(df_prog[df_prog["barra_prog"] == barra_prog].head(100),
                         use_container_width=True)
