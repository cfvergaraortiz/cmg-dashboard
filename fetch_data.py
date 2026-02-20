"""
fetch_data.py — Recolector de datos CMg del CEN
================================================
Este script se puede correr:
  - Manualmente:          python fetch_data.py
  - Programado (GitHub Actions): automáticamente cada hora

Guarda los datos en la carpeta data/ como archivos CSV.
El dashboard (cmg_dashboard.py) lee desde esos archivos.
"""

import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

BASE_URL  = "https://sipub.api.coordinador.cl:443"
DATA_DIR  = "data"   # carpeta donde se guardan los CSVs

# El token se lee desde variable de entorno (configurada en GitHub Actions Secrets)
USER_KEY = os.environ.get("CEN_TOKEN", "")

# Cuántos días hacia atrás guardar (ajustable)
DIAS_HISTORICO = 7

# Diccionario de barras: clave = fragmento del nombre barra_info (no key sensitive)
#                         valor = llave_cmg del programado
BARRAS = {
    "PUERTO MONTT":  "PMontt220",
    "JAHUEL":        "AJahuel220",
    "POLPAICO":      "Polpaico220",
    "AZUCAR":        "PAzucar220",
    "CARDONES":      "Cardones220",
    "QUILLOTA":      "Quillota220",
    "CRUCERO":       "Crucero220",
    "CHARRUA":       "Charrua220",
}

# =============================================================================
# FUNCIONES API
# =============================================================================

def fetch_paginated(url: str, params: dict, page_size: int = 500) -> list:
    """Trae todos los registros de un endpoint con paginación y reintentos."""
    all_records = []
    page = 1
    BACKOFF = [10, 30, 60]

    while True:
        params_page = {**params, "user_key": USER_KEY, "page": page, "limit": page_size}

        for intento, espera in enumerate([0] + BACKOFF):
            if espera:
                print(f"  → Reintentando en {espera}s...")
                time.sleep(espera)
            try:
                r = requests.get(url, params=params_page,
                                 headers={"accept": "application/json"}, timeout=30)
                r.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                print(f"  ⚠ Error {status} (intento {intento+1})")
                if intento == len(BACKOFF):
                    print("  ✗ Se agotaron los reintentos.")
                    return all_records
            except requests.exceptions.RequestException as e:
                print(f"  ✗ Error de conexión: {e}")
                return all_records

        records = r.json().get("data", [])
        if not records:
            break
        all_records.extend(records)
        if len(records) < page_size:
            break
        page += 1
        time.sleep(1)

    return all_records


def fetch_online(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Trae CMg Online sin filtro de barra y filtra localmente por barra_info.
    Así evitamos depender del nombre exacto (key sensitive) de barra_transf.
    """
    url = f"{BASE_URL}/costo-marginal-online/v4/findByDate"
    records = fetch_paginated(url, {"startDate": start_date, "endDate": end_date})

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Filtrar localmente: nos quedamos con filas cuyo barra_info contenga
    # alguno de los fragmentos del diccionario BARRAS (no key sensitive)
    def encontrar_barra(nombre_info: str) -> str:
        nombre_up = nombre_info.upper()
        for fragmento in BARRAS:
            if fragmento.upper() in nombre_up:
                return fragmento
        return None

    df["barra_key"] = df["barra_info"].apply(encontrar_barra)
    df = df[df["barra_key"].notna()].copy()

    if df.empty:
        return pd.DataFrame()

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
    # Agregar columna con la clave del programado para el merge posterior
    df["barra_prog"] = df["barra_key"].map(BARRAS)

    cols = ["datetime", "barra_online", "barra_prog", "nombre_barra", "cmg_real", "cmg_real_clp"]
    return df[[c for c in cols if c in df.columns]].sort_values(["barra_online", "datetime"]).reset_index(drop=True)


def fetch_programado(start_date: str, end_date: str) -> pd.DataFrame:
    """Trae CMg Programado PID."""
    url = f"{BASE_URL}/cmg-programado-pid/v4/findByDate"
    records = fetch_paginated(url, {"startDate": start_date, "endDate": end_date})

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["fecha_hora"])
    df = df.rename(columns={
        "nmb_barra_info": "nombre_barra",
        "llave_cmg":      "barra_prog",
        "cmg_usd_mwh":    "cmg_programado",
    })

    barras_prog = set(BARRAS.values())
    df = df[df["barra_prog"].isin(barras_prog)]

    cols = ["datetime", "barra_prog", "nombre_barra", "cmg_programado", "zona", "region"]
    return df[[c for c in cols if c in df.columns]].sort_values(["barra_prog", "datetime"]).reset_index(drop=True)


# =============================================================================
# LÓGICA PRINCIPAL
# =============================================================================

def actualizar_csv(nombre_archivo: str, df_nuevo: pd.DataFrame, col_datetime: str = "datetime"):
    """
    Combina el CSV existente con los datos nuevos y guarda.
    Elimina duplicados manteniendo el registro más reciente.
    """
    ruta = os.path.join(DATA_DIR, nombre_archivo)

    if os.path.exists(ruta):
        df_existente = pd.read_csv(ruta, parse_dates=[col_datetime])
        df_combinado = pd.concat([df_existente, df_nuevo], ignore_index=True)
    else:
        df_combinado = df_nuevo.copy()

    # Eliminar duplicados
    subset_dedup = [col_datetime]
    if "barra_online" in df_combinado.columns:
        subset_dedup.append("barra_online")
    elif "barra_prog" in df_combinado.columns:
        subset_dedup.append("barra_prog")

    df_combinado = (
        df_combinado
        .drop_duplicates(subset=subset_dedup, keep="last")
        .sort_values(subset_dedup)
        .reset_index(drop=True)
    )

    # Recortar al histórico definido para no crecer indefinidamente
    corte = pd.Timestamp.now() - pd.Timedelta(days=DIAS_HISTORICO)
    df_combinado = df_combinado[df_combinado[col_datetime] >= corte]

    df_combinado.to_csv(ruta, index=False)
    print(f"  ✓ {ruta} — {len(df_combinado):,} registros guardados")


def main():
    if not USER_KEY:
        raise ValueError("No se encontró CEN_TOKEN. Configúralo como variable de entorno.")

    os.makedirs(DATA_DIR, exist_ok=True)

    hoy   = datetime.today().date()
    ayer  = hoy - timedelta(days=1)  # usamos ayer como fin para evitar el
    hace  = hoy - timedelta(days=3)  # desfase UTC vs hora Chile
    start_str = hace.strftime("%Y-%m-%d")
    end_str   = ayer.strftime("%Y-%m-%d")

    print(f"\n{'='*50}")
    print(f"Actualizando datos: {start_str} → {end_str}")
    print(f"{'='*50}")

    # CMg Online: probamos hoy y ayer en UTC ya que el endpoint solo
    # entrega datos recientes. Nos quedamos con la fecha que tenga datos.
    hoy_str  = hoy.strftime("%Y-%m-%d")
    ayer_str = ayer.strftime("%Y-%m-%d")
    df_online = pd.DataFrame()
    for fecha_str in [hoy_str, ayer_str]:
        print(f"\n📡 Descargando CMg Online ({fecha_str})...")
        df_online = fetch_online(fecha_str, fecha_str)
        if not df_online.empty:
            print(f"  ✓ Datos encontrados para {fecha_str}")
            actualizar_csv("cmg_online.csv", df_online)
            break
        print(f"  ✗ Sin datos para {fecha_str}, probando fecha anterior...")
    else:
        print("  ✗ Sin datos Online para ninguna fecha")
    df_prog = fetch_programado(start_str, end_str)
    if not df_prog.empty:
        actualizar_csv("cmg_programado.csv", df_prog)
    else:
        print("  ✗ Sin datos Programado")

    # Guardar timestamp de última actualización
    with open(os.path.join(DATA_DIR, "ultima_actualizacion.txt"), "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print("\n✅ Actualización completada.")


if __name__ == "__main__":
    main()
