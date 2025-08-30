from dagster import asset, AssetCheckResult, AssetCheckSeverity, asset_check
import pandas as pd
import requests
from io import StringIO
from pathlib import Path

# -----------------------------
# Parámetros
# -----------------------------
URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
PAIS_1 = "Ecuador"
PAIS_2 = "Argentina"

OUT_DIR = Path("data/outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Paso 1 extra: Perfilado manual
# -----------------------------
@asset(description="Genera y lo exporta a tabla_perfilado.csv")
def tabla_perfilado(datos_procesados: pd.DataFrame) -> str:
    perfil = []

    for col in datos_procesados.columns:
        serie = datos_procesados[col]
        perfil.append({"metrica": "dtype", "columna": col, "valor": str(serie.dtype)})
        perfil.append({"metrica": "nulos", "columna": col, "valor": int(serie.isna().sum())})
        perfil.append({"metrica": "valores_unicos", "columna": col, "valor": int(serie.nunique())})

        if pd.api.types.is_numeric_dtype(serie):
            perfil.append({"metrica": "min", "columna": col, "valor": serie.min(skipna=True)})
            perfil.append({"metrica": "max", "columna": col, "valor": serie.max(skipna=True)})
            perfil.append({"metrica": "mean", "columna": col, "valor": round(serie.mean(skipna=True), 2)})

    df_perfil = pd.DataFrame(perfil)
    out_csv = OUT_DIR / "tabla_perfilado.csv"
    df_perfil.to_csv(out_csv, index=False)

    return str(out_csv)


# -----------------------------
# Paso 2: Lectura sin transformar
# -----------------------------
@asset(description="Lee el CSV canónico de OWID (sin transformar).")
def leer_datos() -> pd.DataFrame:
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

@asset_check(asset=leer_datos, description="Chequeos de calidad de entrada para leer_datos.")
def checks_entrada(leer_datos: pd.DataFrame):
    df = leer_datos
    results = []

    if "date" in df.columns:
        max_date = pd.to_datetime(df["date"], errors="coerce").max()
        passed = bool(pd.Timestamp.today().normalize() >= max_date.normalize())
        results.append(
            AssetCheckResult(
                passed=passed,
                severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.WARN,
                description=f"max(date)={max_date.date()} <= hoy",
            )
        )

    for col in ["location", "date", "population"]:
        ok = col in df.columns and bool(df[col].notna().all())
        results.append(
            AssetCheckResult(
                passed=ok,
                severity=AssetCheckSeverity.ERROR if not ok else AssetCheckSeverity.WARN,
                description=f"Columna clave '{col}' presente y no nula.",
            )
        )
    if {"location", "date"}.issubset(df.columns):
        dup = df.duplicated(subset=["location", "date"]).sum()
        results.append(
            AssetCheckResult(
                passed=bool(dup == 0),
                severity=AssetCheckSeverity.WARN if dup > 0 else AssetCheckSeverity.WARN,
                description=f"Duplicados (location,date): {dup}",
            )
        )

    if "population" in df.columns:
        nonpos = (df["population"] <= 0).sum()
        results.append(
            AssetCheckResult(
                passed=bool(nonpos == 0),
                severity=AssetCheckSeverity.ERROR if nonpos > 0 else AssetCheckSeverity.WARN,
                description=f"population>0 filas no cumplidas: {nonpos}",
            )
        )

    if "new_cases" in df.columns:
        negatives = (df["new_cases"] < 0).sum()
        results.append(
            AssetCheckResult(
                passed=True,
                severity=AssetCheckSeverity.WARN if negatives > 0 else AssetCheckSeverity.WARN,
                description=f"new_cases negativos (posibles revisiones): {negatives}",
            )
        )

    return results


# -----------------------------
# Paso 3: Procesamiento de datos
# -----------------------------
@asset(description="Limpieza, deduplicación y filtro a Ecuador y Argentina. Selección de columnas esenciales.")
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    df = leer_datos.copy()

    if "country" in df.columns:
        df = df.rename(columns={"country": "location"})

    if {"location", "date"}.issubset(df.columns):
        df = df.sort_values("date").drop_duplicates(subset=["location", "date"], keep="last")

    if "location" in df.columns:
        df = df[df["location"].isin([PAIS_1, PAIS_2])]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    cols = ["location", "date", "new_cases", "people_vaccinated", "population"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].reset_index(drop=True)

    return df

# -----------------------------
# Paso 4A: Métrica incidencia 7d por 100k
# -----------------------------
@asset(description="Incidencia acumulada a 7 días por 100k habitantes (promedio móvil).")
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df = df.sort_values(["location", "date"])

    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100000

    df["incidencia_7d"] = (
        df.groupby("location", group_keys=False)["incidencia_diaria"]
        .rolling(7, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    out = df[["date", "location", "incidencia_7d"]].copy()
    out = out.rename(columns={"location": "pais", "date": "fecha"})
    return out.reset_index(drop=True)

# -----------------------------
# Paso 4B: Factor de crecimiento semanal (7 días)
# -----------------------------
@asset(description="Factor de crecimiento 7d: casos_semana_actual / casos_semana_prev.")
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df = df.sort_values(["location", "date"])

    df["casos_7d"] = (
        df.groupby("location", group_keys=False)["new_cases"]
        .rolling(7, min_periods=7)
        .sum()
        .reset_index(level=0, drop=True)
    )

    df["casos_7d_prev"] = df.groupby("location")["casos_7d"].shift(7)

    df["factor_crec_7d"] = df["casos_7d"] / df["casos_7d_prev"]

    out = df[["date", "location", "casos_7d", "factor_crec_7d"]].copy()
    out = out.rename(columns={"date": "semana_fin", "location": "pais", "casos_7d": "casos_semana"})
    out = out.dropna(subset=["casos_semana", "factor_crec_7d"]).reset_index(drop=True)
    return out

# -----------------------------
# Paso 5: Chequeos de salida
# -----------------------------
@asset_check(asset=metrica_incidencia_7d, description="Validación de rango incidencia_7d (0 a 2000).")
def checks_salida_incidencia(metrica_incidencia_7d: pd.DataFrame):
    df = metrica_incidencia_7d
    bad = df[(df["incidencia_7d"] < 0) | (df["incidencia_7d"] > 2000)]
    passed = bad.empty
    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.WARN,
        description=f"Valores fuera de rango [0,2000]: {len(bad)} filas"
    )

@asset_check(asset=metrica_factor_crec_7d, description="Validación de factor_crec_7d (no negativo, finito).")
def checks_salida_factor(metrica_factor_crec_7d: pd.DataFrame):
    df = metrica_factor_crec_7d
    bad = df[(~pd.Series(pd.notna(df["factor_crec_7d"]))) | (df["factor_crec_7d"] < 0)]
    passed = bad.empty
    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.WARN if not passed else AssetCheckSeverity.WARN,
        description=f"Valores inválidos de factor_crec_7d: {len(bad)} filas"
    )

# -----------------------------
# Paso 6: Exportación de resultados
# -----------------------------
@asset(description="Exporta datos procesados y métricas a Excel y CSVs.")
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    xlsx_path = OUT_DIR / "reporte_covid.xlsx"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        datos_procesados.to_excel(writer, sheet_name="datos_procesados", index=False)
        metrica_incidencia_7d.to_excel(writer, sheet_name="incidencia_7d", index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name="factor_crec_7d", index=False)

    metrica_incidencia_7d.to_csv(OUT_DIR / "incidencia_7d.csv", index=False)
    metrica_factor_crec_7d.to_csv(OUT_DIR / "factor_crec_7d.csv", index=False)

    return str(xlsx_path)
