import pandas as pd
from pathlib import Path

INPUT = Path("data/inputs/covid-19.csv")
OUTPUT_DIR = Path("data/outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTPUT_DIR / "tabla_perfilado.csv"

def main():
    df = pd.read_csv(INPUT)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    perfil = []

    for col in df.columns:
        perfil.append({"metrica": "dtype", "columna": col, "valor": str(df[col].dtype)})

    if "new_cases" in df.columns:
        perfil.append({"metrica": "min(new_cases)", "columna": "new_cases", "valor": df["new_cases"].min(skipna=True)})
        perfil.append({"metrica": "max(new_cases)", "columna": "new_cases", "valor": df["new_cases"].max(skipna=True)})

    for target in ["new_cases", "people_vaccinated"]:
        if target in df.columns:
            pct_null = df[target].isna().mean() * 100
            perfil.append({"metrica": "%nulos", "columna": target, "valor": round(pct_null, 2)})

    if "date" in df.columns:
        perfil.append({"metrica": "min(date)", "columna": "date", "valor": df["date"].min()})
        perfil.append({"metrica": "max(date)", "columna": "date", "valor": df["date"].max()})

    pd.DataFrame(perfil).to_csv(OUT_CSV, index=False)
    print(f"Perfil guardado en: {OUT_CSV}")

if __name__ == "__main__":
    main()
