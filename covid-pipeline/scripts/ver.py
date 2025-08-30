import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ---------------------------------------
# 1️⃣ Cargar archivos generados por el pipeline
# ---------------------------------------
incidencia = pd.read_csv("data/outputs/incidencia_7d.csv")
factor = pd.read_csv("data/outputs/factor_crec_7d.csv")

# ---------------------------------------
# 2️⃣ Limpieza de datos
# ---------------------------------------
# Incidencia: eliminar NaN
incidencia_clean = incidencia.dropna(subset=['incidencia_7d'])

# Factor de crecimiento: reemplazar inf y eliminar NaN
factor['factor_crec_7d'] = factor['factor_crec_7d'].replace([float('inf'), float('-inf')], pd.NA)
factor_clean = factor.dropna(subset=['factor_crec_7d'])

# ---------------------------------------
# 3️⃣ Estadísticas descriptivas
# ---------------------------------------
print("=== Incidencia 7d por país ===")
print(incidencia_clean.groupby('pais')['incidencia_7d'].describe())

print("\n=== Factor de crecimiento 7d por país ===")
print(factor_clean.groupby('pais')['factor_crec_7d'].describe())

# ---------------------------------------
# 4️⃣ Gráficos de tendencia
# ---------------------------------------
sns.set(style="whitegrid", palette="muted", font_scale=1.2)

# Incidencia 7d
plt.figure(figsize=(12,5))
sns.lineplot(data=incidencia_clean, x='fecha', y='incidencia_7d', hue='pais')
plt.title("Incidencia acumulada a 7 días por 100k habitantes")
plt.xlabel("Fecha")
plt.ylabel("Incidencia 7d")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Factor de crecimiento 7d
plt.figure(figsize=(12,5))
sns.lineplot(data=factor_clean, x='semana_fin', y='factor_crec_7d', hue='pais')
plt.axhline(1, color='red', linestyle='--', label='Crecimiento = 1')
plt.title("Factor de crecimiento semanal (7 días)")
plt.xlabel("Semana fin")
plt.ylabel("Factor 7d")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()

# ---------------------------------------
# 5️⃣ Revisión de valores extremos
# ---------------------------------------
# Incidencia muy alta (>1000) o negativa
incidencia_extremos = incidencia_clean[(incidencia_clean['incidencia_7d'] < 0) | (incidencia_clean['incidencia_7d'] > 1000)]
print("\nValores extremos en incidencia_7d:")
print(incidencia_extremos)

# Factor de crecimiento negativo
factor_extremos = factor_clean[factor_clean['factor_crec_7d'] < 0]
print("\nValores extremos en factor_crec_7d:")
print(factor_extremos)
