import pandas as pd
from pathlib import Path

# Ruta al archivo de salida
ruta_perfilado = Path("data/outputs/tabla_perfilado.csv")

# Leer CSV
tabla_perfilado = pd.read_csv(ruta_perfilado)

# Mostrar contenido
print(tabla_perfilado)
