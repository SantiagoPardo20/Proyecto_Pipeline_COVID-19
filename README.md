# 📊 Proyecto: Pipeline de Datos COVID-19

Este proyecto implementa un **pipeline de datos** para procesar y analizar información relacionada con COVID-19.  
Incluye un **EDA manual** (análisis exploratorio de datos) y un pipeline orquestado con **Dagster**.

---
## 🎯 Objetivo
- Procesar y transformar datos de COVID-19.  
- Realizar un **EDA manual** para generar tablas de perfilado.  
- Ejecutar un pipeline reproducible con **Dagster**.  
---
## 🚀 Instalación y ejecución

### 1. Clonar el repositorio

git clone https://github.com/SantiagoPardo20/Proyecto_Pipeline_COVID-19.git

cd proyecto-covid

### 2. Crear y activar entorno virtual

# En la raíz del repo
python -m venv .venv

# Activar entorno
# Windows
.venv\Scripts\activate

### 3. Instalar dependencias
pip install -r requirements.txt

## ⚙️ Ejecutar el pipeline con Dagster

dagster dev -m dagster_project.defs -p 8050
Esto levanta la interfaz de Dagster en http://localhost:8050.

🔎 EDA Manual
Genera la tabla de perfilado tabla_perfilado.csv en data/outputs/.

python scripts/eda_manual.py

covid-pipeline/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ data/
│  ├─ inputs/
│  │  └─ covid-19.csv              
│  └─ outputs/                    
├─ scripts/
│  └─ eda_manual.py                
|___dagster_home/
└─ dagster_project/
   ├─ __init__.py
   ├─ assets.py                   
   └─ defs.py
bash
Copiar código
data/outputs/
