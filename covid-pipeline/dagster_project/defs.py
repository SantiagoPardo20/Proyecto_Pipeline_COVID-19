from dagster import Definitions
from .assets import (
    leer_datos,
    checks_entrada,
    datos_procesados,
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    checks_salida_incidencia,
    checks_salida_factor,
    reporte_excel_covid,
)

defs = Definitions(
    assets=[
        leer_datos,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid,
        
    ],
    asset_checks=[
        checks_entrada,
        checks_salida_incidencia,
        checks_salida_factor,
    ],
)
