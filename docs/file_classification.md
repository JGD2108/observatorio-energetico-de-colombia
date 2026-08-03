# Clasificación de archivos

## Activos

`databricks.yml`, `Automation/Job.yaml`, `setup/00_bootstrap.py`, nueve
ingestas, `02_bronze_daily.py`, nueve Silver, Gold, quality incremental,
Analytics y configuración.

## Compatibilidad

Los DDL Bronze/Silver redirigen al bootstrap. DDL Gold es un contrato
declarativo no destructivo usado por el bootstrap.

## Referencia histórica

- `Automation/05_quality_checks.py`
- `Automation/gold_quality_validation.py`
- `Bronze_Load/02_load_json_bronze.py`
- EDA y `GOLD LOAD/Arquitectura GOLD decisiones.md`

## Retirados

- `.ipynb` duplicados.
- Notebook de prueba con `:` en el nombre.
- `observatorio-energetico-astro/**/dist/`, compilación regenerable.
