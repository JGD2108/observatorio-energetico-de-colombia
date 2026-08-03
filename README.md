# Observatorio Energético de Colombia

Pipeline Lakehouse en Databricks para integrar y analizar datos del sistema
eléctrico colombiano. Usa arquitectura Medallion, Delta Lake, Unity Catalog y
un modelo dimensional con cinco hechos.

> Fases 1, 2 y 3 certificadas mediante ejecuciones completas en Databricks:
> pipeline reproducible, observabilidad y calidad gobernada con cuarentena.

## Flujo

```text
Audit Start → SIMEM / maestros → Landing → Bronze → Silver → Gold
           → Quality Gate → Analytics → Audit Finalize
```

- Nueve dominios de ingesta, nueve tablas Bronze y nueve Silver.
- Cinco dimensiones, cinco hechos y un bridge en Gold.
- Once vistas en `gold_analytics`.
- Quality incremental con las 49 validaciones originales más controles de
  esquema, cobertura horaria y variación de volumen.
- Job diario a las 08:00 `America/Bogota`.

## Estructura

| Ruta | Propósito |
|---|---|
| `Ingestion/` | Extracción de SIMEM y maestros |
| `Bronze_Load/02_bronze_daily.py` | Carga Bronze canónica |
| `Silver_Load/` | Normalización y MERGE por dominio |
| `GOLD LOAD/GOLD_LOAD.py` | Modelo dimensional |
| `Automation/` | Job y quality checks |
| `observability/` | Auditoría de corridas, tareas y métricas por capa |
| `Gold_Analytics/` | Vistas analíticas; certificación pendiente |
| `config/` | Configuración parametrizable |
| `setup/00_bootstrap.py` | Bootstrap idempotente |

Databricks Source `.py` es el formato canónico. Los `.ipynb` duplicados y el
notebook con `:` en el nombre se retiraron para permitir checkout en Windows.

## Despliegue

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run observatorio_daily_pipeline -t dev
```

El bundle acepta `catalog`, `environment`, `historical_start_date`,
`lookback_days` y `max_lag_days`. El bootstrap no ejecuta `DROP TABLE` ni
cambia tipos automáticamente.

Dependencias fijadas:

- `pydataxm==0.3.18`
- `geopy==2.5.0`

## Documentación

- [Arquitectura actual](docs/architecture.md)
- [Inventario técnico](docs/technical_inventory.md)
- [Fase 1](docs/phase1_hardening.md)
- [Fase 2 y manual de observabilidad](docs/phase2_observability.md)
- [Fase 3: calidad, cuarentena y alertas](docs/phase3_data_quality.md)
- [Clasificación de archivos](docs/file_classification.md)

## Limitaciones

- Landing todavía usa nombres fijos.
- La cuarentena de Fase 3 conserva excepciones agregadas por regla; el rechazo
  físico fila a fila durante Silver se ampliará cuando se gobiernen sus contratos.
- La regla TX requiere gobierno formal en Fase 4.
- Las vistas no están certificadas semánticamente; corresponde a Fase 6.
- No existe todavía API, serving ni dashboard conectado a datos.
