# Fase 5: backfill histórico y optimización Silver

La Fase 5 permite ejecutar ventanas históricas explícitas sin cambiar el comportamiento diario. En modo `AUTO`, cada ingesta conserva su cálculo incremental. En modo `BACKFILL`, las siete fuentes SIMEM reciben el mismo rango y lo descargan en fragmentos configurables; los `MERGE` de Bronze y Silver mantienen la repetición idempotente.

## Parámetros

- `execution_mode`: `AUTO`, `INCREMENTAL` o `BACKFILL`.
- `backfill_start_date` y `backfill_end_date`: fechas inclusivas `YYYY-MM-DD`; son obligatorias en `BACKFILL`.
- `backfill_chunk_days`: tamaño de fragmento SIMEM, entre 1 y 366; por defecto 31.
- `backfill_id`: identificador auditable; el job usa su `run_id` por defecto.

Ejemplo desde Windows:

```bat
databricks bundle run observatorio_daily_pipeline -t dev --params execution_mode=BACKFILL,backfill_start_date=2024-01-01,backfill_end_date=2024-12-31,backfill_chunk_days=31
```

`audit.backfill_runs` registra la solicitud y su resultado. `audit.backfill_coverage` verifica que demanda, disponibilidad, generación, niveles y precio cubran la ventana antes de actualizar Gold.

## Optimización Silver

Los nueve loaders Silver evitan `cache` y `persist`. Los hechos y maestros usan perfiles agregados únicos, deduplicación por ventana, enriquecimiento antes del `MERGE` y un solo perfil final de unicidad. Los `display` quedan limitados a muestras de error. Esto elimina acciones Spark repetidas sin reservar memoria ni disco para caché.
