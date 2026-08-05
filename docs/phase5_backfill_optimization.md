# Fase 5: backfill histórico y optimización Silver

La Fase 5 permite ejecutar ventanas históricas explícitas sin cambiar el comportamiento diario. En modo `AUTO`, cada ingesta conserva su cálculo incremental. En modo `BACKFILL`, las siete fuentes SIMEM reciben el mismo rango y lo descargan en fragmentos configurables; los `MERGE` de Bronze y Silver mantienen la repetición idempotente.

## Parámetros

- `execution_mode`: `AUTO`, `INCREMENTAL` o `BACKFILL`.
- `backfill_start_date` y `backfill_end_date`: fechas inclusivas `YYYY-MM-DD`; son obligatorias en `BACKFILL`.
- `backfill_chunk_days`: tamaño de fragmento SIMEM, entre 1 y 366; por defecto 31.
- `simem_max_retries`: reintentos ante `429/500/502/503/504` o desconexiones; por defecto 5.
- `simem_retry_base_seconds`: espera inicial exponencial entre reintentos; por defecto 5 segundos y tope de 60.

Disponibilidad de plantas limita internamente cada descarga a 31 días y escribe cada bloque directamente en Landing. Esto evita materializar los aproximadamente 12 millones de filas anuales en memoria, aunque el chunk general del backfill sea 366.
- `backfill_id`: identificador auditable; el job usa su `run_id` por defecto.

Ejemplo desde Windows:

```bat
databricks bundle run observatorio_daily_pipeline -t dev --params execution_mode=BACKFILL,backfill_start_date=2024-01-01,backfill_end_date=2024-12-31,backfill_chunk_days=31
```

`audit.backfill_runs` registra la solicitud y su resultado. `audit.backfill_coverage` verifica que demanda, disponibilidad, generación, niveles y precio comiencen en la fecha solicitada y terminen dentro del SLA de publicación (`max_lag_days`) antes de actualizar Gold.

## Optimización Silver

Los nueve loaders Silver evitan `cache` y `persist`. Los hechos y maestros usan perfiles agregados únicos, deduplicación por ventana, enriquecimiento antes del `MERGE` y un solo perfil final de unicidad. Los `display` quedan limitados a muestras de error. Esto elimina acciones Spark repetidas sin reservar memoria ni disco para caché.
