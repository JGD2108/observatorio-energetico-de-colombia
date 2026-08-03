# Fase 2 - Observabilidad

## Objetivo

Cada ejecución del Job queda identificada por el `run_id` nativo de Databricks.
La auditoría registra el estado general, el resultado de cada tarea y métricas
operativas de Landing, Bronze, Silver y Gold.

## Flujo

1. `setup_catalog` crea o extiende los objetos de auditoría sin eliminar datos.
2. `audit_start` registra la corrida con estado `RUNNING`.
3. El pipeline ejecuta las 23 tareas funcionales.
4. `audit_finalize` usa `ALL_DONE`, por lo que se ejecuta aunque una tarea
   anterior falle o sea omitida por una dependencia fallida.
5. El finalizador consulta el Jobs API cuando está disponible y usa referencias
   dinámicas del Job como respaldo para guardar estados y errores.
6. Las métricas Delta se obtienen de `DESCRIBE HISTORY` desde el inicio de la
   corrida y se complementan con conteo, rango temporal y rezago actual.

Las reparaciones reutilizan el mismo `run_id`. Los `MERGE` de auditoría
actualizan la corrida y sus tareas en lugar de duplicarlas; `repair_count`
permite reconocer que hubo una reparación.

## Objetos

### `audit.pipeline_runs`

Una fila por ejecución del Job. Incluye Job, entorno, catálogo, disparador,
inicio, fin, duración, estado, resumen de tareas y error consolidado.

### `audit.task_runs`

Una fila por `run_id` y `task_key`. Registra capa, fuente, identificador de la
tarea en Databricks, intento, tiempos, duración, estado y error.

### `audit.layer_metrics`

Una fila por corrida, capa, fuente y tabla o archivo. Registra filas recibidas,
insertadas, actualizadas, rechazadas, sin cambios y actuales; también fechas
mínima y máxima, rezago, estado de recolección y error.

`rows_rejected` se mantiene en cero en fuentes exitosas durante Fase 2. La
persistencia de registros rechazados y su contabilización pertenecen a la
cuarentena de Fase 3.

### Vistas

- `audit.vw_latest_pipeline_run`: última corrida registrada.
- `audit.vw_source_freshness`: última fecha disponible y rezago por fuente y capa.

## Consultas operativas

```sql
SELECT *
FROM observatorio_dev.audit.pipeline_runs
ORDER BY started_at DESC;
```

```sql
SELECT task_key, layer, source_name, status, duration_seconds, error_message
FROM observatorio_dev.audit.task_runs
WHERE run_id = '<run_id>'
ORDER BY started_at, task_key;
```

```sql
SELECT layer, source_name, rows_received, rows_inserted, rows_updated,
       rows_unchanged, rows_current, max_event_time, lag_seconds, status
FROM observatorio_dev.audit.layer_metrics
WHERE run_id = '<run_id>'
ORDER BY layer, source_name;
```

```sql
SELECT *
FROM observatorio_dev.audit.vw_source_freshness
ORDER BY layer, source_name;
```

## Validación de Fase 2

1. Desplegar el target `dev` y ejecutar el Job completo.
2. Confirmar una fila `SUCCESS` en `audit.pipeline_runs`.
3. Confirmar 23 tareas funcionales en `audit.task_runs`.
4. Confirmar métricas para nueve Landing, nueve Bronze, nueve Silver y once Gold.
5. Forzar en una prueba controlada el fallo de una tarea y confirmar que
   `audit_finalize` registra `FAILED` o `UPSTREAM_FAILED`.
6. Reparar la corrida y confirmar que el mismo `run_id` se actualiza y aumenta
   `repair_count`, sin crear duplicados.

## Evidencia de certificación en dev

La corrida `249851600694851` terminó en `SUCCESS` y dejó:

- 23 tareas funcionales registradas y exitosas.
- 0 tareas fallidas.
- 38 métricas: 9 Landing, 9 Bronze, 9 Silver y 11 Gold.
- 0 errores durante la recolección de métricas.
- 1.530 segundos de duración total registrada.

Durante la certificación se detectó que `slv_niveles_embalses` repetía acciones
Spark sobre la misma ventana y los mismos joins. Se consolidaron conteos y
validaciones en menos consultas, sin `cache` ni `persist`. Una ejecución aislada
posterior bajó de 636 segundos a 35 segundos.
