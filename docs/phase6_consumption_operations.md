# Fase 6: consumo y operación productiva

La Fase 6 convierte Gold validado en dos contratos estables: una capa de negocio para herramientas BI y una capa técnica para operar el pipeline. También introduce una compuerta de SLO antes de cerrar cada ejecución.

## Contratos de consumo

El esquema `serving` publica:

- `kpi_sistema_diario`: generación, demanda, disponibilidad, precio, completitud y medias móviles de 7 días.
- `operacion_planta_diaria`: operación y consistencia por planta.
- `demanda_mercado_diaria`: demanda por mercado y calendario.
- `energia_embalsada_diaria`: energía, variación y cobertura de atribución.
- `estado_fuentes`: cobertura, rezago y estado operativo por fuente.

El esquema `serving_technical` publica:

- `pipeline_health`: estado, duración y tasa móvil de éxito de las últimas 10 ejecuciones.
- `task_performance`: duración promedio, p95, máximo y tasa de éxito por tarea.
- `quality_alerts`: alertas de calidad con su edad y resolución.

## KPIs y SLO

| SLO | Definición | Umbral | Acción |
| --- | --- | --- | --- |
| Frescura | Máximo rezago entre fuentes Gold | ≤ `max_lag_days` (45 por defecto) | Bloquea producción |
| Completitud | Horas presentes en el último día disponible | 100% (24/24) | Bloquea producción |
| Calidad | Reglas bloqueantes fallidas en la ejecución | 0 | Bloquea producción |
| Evidencia | Reglas de calidad ejecutadas en la ejecución | ≥1 | Bloquea producción |
| Alertas | Alertas HIGH/CRITICAL abiertas | 0 | Bloquea producción |
| Confiabilidad | Éxito de las últimas 10 ejecuciones finalizadas | ≥95% | Alerta operativa; no bloquea contratos actuales |
| Contrato serving | Vistas publicadas y consultables | 8/8 | Bloquea producción |

Los resultados se guardan en `monitoring.slo_results`. Los incumplimientos se abren en `monitoring.operational_alerts` y se resuelven automáticamente cuando el SLO vuelve a aprobar.

## Operación diaria

1. Revisar el job `observatorio_daily_pipeline_<target>` en **Workflows → Jobs**.
2. Confirmar que `operational_readiness` y `audit_finalize` estén verdes.
3. Consultar `serving_technical.pipeline_health` para éxito y duración.
4. Consultar `monitoring.operational_alerts` y `audit.data_quality_alerts` si el gate falla.
5. Reparar desde la primera tarea fallida; los `MERGE` hacen segura la repetición.

## Promoción a producción

```bat
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run observatorio_daily_pipeline -t prod --params execution_mode=AUTO
```

El calendario de `prod` se despliega inicialmente en `PAUSED`. Solo debe activarse después de que la ejecución manual termine verde, las ocho vistas serving sean consultables y el propietario operativo acepte los SLO. Dev permanece `UNPAUSED`.

## Recuperación

- Error de fuente: reintentar la tarea de ingesta; no truncar tablas.
- Error Silver/Gold: reparar desde la tarea fallida; los merges son idempotentes.
- Backfill: usar un `backfill_id` nuevo y revisar `audit.backfill_coverage`.
- Gate operativo fallido: corregir el SLO indicado y reparar `operational_readiness`.
- Rollback de código: desplegar el último commit certificado; no eliminar datos Delta.
