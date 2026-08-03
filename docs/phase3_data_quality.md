# Fase 3 - Calidad, cuarentena y alertas

## Objetivo

Convertir el control Gold existente en un quality gate gobernado, trazable por
el `run_id` nativo de Databricks y capaz de distinguir errores bloqueantes de
advertencias operativas.

## Política de severidad

| Severidad | Comportamiento | Ejemplos |
|---|---|---|
| `CRITICAL` | Bloquea Analytics | grano duplicado, PK nula, huérfanos, reconciliación o esquema |
| `HIGH` | Bloquea Analytics | valores inválidos, TX, frescura o cobertura horaria |
| `MEDIUM` | Registra advertencia | variación diaria fuera de 0.5x–2x la mediana |
| `LOW` | Registra advertencia | reservado para reglas informativas futuras |

La variación de volumen no bloquea porque el mercado eléctrico puede presentar
cambios legítimos. Se conserva como evidencia para análisis temporal.

## Objetos

- `audit.data_quality_results`: una fila idempotente por corrida y regla, con
  dimensión, severidad, tasa de error y condición de bloqueo.
- `quarantine.data_quality_exceptions`: evidencia de cada regla fallida, tabla
  origen, motivo y payload JSON. El `MERGE` evita duplicados durante reparaciones.
- `audit.data_quality_alerts`: alertas `OPEN` para fallos bloqueantes. Una regla
  aprobada en una corrida posterior resuelve sus alertas abiertas.
- `audit.vw_latest_data_quality`: último resultado de cada regla y componente.
- `audit.vw_open_data_quality_alerts`: alertas que todavía requieren atención.

La tabla histórica anterior
`monitoring.gold_incremental_quality_results` continúa escribiéndose para no
romper consumidores existentes.

## Controles añadidos

1. Contrato mínimo de columnas y tipos para los cinco hechos Gold.
2. Cobertura global de 24 horas por día para los cuatro hechos horarios.
3. Días faltantes entre la fecha mínima y máxima observada.
4. Variación diaria robusta frente a la mediana de la ventana.
5. Propagación del `run_id` del Job a resultados, cuarentena y alertas.
6. Conteo de errores detectados en `audit.layer_metrics.rows_rejected`.

Las consultas se ejecutan mediante agregaciones y `MERGE`; no se utiliza
`cache`, `persist` ni almacenamiento temporal de DataFrames.

## Consultas operativas

```sql
SELECT component, rule_name, severity, error_count, error_rate, passed
FROM observatorio_dev.audit.vw_latest_data_quality
ORDER BY severity, component, rule_name;
```

```sql
SELECT *
FROM observatorio_dev.audit.vw_open_data_quality_alerts
ORDER BY created_at DESC;
```

```sql
SELECT run_id, component, severity, reason, source_table, payload_json
FROM observatorio_dev.quarantine.data_quality_exceptions
ORDER BY quarantined_at DESC;
```

## Criterios de certificación

1. El bootstrap crea los cinco objetos nuevos y las dos vistas.
2. El Job aprobado deja resultados gobernados con el mismo `run_id`.
3. Un fallo `MEDIUM` produce excepción pero no bloquea Analytics.
4. Un fallo `HIGH` o `CRITICAL` crea una alerta y bloquea Analytics.
5. `audit_finalize` se ejecuta con `ALL_DONE` y registra los rechazos.
6. Una reparación no duplica resultados, excepciones ni alertas de la corrida.

## Evidencia de certificación en dev

La corrida `320481212878233` terminó en `SUCCESS` con las 25 tareas aprobadas y
dejó resultados asociados al mismo identificador:

- 62 reglas ejecutadas y 62 aprobadas.
- 39 reglas `CRITICAL`, 19 `HIGH` y 4 `MEDIUM`.
- 0 fallos bloqueantes.
- 0 excepciones en cuarentena y 0 alertas abiertas para la corrida.
- 0 rechazos registrados porque los datos cumplieron todos los contratos.

La corrida completa duró 1.381 segundos. `slv_generacion` consumió 362 segundos;
es una oportunidad de rendimiento de Silver independiente de Fase 3.
