# Fase 4 - Gobierno Silver y Gold

## Objetivo

Retirar reglas maestras embebidas en notebooks, conservar la trazabilidad de
cambios en dimensiones y relaciones, y verificar el recorrido
Bronze–Silver–Gold en cada corrida.

## Política TX gobernada

`governance.ref_version_tx` reemplaza las expresiones duplicadas de Gold y
Quality. Contiene tres reglas activas:

| Regla | Comportamiento |
|---|---|
| `TXF` | prioridad 10000 |
| `TXR` | prioridad 9000 |
| `TX_NUMERIC` | número de TX multiplicado por 100 |

La tabla tiene vigencia, estado, aprobador y Change Data Feed. El bootstrap solo
inserta reglas faltantes: no sobrescribe modificaciones gobernadas. Esta fase
preserva la semántica técnica existente; no sustituye la aprobación funcional
de XM o del dueño del dato.

## Alias gobernados

Los alias de embalses se trasladaron de una lista Python a
`governance.ref_entity_alias`. Silver solo utiliza alias `APPROVED` dentro de su
vigencia. La referencia inicial contiene `CALIMA1`, `PORCEII`, `PORCEIII` y
`URRA1`.

## SCD2 y miembros inferidos

- `dim_agente` se reconstruye desde el historial Silver y retira versiones
  obsoletas cuando una corrección cambia una frontera SCD2.
- El gate comprueba una única versión actual por agente y cero solapamientos.
- Cuando una planta inferida aparece después en el maestro, pasa a oficial pero
  `origen_registro` conserva la transición `INFERIDO:<origen>->MAESTRO_PLANTAS`.

## Bridge planta-embalse

- Silver procesa únicamente el snapshot Bronze más reciente.
- Una relación retirada se marca `activo=false` y recibe `fecha_retiro`; no se
  elimina físicamente.
- Gold replica esta inactivación lógica y conserva el historial.
- La vista de energía embalsada solo atribuye cuando la relación está activa,
  `validated`, no requiere revisión, permite atribución y está vigente en la
  fecha de medición.

## Reconciliación

La tarea `governance_check` corre entre Gold y Quality. Valida TX, unicidad de
alias, SCD2, inferidos y bridge. También escribe nueve filas por corrida en
`governance.layer_reconciliation`, con conteos y diferencias por fuente para
Bronze, Silver y Gold. Las diferencias esperadas por deduplicación y selección
TX se documentan; la reconciliación semántica Silver–Gold continúa bloqueando
en el quality gate de Fase 3.

## Optimización de `slv_generacion`

El notebook repetía más de veinte acciones Spark sobre la misma fuente. Ahora
utiliza una consulta para el watermark, una agregación de entrada, el `MERGE`,
las métricas Delta y una agregación final.

No utiliza `cache`, `persist`, memoria ni disco temporal. En la corrida de
certificación bajó de 362 a 48 segundos, una reducción de 86,7 %.

## Evidencia de certificación en dev

La corrida `840950247397110` terminó en `SUCCESS`:

- 26 tareas exitosas y 24 tareas funcionales auditadas.
- 3 reglas TX activas y 4 alias aprobados.
- 9 reconciliaciones de capa y 0 advertencias.
- 23 relaciones planta-embalse activas y 0 retiradas en el snapshot actual.
- 375 agentes con versión actual.
- 62 reglas de calidad ejecutadas y 0 fallidas.
- 1.522 segundos de duración total.

## Consultas operativas

```sql
SELECT * FROM observatorio_dev.governance.ref_version_tx;
SELECT * FROM observatorio_dev.governance.ref_entity_alias;
```

```sql
SELECT *
FROM observatorio_dev.governance.layer_reconciliation
WHERE run_id = '<run_id>'
ORDER BY source_name;
```

```sql
SELECT codigo_planta, codigo_embalse, activo, fecha_retiro,
       permite_atribucion, valido_desde, valido_hasta
FROM observatorio_dev.gold.bridge_planta_embalse
ORDER BY codigo_planta, codigo_embalse;
```
