# Inventario técnico

- Plataforma: Databricks Free Edition / Serverless.
- Catálogo parametrizable; `observatorio_dev` por defecto.
- Formato canónico: Databricks Source `.py`.
- Orquestación: Databricks Asset Bundle.
- Horario: 08:00 `America/Bogota`.

| Capa | Objetos |
|---|---:|
| Landing | volumen `raw_files`, 9 fuentes |
| Bronze | 9 tablas Delta |
| Silver | 9 tablas Delta |
| Gold | 5 dimensiones, 5 hechos, 1 bridge |
| Gold Analytics | 11 vistas |

El job contiene 23 tareas: bootstrap, nueve ingestas, Bronze, nueve Silver,
Gold, quality y Analytics. Los maestros de embalses y planta–embalse forman
parte del DAG. Analytics depende de quality, no directamente de Gold.

`dim_agente` es SCD2. `dim_planta` y `dim_embalse` son Tipo 1; planta admite
miembros inferidos. El resultado previo de 49 validaciones y 0 fallos no se
revalidó localmente porque requiere el catálogo Databricks.

Pendiente: observabilidad extremo a extremo, gobierno TX/alias, hardening
SCD2/bridge, backfill, certificación de vistas, API y publicación conectada.
