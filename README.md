# Observatorio Energético de Colombia

Plataforma de datos y analítica para observar el sistema eléctrico colombiano,
integrar fuentes del mercado y convertirlas en indicadores confiables para
operación, análisis y toma de decisiones.

## Objetivo

El proyecto resuelve el recorrido completo desde los datos públicos de SIMEM y
los maestros del sistema hasta un dashboard ejecutivo en Power BI. Su foco es
responder preguntas como:

- ¿Cómo evolucionan generación, demanda, disponibilidad y precio de bolsa?
- ¿Qué plantas y tecnologías explican el desempeño del sistema?
- ¿Cuál es el nivel y la variación de la energía embalsada?
- ¿Qué tan frescos, completos y confiables son los datos publicados?
- ¿Está operando correctamente el pipeline que alimenta las cifras?

El resultado es una base reproducible para analizar el mercado colombiano sin
mezclar fechas de corte, fuentes sin validar o reglas maestras escondidas en
visualizaciones.

## Dashboard publicado

[Abrir el dashboard en Power BI](https://app.powerbi.com/groups/me/reports/1b9cb56e-7426-4caa-82d5-8862cdc6b9e3/2d35f50cc70c0b3d126e?experience=power-bi)

El enlace requiere permisos de Power BI. Actualmente apunta al reporte
publicado en **Mi área de trabajo**; para compartirlo con un equipo se recomienda
publicarlo también en un workspace institucional.

El PBIP versionado se encuentra en
[`powerbi/Dashboard_observatorio/`](powerbi/Dashboard_observatorio/).

## Arquitectura

```text
SIMEM / maestros
       │
       ▼
Landing en Unity Catalog Volumes
       │
       ▼
Bronze canónica (Delta)
       │
       ▼
Silver normalizada + MERGE idempotente
       │
       ├── Gobierno: TX, alias, SCD2 y bridge planta-embalse
       ├── Calidad: esquema, grano, cobertura, frescura y cuarentena
       ▼
Gold dimensional
       │
       ▼
Gold Analytics
       │
       ▼
Serving de negocio y Serving técnico
       │
       ▼
Power BI Service
```

La plataforma usa Databricks, Delta Lake y Unity Catalog. El pipeline se
ejecuta diariamente a las 08:00 en `America/Bogota` y conserva auditoría por
`run_id` para tareas, capas, métricas y resultados de calidad.

### Modelo de datos

- Nueve dominios de ingesta, nueve tablas Bronze y nueve tablas Silver.
- Cinco dimensiones y cinco hechos en Gold.
- Un bridge gobernado entre plantas y embalses.
- Vistas `gold_analytics` para el modelo dimensional y vistas `serving` para
  consumo estable.
- Capa `serving_technical` para salud del pipeline, rendimiento de tareas y
  calidad.

### Contratos Serving

| Vista | Uso |
|---|---|
| `kpi_sistema_diario` | generación, demanda, disponibilidad y precio |
| `operacion_planta_diaria` | desempeño y capacidad por planta |
| `demanda_mercado_diaria` | demanda por mercado, picos y cobertura horaria |
| `energia_embalsada_diaria` | nivel, variación y cobertura de asignación |
| `estado_fuentes` | fecha máxima, rezago y estado operativo |
| `generacion_tecnologia_diaria` | mezcla y utilización por tecnología |
| `pipeline_health` | estado y duración de ejecuciones |
| `task_performance` | promedio, p95 y éxito por tarea |
| `quality_alerts` | resultados de calidad abiertos e históricos |

## Páginas del dashboard

1. **Resumen del sistema eléctrico**: KPI ejecutivos, generación contra demanda,
   precio y frescura de las fuentes.
2. **Análisis por planta**: generación, disponibilidad, capacidad, factor de
   capacidad, ranking y tecnología.
3. **Operación técnica**: estado del pipeline, duración, tareas lentas y calidad.
4. **Demanda y mercado**: demanda acumulada, promedio, pico, mercado regulado y
   no regulado, con detalle diario.
5. **Embalses y cobertura**: energía embalsada, variación diaria, cobertura de
   asignación directa y plantas medidas.

## Fases completadas

| Fase | Resultado |
|---|---|
| 1 | endurecimiento del pipeline y contratos iniciales |
| 2 | observabilidad por corrida, tarea y capa |
| 3 | calidad, cuarentena y reglas bloqueantes |
| 4 | gobierno TX, alias, SCD2 y bridge planta-embalse |
| 5 | backfill auditable y optimización Silver sin cache/persist |
| 6 | Serving de negocio, Serving técnico y SLO operativo |
| 7 | modelo semántico y dashboard Power BI de cinco páginas |
| 8 | publicación y actualización en Power BI Service |
| 9 | revisión manual de salud y hoja de ruta de evolución |

## Desarrollo local y Databricks

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run observatorio_daily_pipeline -t dev
```

El bundle acepta `catalog`, `environment`, `historical_start_date`,
`lookback_days` y `max_lag_days`. El bootstrap es idempotente y no ejecuta
`DROP TABLE` ni cambia tipos automáticamente.

El notebook manual de Fase 9 está en
`Automation/96_manual_phase9_review.py`. Después de desplegarlo se abre en el
Workspace como:

```text
/Workspace/Users/jgomezdelahoz2108@gmail.com/.bundle/observatorio-energetico-colombia/dev/files/Automation/96_manual_phase9_review
```

Es una revisión de solo lectura: devuelve `APTO` o `REVISAR` y no crea alertas,
no escribe tablas y no programa ejecuciones.

## Estructura principal

| Ruta | Propósito |
|---|---|
| `Ingestion/` | extracción de SIMEM y maestros |
| `Bronze_Load/` | carga Bronze canónica |
| `Silver_Load/` | normalización y MERGE por dominio |
| `GOLD LOAD/` | modelo dimensional |
| `Automation/` | job, quality gates y revisión operativa |
| `observability/` | auditoría de corridas y métricas |
| `governance/` | reglas TX, alias y reconciliación |
| `Gold_Analytics/` | vistas analíticas |
| `Serving/` | contratos estables de consumo |
| `powerbi/` | proyecto PBIP y modelo semántico |
| `config/` | configuración parametrizable |
| `docs/` | arquitectura, fases y operación |

Databricks Source `.py` es el formato canónico. Las dependencias principales
están fijadas en `pydataxm==0.3.18` y `geopy==2.5.0`.

## Documentación

- [Arquitectura actual](docs/architecture.md)
- [Inventario técnico](docs/technical_inventory.md)
- [Fase 2: observabilidad](docs/phase2_observability.md)
- [Fase 3: calidad y cuarentena](docs/phase3_data_quality.md)
- [Fase 4: gobierno](docs/phase4_governance.md)
- [Fase 5: backfill y optimización](docs/phase5_backfill_optimization.md)
- [Fase 6: consumo y operación](docs/phase6_consumption_operations.md)
- [Fase 7: dashboard Power BI](docs/phase7_powerbi_dashboard.md)
- [Fase 9: monitoreo manual y evolución](docs/phase9_monitoring_evolution.md)
- [Guion de videos y presentación LinkedIn](docs/video_script_observatorio.md)

## Próxima evolución

La Fase 10 podrá incorporar tarifas al usuario final, restricciones de red,
calidad del servicio, hidrología y pronósticos. Cada dominio deberá contar antes
con fuente oficial, permisos, grano, cobertura, dueño, reglas de calidad,
reconciliación y una vista Serving estable.

## Limitaciones conocidas

- El reporte publicado requiere permisos de Power BI.
- Tarifas, restricciones y calidad del servicio aún no tienen contratos de datos
  certificados dentro de este repositorio.
- La política TX conserva la precedencia técnica vigente; un cambio semántico
  requiere aprobación funcional del dueño del dato.
