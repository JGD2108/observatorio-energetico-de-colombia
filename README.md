# Observatorio Energético de Colombia

Pipeline Lakehouse en Databricks para integrar y analizar datos del sistema
eléctrico colombiano. Usa arquitectura Medallion, Delta Lake, Unity Catalog y
un modelo dimensional con cinco hechos.

> Fases 1 a 6 certificadas mediante ejecuciones completas en Databricks:
> pipeline reproducible, observabilidad, calidad, gobierno, backfill y Serving.

## Flujo

```text
Audit Start → SIMEM / maestros → Landing → Bronze → Silver → Gold
           → Governance Gate → Quality Gate → Analytics → Audit Finalize
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
| `governance/` | Políticas TX y funciones compartidas de gobierno |
| `Gold_Analytics/` | Vistas analíticas certificadas |
| `Serving/` | Contratos estables de negocio y operación |
| `powerbi/` | Proyecto Power BI y modelo semántico de la Fase 7 |
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
- [Fase 4: gobierno Silver y Gold](docs/phase4_governance.md)
- [Fase 5: backfill y optimización](docs/phase5_backfill_optimization.md)
- [Fase 6: consumo y operación](docs/phase6_consumption_operations.md)
- [Fase 7: dashboard Power BI](docs/phase7_powerbi_dashboard.md)
- [Clasificación de archivos](docs/file_classification.md)

## Limitaciones

- Landing todavía usa nombres fijos.
- La cuarentena de Fase 3 conserva excepciones agregadas por regla; el rechazo
  físico fila a fila durante Silver se ampliará cuando se gobiernen sus contratos.
- La política TX conserva la precedencia técnica anterior; cualquier cambio de
  semántica requiere aprobación funcional del responsable del dato.
- El PBIP requiere autenticación interactiva del propietario para actualizar y
  publicar el modelo en Power BI Service.
- Tarifas y calidad del servicio siguen pendientes de contratos certificados.
