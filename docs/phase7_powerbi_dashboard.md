# Fase 7: dashboard Power BI

La Fase 7 conecta el dashboard del Observatorio a los contratos estables de
Serving, corrige la comparabilidad temporal de los KPI y añade una experiencia
técnica para operar el pipeline. El PBIP versionado está en
`powerbi/Dashboard_observatorio/`; el archivo original de OneDrive no se modifica.

## Arquitectura de consumo

```text
Gold validado
  -> observatorio_dev.serving
       -> resumen del sistema
       -> análisis por planta
  -> observatorio_dev.serving_technical
       -> operación técnica
  -> modelo semántico Power BI (Import)
  -> tres páginas del reporte
```

Power BI consume estas vistas estables:

| Capa | Vista | Uso principal |
|---|---|---|
| negocio | `kpi_sistema_diario` | generación, demanda, disponibilidad y precio |
| negocio | `operacion_planta_diaria` | desempeño y ranking de plantas |
| negocio | `generacion_tecnologia_diaria` | mezcla y utilización por tecnología |
| negocio | `estado_fuentes` | frescura y cumplimiento del SLA |
| técnica | `pipeline_health` | estado, duración y éxito de ejecuciones |
| técnica | `task_performance` | rendimiento de tareas en 30 días |
| técnica | `quality_alerts` | alertas abiertas e historial de calidad |

## Criterio de comparabilidad

Los KPI del resumen sólo acumulan días hasta `fecha_corte_comparable`: la última
fecha con generación, demanda, disponibilidad y precio presentes y 24 horas de
datos. Esto evita comparar totales calculados con fechas de cierre diferentes.
La fecha máxima individual de cada fuente continúa visible en la tabla de
actualización, por lo que frescura y comparabilidad no se confunden.

## Páginas

1. **Resumen del sistema eléctrico**: KPI ejecutivos, generación contra demanda,
   precio de bolsa y frescura de fuentes.
2. **Análisis por planta**: generación, capacidad, disponibilidad, factor de
   capacidad, ranking y comparación por tecnología. El segmentador de tecnología
   usa `DimTecnologia` y filtra coherentemente las dos tablas de hechos.
3. **Operación técnica**: estado y duración de la última ejecución, tasa de éxito
   reciente, alertas abiertas, duración histórica, tareas lentas y detalle de
   alertas.

## Abrir y actualizar localmente

1. Desplegar el bundle y publicar las vistas Serving:

   ```powershell
   databricks bundle deploy -t dev
   databricks bundle run observatorio_daily_pipeline -t dev --only serving_publish
   ```

2. Abrir `powerbi/Dashboard_observatorio/Dashboard observatorio_dev.pbip` con
   Power BI Desktop.
3. Si Power BI lo solicita, iniciar sesión en Databricks para el servidor
   `dbc-4c2404fa-9e73.cloud.databricks.com` y seleccionar autenticación OAuth.
4. Elegir **Inicio > Actualizar** y esperar a que termine el modelo completo.
5. Verificar las tres páginas y guardar. Este primer guardado también permite a
   Power BI normalizar los metadatos TMDL heredados del archivo original.

## Publicar en Power BI Service

Desde Power BI Desktop usar **Inicio > Publicar**, seleccionar el workspace y
configurar las credenciales Databricks del modelo semántico en Power BI Service.
Programar la actualización después del job diario de Databricks; se recomienda
una separación mínima de 30 minutos. La publicación requiere la sesión y el
workspace del propietario, por eso no se automatiza desde el repositorio.

## Alcance pendiente

La Fase 7 cubre operación, mercado, disponibilidad, plantas y salud técnica. Los
indicadores de tarifas al usuario y calidad del servicio no se muestran porque
todavía no existen contratos de datos certificados para esos dominios.
