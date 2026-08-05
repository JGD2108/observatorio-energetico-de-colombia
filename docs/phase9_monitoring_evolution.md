# Fase 9: monitoreo manual y evolución del Observatorio

## Objetivo

Mantener el dashboard confiable después de su publicación y preparar nuevas
capacidades de mercado sin introducir alertas automáticas ni consumir fuentes
sin contrato certificado.

La Fase 9 no cambia las reglas de calidad existentes, no escribe tablas y no
envía notificaciones. Reutiliza los contratos Serving y técnicos ya aprobados.

## Revisión manual de salud

El notebook `Automation/96_manual_phase9_review.py` es de solo lectura. En
Databricks, ejecútalo manualmente desde **Workspace > Automation >
96_manual_phase9_review** después de desplegar el bundle. Muestra:

| Revisión | Fuente | Decisión |
|---|---|---|
| Frescura | `serving.estado_fuentes` | `REVISAR` si una fuente incumple el SLA |
| Última corrida | `serving_technical.pipeline_health` | `REVISAR` si la última corrida no fue `SUCCESS` |
| Calidad bloqueante | `serving_technical.quality_alerts` | `REVISAR` si hay alertas `HIGH` o `CRITICAL` abiertas |
| Contratos de consumo | nueve vistas Serving/técnicas | `REVISAR` si falta una vista |
| Rendimiento | `serving_technical.task_performance` | revisar las diez tareas con mayor p95 |

El resultado `APTO` significa que los controles operativos disponibles no
encontraron un bloqueo. `REVISAR` no modifica el pipeline ni el dashboard: se
debe investigar la fuente, tarea o regla que el notebook detalla.

Cadencia recomendada, sin alertas automáticas:

1. Revisar después de un cambio de código, backfill o incidente de SIMEM.
2. Revisar semanalmente antes de compartir cifras de mercado.
3. Registrar una decisión sólo si el resultado es `REVISAR`: causa, responsable,
   acción y fecha de nueva revisión.

## Gestión de cambios del dashboard

Todo cambio de visual, medida o fuente debe cumplir estos mínimos antes de
publicarse:

1. La medida declara grano, unidad, ventana temporal y fuente Serving.
2. Las cifras combinadas usan un corte temporal comparable o indican la
   cobertura distinta de cada fuente.
3. El PBIP pasa las pruebas y el analizador TMDL antes de abrirse en Desktop.
4. La actualización manual en Power BI Service termina correctamente antes de
   cambiar la programación.
5. Una nueva fuente se valida en Landing, Bronze, Silver, Gold y Serving; no se
   conecta Power BI directamente a una fuente externa.

## Hoja de ruta de datos de mercado

Los siguientes dominios son candidatos; no forman parte del dashboard hasta que
exista una fuente autorizada y un contrato de consumo aprobado.

| Prioridad | Dominio | Decisión que habilita | Condición para iniciar |
|---|---|---|---|
| 1 | Tarifas al usuario | comparar precio de bolsa con señales tarifarias al usuario final | fuente oficial, periodicidad, unidad y cobertura geográfica certificadas |
| 2 | Restricciones y red | explicar diferencias entre despacho, precio y disponibilidad | grano horario, códigos de activos y regla de reconciliación aprobados |
| 3 | Calidad del servicio | relacionar continuidad/calidad con territorio y operador | definición regulatoria, denominador, periodo y permisos de publicación aprobados |
| 4 | Hidrología y clima | contextualizar embalses y riesgo de oferta | estación/área, latencia, licencia y método de agregación documentados |

Para incorporar cualquiera de ellos se requiere: dueño del dato, fuente y
licencia, grano, actualización esperada, claves de unión, regla de calidad,
reconciliación con un periodo conocido y diseño de la vista Serving. Hasta ese
momento, se conserva fuera de las tarjetas y gráficos ejecutivos para no
presentar inferencias como datos certificados.
