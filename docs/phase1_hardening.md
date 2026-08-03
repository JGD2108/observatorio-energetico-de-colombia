# Fase 1 — Estructura y reproducibilidad

Implementado:

- `.py` canónico y nombres compatibles con Windows.
- Rutas personales eliminadas y configuración centralizada.
- Dependencias fijadas; retirados los `!pip install`.
- Asset Bundle con targets `dev` y `prod`.
- DAG completo de 23 tareas y quality gate obligatorio.
- Bootstrap idempotente con migraciones solo aditivas.
- DDL destructivos reemplazados.
- Fallo Bronze planta–embalse corregido.
- Documentación alineada con cinco hechos y once vistas.

Validación pendiente en Databricks:

1. `bundle validate` y despliegue en catálogo vacío.
2. Ejecución completa y confirmación de los 49 controles.
3. Prueba de bloqueo de Analytics cuando falle quality.
4. Segunda ejecución sin cambios para verificar idempotencia.

La Fase 1 no certifica los KPI de Analytics; corresponde a Fase 6.
