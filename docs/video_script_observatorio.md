# Guion audiovisual del Observatorio Energético de Colombia

Este documento contiene una serie de videos cortos y un video completo para
LinkedIn. El guion está escrito para explicar el proyecto con evidencia visible
en Databricks, el repositorio y Power BI, sin exponer tokens, correos, rutas
privadas ni datos de conexión.

## Mensaje central

> Construí una plataforma reproducible que transforma datos públicos del sistema
> eléctrico colombiano en indicadores confiables para entender operación,
> mercado y energía embalsada.

La historia debe seguir este orden: problema → arquitectura → confianza del dato
→ consumo → resultado → siguiente evolución.

## Preparación antes de grabar

### Material que debe estar abierto

1. README principal del repositorio.
2. Diagrama de `docs/architecture.md`.
3. Databricks con el Job y las vistas `serving`.
4. Notebook `Automation/96_manual_phase9_review`.
5. Dashboard publicado en Power BI.
6. Una carpeta con capturas de respaldo por si falla una sesión en vivo.

### Reglas de grabación

- Ocultar tokens, claves, correos, rutas personales y nombres de workspace
  privados.
- Mostrar nombres de tablas y contratos, no credenciales.
- Explicar siempre la unidad: GWh, TWh, MW, COP/kWh o porcentaje.
- Aclarar que el enlace de Power BI requiere permisos.
- No presentar tarifas, restricciones o calidad del servicio como datos ya
  integrados; son parte de la hoja de ruta futura.
- Usar subtítulos en español y una versión corta del mensaje clave en inglés si
  se publica en LinkedIn.

## Serie de videos

### Video 1 — El problema: datos energéticos fragmentados

**Duración:** 90 segundos.

**Mostrar:** título del proyecto, una captura del dashboard y el árbol de
carpetas del repositorio.

**Decir:**

> El sistema eléctrico colombiano produce información valiosa, pero para tomar
> una decisión hay que cruzar generación, demanda, disponibilidad, precio,
> plantas, agentes y embalses. El reto no era crear una gráfica aislada: era
> asegurar que cada cifra tuviera fuente, fecha, unidad y trazabilidad.
>
> Por eso construí el Observatorio Energético de Colombia. La plataforma recibe
> datos públicos, los normaliza, valida su calidad y publica contratos estables
> para Power BI. El usuario final ve indicadores; detrás existe un pipeline que
> puede auditarse y repetirse.
>
> En esta serie mostraré cómo se construyó y por qué el dashboard no debe
> separarse de la ingeniería de datos que lo alimenta.

**Cerrar con:** “En el siguiente video: la arquitectura completa, desde SIMEM
hasta Power BI”.

### Video 2 — Arquitectura Medallion de extremo a extremo

**Duración:** 2 minutos.

**Mostrar:** diagrama `SIMEM → Landing → Bronze → Silver → Gold → Serving →
Power BI` y luego las carpetas del repo.

**Decir:**

> La arquitectura sigue un flujo Medallion en Databricks. Landing conserva la
> llegada de los archivos. Bronze guarda una representación canónica. Silver
> normaliza nombres, tipos, fechas y reglas de negocio. Gold organiza el modelo
> dimensional. Gold Analytics prepara vistas analíticas y Serving publica la
> interfaz que consume el dashboard.
>
> Cada capa tiene una responsabilidad. Esto evita que Power BI consulte
> directamente una fuente externa o una tabla temporal. También permite reparar
> una etapa sin reconstruir todo el proyecto.
>
> El modelo final contiene cinco hechos, cinco dimensiones y un bridge entre
> plantas y embalses. La auditoría mantiene el `run_id`, las tareas y las
> métricas de cada ejecución.

**Mostrar en pantalla:** `config/project_config.py`, `Serving/01_publish_serving.py`
y una vista del Job, sin mostrar valores sensibles.

### Video 3 — Ingesta incremental y backfill controlado

**Duración:** 2 minutos.

**Mostrar:** parámetros `execution_mode`, `backfill_start_date`,
`backfill_end_date`, `backfill_chunk_days` y el flujo del Job.

**Decir:**

> Hay dos necesidades distintas. La operación diaria debe reprocesar una ventana
> controlada porque algunas fuentes publican correcciones. El backfill debe
> recuperar una ventana histórica explícita y dejar evidencia de cobertura.
>
> El proyecto separa ambos modos con parámetros. La carga incremental usa
> MERGE idempotente y el backfill registra `backfill_id`, fechas y cobertura. Si
> una descarga externa falla, el sistema reintenta con límites y no publica una
> falsa completitud.
>
> La optimización se hizo sin cache, persistencia ni memoria temporal. Se
> redujeron acciones repetidas y se conservaron las consultas necesarias para
> validar y escribir cada etapa.

**Mostrar:** una ejecución verde y la tabla de cobertura; no mostrar URLs con
tokens ni payloads completos de la API.

### Video 4 — Calidad, gobierno y trazabilidad

**Duración:** 2 minutos.

**Mostrar:** `docs/phase3_data_quality.md`, `docs/phase4_governance.md`, vistas
`quality_alerts` y `estado_fuentes`.

**Decir:**

> Una cifra puede tener formato correcto y aun así ser incorrecta. Por eso el
> pipeline valida esquema, grano, claves, cobertura horaria, frescura,
> relaciones y reglas de negocio.
>
> Los fallos se clasifican por severidad. Un error bloqueante no debe llegar al
> dashboard como si fuera un dato normal. Las excepciones quedan en cuarentena y
> se relacionan con el mismo `run_id` de la ejecución.
>
> El gobierno centraliza reglas TX, alias, versiones SCD2 y la relación entre
> plantas y embalses. Esto evita que una prioridad de negocio quede escondida en
> un notebook o en una medida de Power BI.

**Frase clave:** “La calidad no es una página del dashboard; es una condición
para que el dashboard sea confiable”.

### Video 5 — Gold y Serving: convertir datos en contratos

**Duración:** 2 minutos.

**Mostrar:** catálogo `observatorio_dev.serving` y las nueve vistas de contrato.

**Decir:**

> Gold representa el negocio; Serving representa cómo otros sistemas lo
> consumen. Esa separación permite cambiar una consulta interna sin romper el
> modelo semántico.
>
> Los contratos de negocio cubren KPI diarios, plantas, demanda por mercado,
> energía embalsada, fuentes y tecnología. Los contratos técnicos cubren salud
> del pipeline, rendimiento de tareas y calidad.
>
> Cada vista tiene un grano y una finalidad. Por ejemplo, la demanda por mercado
> es diaria y separa tipos de mercado; la vista de embalses permite observar
> nivel, variación y cobertura de asignación. Esa definición es parte del
> producto, no un detalle invisible de implementación.

### Video 6 — El dashboard para tomar decisiones

**Duración:** 2 minutos.

**Mostrar:** las cinco páginas del dashboard, en este orden:

1. Resumen del sistema.
2. Análisis por planta.
3. Operación técnica.
4. Demanda y mercado.
5. Embalses y cobertura.

**Decir:**

> La primera página responde qué está ocurriendo en el sistema: generación,
> demanda, disponibilidad, precio y frescura.
>
> La segunda explica qué plantas y tecnologías contribuyen al resultado. La
> tercera muestra si el pipeline está sano antes de confiar en una cifra.
>
> La cuarta separa demanda por mercado y permite estudiar promedios, picos y
> cobertura. La quinta ayuda a seguir la energía embalsada y la calidad de su
> asignación.
>
> Los filtros de fecha comparten una dimensión común. Los KPI combinados usan un
> corte comparable para no sumar fuentes que terminan en días diferentes.

**Demostrar:** cambiar el periodo, seleccionar una tecnología y volver a “Todas”;
mostrar que las tarjetas y gráficos responden coherentemente.

### Video 7 — Operación manual después de publicar

**Duración:** 90 segundos.

**Mostrar:** `Automation/96_manual_phase9_review` y su salida `APTO`.

**Decir:**

> Después de publicar el dashboard no termina la responsabilidad. La Fase 9
> incorpora una revisión manual de solo lectura. Comprueba frescura de fuentes,
> última corrida, calidad bloqueante, contratos Serving y rendimiento de tareas.
>
> Si el resultado es `APTO`, no se encontraron bloqueos operativos en la revisión.
> Si es `REVISAR`, el notebook detalla qué fuente, tarea o control necesita
> investigación. No crea alertas automáticas ni escribe tablas; deja la decisión
> en manos del responsable operativo.

**Mostrar:** el resultado `REVISION FASE 9: APTO - Sin hallazgos operativos`.

### Video 8 — Resultado y hoja de ruta

**Duración:** 2 minutos.

**Mostrar:** README, lista de fases y sección de próxima evolución.

**Decir:**

> El resultado es una plataforma que une ingeniería, gobierno y analítica. Las
> fases completadas cubren ingesta, calidad, backfill, Serving, Power BI y
> operación manual.
>
> El siguiente paso no es agregar tarjetas por agregar. La Fase 10 deberá
> incorporar dominios que respondan decisiones reales: tarifas al usuario,
> restricciones de red, calidad del servicio, hidrología y pronósticos.
>
> Cada nueva fuente tendrá que demostrar propietario, permisos, grano,
> cobertura, calidad, reconciliación y un contrato Serving antes de aparecer en
> el dashboard.

**Cerrar con:** “Un dashboard confiable comienza mucho antes de la visualización”.

## Video completo para LinkedIn

**Duración objetivo:** 4 a 6 minutos.

**Formato:** 16:9, subtítulos, cortes cada 8–12 segundos, voz clara y capturas
de pantalla con zoom suave. No mostrar credenciales.

### Guion narrado con tiempos

**0:00–0:25 — Gancho**

> ¿Cómo pasar de datos públicos del sistema eléctrico colombiano a una decisión
> confiable? No basta con poner cuatro tarjetas en Power BI. Hay que controlar
> origen, fecha, grano, calidad y operación. Ese fue el objetivo del Observatorio
> Energético de Colombia.

**Mostrar:** portada del README y dashboard.

**0:25–1:10 — Problema y objetivo**

> El proyecto integra generación, demanda, disponibilidad, precio de bolsa,
> plantas, agentes, tecnología y energía embalsada. La pregunta no es solamente
> cuánto ocurrió, sino si la cifra está completa, si las fuentes terminan en la
> misma fecha y si el pipeline que la produce terminó correctamente.

**Mostrar:** KPI de resumen y tabla de actualización de fuentes.

**1:10–2:00 — Arquitectura**

> La solución usa Databricks, Delta Lake y Unity Catalog. Los datos pasan por
> Landing, Bronze, Silver y Gold. Luego se publican vistas Gold Analytics y
> contratos Serving para Power BI. La auditoría conserva el run_id, tareas,
> métricas y resultados de calidad.

**Mostrar:** diagrama de arquitectura y carpetas del repositorio.

**2:00–2:45 — Calidad y gobierno**

> El pipeline valida esquema, grano, cobertura horaria, frescura, claves y
> relaciones. Las reglas TX, alias, SCD2 y el bridge planta-embalse están
> gobernados. Los problemas bloqueantes no deben convertirse en KPI ejecutivos.

**Mostrar:** vista técnica, calidad y contrato `estado_fuentes`.

**2:45–3:45 — Dashboard**

> El dashboard tiene cinco páginas. El resumen muestra el estado del sistema. El
> análisis por planta explica contribuciones. La operación técnica verifica la
> salud del pipeline. Demanda y mercado muestra picos y cobertura. Embalses y
> cobertura sigue la reserva energética y la trazabilidad del dato.

**Mostrar:** recorrido de las cinco páginas y cambio de filtros.

**3:45–4:25 — Operación**

> Después de publicar, la Fase 9 ejecuta una revisión manual de solo lectura. El
> resultado actual es APTO: fuentes dentro del SLA, última corrida correcta,
> contratos disponibles y sin bloqueos operativos. No activé alertas automáticas;
> la revisión queda bajo control del responsable.

**Mostrar:** notebook y salida `APTO`.

**4:25–5:10 — Próximos pasos**

> La siguiente evolución incorporará tarifas al usuario, restricciones de red,
> calidad del servicio, hidrología y pronósticos, pero solo cuando cada dominio
> tenga fuente oficial, permisos, grano, cobertura, reglas de calidad y contrato
> Serving. La prioridad es crecer sin perder confianza.

**Mostrar:** tabla de roadmap del README.

**5:10–5:30 — Cierre para LinkedIn**

> Este proyecto demuestra que la analítica energética no empieza en el gráfico.
> Empieza en una arquitectura reproducible, continúa con datos gobernados y
> termina en una decisión que puede explicarse. El Observatorio Energético de
> Colombia ya está publicado en Power BI.

**Mostrar:** dashboard, repositorio y llamada a la acción.

### Texto de publicación para LinkedIn

> Presento el Observatorio Energético de Colombia: una plataforma en Databricks
> que transforma datos públicos del sistema eléctrico en contratos analíticos y
> un dashboard de Power BI para operación, mercado y energía embalsada.
>
> El proyecto integra arquitectura Medallion, calidad, gobierno, backfill,
> auditoría y Serving. La idea central es simple: una visualización solo es útil
> cuando se puede explicar de dónde viene, qué fecha representa y si el pipeline
> que la produce está sano.
>
> Próximo reto: incorporar tarifas al usuario, restricciones de red, calidad del
> servicio, hidrología y pronósticos con fuentes certificadas.
>
> #DataEngineering #Databricks #PowerBI #DataQuality #EnergyAnalytics
