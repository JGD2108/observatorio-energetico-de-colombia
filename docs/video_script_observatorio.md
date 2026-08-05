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

## Video completo profesional para LinkedIn y portafolio

**Duración objetivo:** 6 minutos y 45 segundos.

**Formato:** 16:9, subtítulos, voz directa y cortes visuales cada 8–12 segundos.
Usar zoom únicamente para señalar evidencia y ocultar credenciales, identificadores
personales o configuraciones sensibles.

**Tesis del video:** no construí solamente un dashboard; diseñé un producto de
datos auditable que conecta decisiones del sector eléctrico con arquitectura,
calidad, gobierno y operación.

**Qué debe reconocer cada audiencia:**

- **Reclutadores y líderes de datos:** criterio de arquitectura, modelado,
  confiabilidad, ownership y capacidad de llevar una solución a producción.
- **Profesionales y clientes del sector energético:** preguntas de negocio,
  unidades, cobertura temporal, comparabilidad y límites de interpretación.
- **Observadores no técnicos:** por qué una cifra confiable requiere mucho más
  que una gráfica atractiva.

### Guion narrado con tiempos y decisiones

**0:00–0:30 — Propuesta de valor**

**Decisión que debe quedar clara:** construir un producto auditable, no una
visualización aislada.

> ¿Cómo convertir datos públicos del sistema eléctrico colombiano en decisiones
> que un analista, un cliente o un líder pueda defender? Mi respuesta fue el
> Observatorio Energético de Colombia: una plataforma que conecta ingeniería de
> datos, gobierno y analítica para explicar generación, demanda, disponibilidad,
> precio de bolsa y reserva energética. El resultado visible es Power BI, pero la
> confianza se construye mucho antes del dashboard.

**Mostrar:** portada del proyecto y vista general del dashboard. Mantener en
pantalla el título “De datos públicos a decisiones defendibles”.

**0:30–1:15 — Alcance y criterio de producto**

**Decisión que debe quedar clara:** cada dominio entra solamente cuando puede
soportar una pregunta concreta y una interpretación responsable.

> Primero definí qué decisión podía soportar cada dato. Para el alcance actual
> integré operación del sistema, mercado y energía embalsada: generación y
> demanda para observar balance; disponibilidad y desempeño por planta para
> estudiar utilización; precio de bolsa para seguir señales de mercado; y
> embalses para entender la reserva energética. También hice explícito lo que el
> producto todavía no responde: tarifas al usuario, restricciones de red,
> calidad del servicio y pronósticos. Prefiero declarar un límite antes que
> publicar una conclusión sin fuente, cobertura o reconciliación suficiente.

**Mostrar:** resumen del sistema, frescura por fuente y las páginas de demanda y
embalses. Señalar que las fechas máximas pueden diferir entre fuentes y que los
KPI combinados usan un corte comparable.

**1:15–2:20 — Arquitectura Medallion y trade-offs**

**Decisión que debe quedar clara:** separar responsabilidades para poder reparar,
evolucionar y volver a publicar sin romper a los consumidores.

> Diseñé una arquitectura Medallion sobre Databricks, Delta Lake y Unity Catalog.
> Landing conserva la respuesta recibida y su evidencia. Bronze normaliza la
> persistencia técnica sin inventar significado de negocio. Silver limpia,
> tipifica y resuelve entidades. Gold organiza el dominio en cinco tablas de
> hechos, cinco dimensiones y un bridge planta–embalse. Finalmente, Serving
> publica contratos estables para Power BI. Elegí más capas y controles porque
> el beneficio es trazabilidad, reparabilidad y desacoplamiento: una corrección
> interna no tiene que romper el reporte. Cada ejecución conserva `run_id`,
> tareas, métricas y resultados de calidad en Unity Catalog.

**Mostrar:** la diapositiva Medallion completa. Recorrer visualmente
`FUENTES → LANDING → BRONZE → SILVER → GOLD → SERVING → POWER BI` y después los
controles transversales: catálogo, auditoría, calidad bloqueante y modos de carga.

**1:55–2:20 — Decisiones de ingestión dentro de la misma diapositiva**

> Para el día a día uso carga incremental; para reconstruir historia uso un
> backfill controlado y auditable. Las llamadas HTTP tienen reintentos limitados,
> la cobertura se valida explícitamente y las escrituras Delta usan operaciones
> idempotentes para que repetir una ventana no duplique resultados. Las
> optimizaciones se concentraron en filtros, proyecciones, agregaciones y joins,
> sin depender de caché ni de persistencia en memoria.

**2:20–3:25 — Calidad, identidad y gobierno**

**Decisión que debe quedar clara:** la calidad es una puerta de publicación; un
fallo bloqueante nunca debe convertirse en KPI ejecutivo.

> Un dato puede tener el tipo correcto y aun así representar mal el negocio. Por
> eso el pipeline valida esquema, grano, duplicados, cobertura horaria, frescura,
> relaciones y reconciliaciones antes de publicar. Normalicé códigos TX para
> evitar categorías equivalentes con nombres distintos; usé dimensiones SCD2
> para conservar historia; miembros inferidos para no perder hechos tempranos; y
> un bridge para representar la relación real entre plantas y embalses. En la
> ejecución validada se corrieron 62 controles: cero fallidos y cero bloqueantes.
> Ese número no reemplaza el criterio profesional, pero sí demuestra que la
> publicación pasó por controles reproducibles.

**Mostrar:** evidencia de calidad `62 / 0 / 0`, resultado del job en verde y un
detalle breve de gobierno o relaciones. No recorrer código línea por línea.

**3:25–4:35 — Del modelo Gold al producto de decisión**

**Decisión que debe quedar clara:** Gold expresa el negocio y Serving protege el
contrato que consumen las herramientas externas.

> Sobre el modelo Gold publiqué contratos Serving orientados a consumo. Esta
> separación permite evolucionar cálculos internos sin cambiar inesperadamente
> nombres, tipos o granularidad en Power BI. El dashboard resume el sistema,
> compara generación y demanda, analiza desempeño por planta, muestra salud
> operativa, separa demanda regulada y no regulada y sigue energía embalsada. Las
> cinco páginas comparten una dimensión de fecha y los indicadores cruzados usan
> el último corte común disponible. Así, el diseño visual no solo muestra cifras:
> ayuda a distinguir estado, causa, frescura y capacidad de acción.

**Mostrar:** recorrido de las cinco páginas. En cada una formular una pregunta:
“¿cómo está el sistema?”, “¿qué planta o tecnología explica el resultado?”,
“¿está sano el pipeline?”, “¿cómo se comporta la demanda?” y “¿cómo evoluciona
la reserva energética?”. Cambiar un filtro para demostrar interacción real.

**4:35–5:30 — Operación responsable**

**Decisión que debe quedar clara:** automatizar también significa saber cuándo
reintentar, cuándo bloquear y cuándo solicitar revisión humana.

> El workflow orquesta catálogo, auditoría, ingestión, Bronze, Silver, Gold,
> calidad, Serving y cierre operativo. Los reintentos son acotados para no
> convertir una falla externa en tráfico indefinido. El backfill verifica
> cobertura antes de aprobarse y la revisión de Fase 9 es de solo lectura. Como
> decidí no activar alertas automáticas, el control devuelve APTO o REVISAR con
> evidencia para que el responsable actúe. El estado validado fue APTO, sin
> hallazgos operativos.

**Mostrar:** workflow completo y salida `REVISION FASE 9: APTO`. Señalar que un
pipeline verde es evidencia operativa, no una garantía absoluta del mercado.

**5:30–6:45 — Resultado, límites y siguiente decisión**

**Decisión que debe quedar clara:** crecer solo cuando una nueva fuente tenga
propietario, permisos, grano, cobertura, calidad, reconciliación y contrato.

> El resultado es una solución reproducible de extremo a extremo: datos públicos,
> arquitectura Medallion, modelo dimensional, 62 validaciones aprobadas, contratos
> de consumo, cinco páginas analíticas y una revisión operativa APTO. También es
> una solución honesta sobre sus límites: no atribuye causalidad donde solo hay
> correlación y no presenta como disponible un dominio que aún no está certificado.
> La siguiente evolución prioriza tarifas, red, calidad del servicio, hidrología y
> pronósticos, cada uno con el mismo estándar de trazabilidad. Este proyecto muestra
> cómo trabajo: diseño la arquitectura, implemento el pipeline, valido el dato y
> comunico la decisión. Si su equipo necesita convertir datos energéticos en un
> producto confiable, estaré encantado de conversar.

**Mostrar:** roadmap, repositorio, dashboard publicado y cierre con datos de
contacto profesional. Mantener cinco segundos finales sin movimiento.

### Lista de grabación del video completo

1. Grabar primero la voz definitiva y luego ajustar los cortes a sus pausas.
2. Capturar arquitectura, job, calidad y dashboard en 1920×1080 o superior.
3. Usar una sola evidencia principal por diapositiva; no superponer párrafos a
   capturas.
4. Resaltar con cursor o zoom únicamente el elemento que se está explicando.
5. Añadir subtítulos revisados manualmente para `run_id`, SCD2, Delta, Serving y
   nombres del sector.
6. Mantener visibles las unidades: MW, GWh, TWh y COP/kWh.
7. Cerrar con GitHub, Power BI y una invitación concreta a conversar.

### Texto de publicación para LinkedIn

> Presento el Observatorio Energético de Colombia, un producto de datos que
> transforma información pública del sistema eléctrico en decisiones trazables.
>
> Lo construí de extremo a extremo: Databricks y Delta Lake, arquitectura
> Medallion, gobierno en Unity Catalog, modelo dimensional, cargas incrementales
> y backfill, controles de calidad bloqueantes, contratos Serving y un dashboard
> de Power BI con cinco perspectivas del sistema.
>
> La ejecución validada completó 62 controles con cero fallos y la revisión
> operativa concluyó APTO. Igual de importante: el producto declara sus límites y
> no publica nuevos dominios sin fuente, cobertura, reconciliación y contrato.
>
> Este proyecto refleja cómo abordo la analítica energética: conectando contexto
> de negocio, ingeniería confiable y comunicación clara. Si trabajas en datos,
> energía o transformación analítica, conversemos.
>
> #DataEngineering #Databricks #PowerBI #DataQuality #EnergyAnalytics
