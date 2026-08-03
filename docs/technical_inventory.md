# Inventario técnico — Observatorio Energético de Colombia

## 1. Información general

- Proyecto: Observatorio Energético de Colombia
- Plataforma: Databricks
- Catálogo principal: `observatorio_dev`
- Arquitectura: Medallion
- Zona horaria: `America/Bogota`
- Ejecución diaria: 8:00 a. m.
- Ventana retrospectiva: 45 días

---

## 2. Capas

### Landing

Ruta:

`/Volumes/observatorio_dev/landing/raw_files`

Función:

Recibir los archivos obtenidos desde las fuentes externas antes de
su procesamiento en Bronze.

Estado actual:

Los archivos utilizan nombres fijos y pueden ser reemplazados por
ejecuciones posteriores.

Mejora pendiente:

Particionar Landing por fuente, fecha de ingesta y `run_id`.

---

### Bronze

Función:

Conservar los datos recibidos desde la fuente junto con metadatos
de trazabilidad.

Tablas actuales:

- `observatorio_dev.bronze.agentes`
- `observatorio_dev.bronze.plantas`
- `observatorio_dev.bronze.generacion_real`
- `observatorio_dev.bronze.demanda_real`
- `observatorio_dev.bronze.disponibilidad_plantas`
- `observatorio_dev.bronze.precio_bolsa`
- `observatorio_dev.bronze.niveles_embalses`
- `observatorio_dev.bronze.embalses`
- `observatorio_dev.bronze.plantas_reservorios`

---

### Silver

Función:

Tipificar, normalizar, deduplicar y preparar la información para
la aplicación de reglas de negocio.

Tablas actuales:

- `observatorio_dev.silver.agentes`
- `observatorio_dev.silver.plantas`
- `observatorio_dev.silver.generacion_real`
- `observatorio_dev.silver.demanda_real`
- `observatorio_dev.silver.disponibilidad_plantas`
- `observatorio_dev.silver.precio_bolsa`
- `observatorio_dev.silver.niveles_embalses`
- `observatorio_dev.silver.embalses`
- `observatorio_dev.silver.plantas_reservorios`

---

### Gold

Función:

Aplicar reglas de negocio y organizar la información mediante un
modelo dimensional.

Dimensiones actuales:

- `dim_fecha`
- `dim_periodo`
- `dim_agente`
- `dim_planta`
- `dim_embalse`

Hechos actuales:

- `fact_generacion_real`
- `fact_demanda_real`
- `fact_disponibilidad_planta`
- `fact_precio_bolsa`
- `fact_energia_embalsada_planta`

Relaciones:

- `bridge_planta_embalse`

---

## 3. Fuentes y granularidad

| Fuente | Granularidad principal | Frecuencia de carga |
|---|---|---|
| Agentes | Diario | Diaria |
| Plantas | Diario | Diaria |
| Generación real | Horaria | Diaria |
| Demanda real | Horaria | Diaria |
| Disponibilidad de plantas | Horaria | Diaria |
| Precio de bolsa | Horaria | Diaria |
| Energía embalsada | Diaria | Diaria |
| Catálogo de embalses | Maestro | Manual / pendiente |
| Relación planta-embalse | Maestro | Manual / pendiente |

---

## 4. Notebooks de ingesta

- `Ingestion/agentes.py`
- `Ingestion/Plantas.py`
- `Ingestion/generacion_real.py`
- `Ingestion/Demanda_real.py`
- `Ingestion/Disponibilidad Planta.py`
- `Ingestion/PrecioBolsa.py`
- `Ingestion/Nivel embalses dado por plantas.py`
- `Ingestion/Embalses.py`
- `Ingestion/embalse_plantas.py`

---

## 5. Notebooks Bronze

- `Bronze_Load/02_bronze_daily.py`
- `Bronze_Load/02_load_json_bronze.py`

Nota:

Debe confirmarse cuál notebook se utiliza actualmente en producción
y cuál corresponde a una carga histórica o implementación anterior.

---

## 6. Notebooks Silver

- `Silver_Load/silver_agentes.py`
- `Silver_Load/silver_plantas.py`
- `Silver_Load/silver_generacion.py`
- `Silver_Load/silver_generacionReal.py`
- `Silver_Load/silver_demanda_real.py`
- `Silver_Load/silver_disponibilidad_planta.py`
- `Silver_Load/silver_precio_bolsa.py`
- `Silver_Load/silver_nivelPlantas.py`
- `Silver_Load/silver_embalses.py`
- `Silver_Load/silver_plantas_reservorios.py`

Pendiente:

Determinar cuál de los siguientes notebooks será la implementación
oficial:

- `silver_generacion.py`
- `silver_generacionReal.py`

---

## 7. Gold

Notebook actual:

- `GOLD LOAD/GOLD_LOAD.py`

Observación:

El notebook contiene dimensiones, hechos, bridge, reglas TX y
validaciones en un único archivo.

Mejora pendiente:

Dividir progresivamente Gold por dominio o tabla.

---

## 8. Automatización

Archivo:

- `Automation/Job.yaml`

Flujo objetivo:

Ingestas → Bronze → Silver → Gold → Quality Gate → Analytics

Horario:

8:00:00 a. m., zona horaria `America/Bogota`.

---

## 9. Calidad

Notebook actual:

- `Automation/05_quality_checks.py`

Estado:

Las validaciones se ejecutan, pero sus resultados no se conservan
de forma histórica.

Mejoras pendientes:

- Crear `run_id`.
- Persistir resultados.
- Clasificar severidad.
- Crear cuarentena.
- Reconciliar Bronze, Silver y Gold.

---

## 10. Analytics

Notebook actual:

- `Gold_Analytics/01_vistas_dashboard.py`

Productos actuales:

- Vistas analíticas.
- Dashboard en Power BI.
- Aplicación web Astro.

Mejora pendiente:

Separar vistas ciudadanas de vistas técnicas.

---

## 11. Deuda técnica identificada

1. Parámetros repetidos en múltiples notebooks.
2. Landing con archivos de nombre fijo.
3. Dos notebooks Silver para generación.
4. DDL duplicado para generación.
5. Operaciones destructivas en algunos DDL.
6. Gold concentrado en un único notebook.
7. Ausencia de auditoría persistente.
8. Ausencia de cuarentena.
9. Reglas TX repetidas.
10. Diferencias de nombres en disponibilidad.
11. Backfill e incremental no están formalmente separados.
12. Algunas ingestas asumen que Bronze ya existe.