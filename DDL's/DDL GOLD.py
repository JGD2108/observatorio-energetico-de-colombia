# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Contrato Gold no destructivo, parametrizado por `setup/00_bootstrap.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC # DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Fecha

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.dim_fecha (
# MAGIC     fecha_key INT NOT NULL
# MAGIC         COMMENT 'Clave determinística de fecha en formato YYYYMMDD',
# MAGIC
# MAGIC     fecha DATE NOT NULL
# MAGIC         COMMENT 'Fecha calendario',
# MAGIC
# MAGIC     anio SMALLINT NOT NULL,
# MAGIC     semestre TINYINT NOT NULL,
# MAGIC     trimestre TINYINT NOT NULL,
# MAGIC
# MAGIC     mes_numero TINYINT NOT NULL,
# MAGIC     mes_nombre STRING NOT NULL,
# MAGIC     mes_nombre_corto STRING NOT NULL,
# MAGIC
# MAGIC     anio_mes INT NOT NULL
# MAGIC         COMMENT 'Identificador en formato YYYYMM',
# MAGIC
# MAGIC     anio_mes_nombre STRING NOT NULL,
# MAGIC
# MAGIC     semana_anio TINYINT NOT NULL,
# MAGIC     dia_anio SMALLINT NOT NULL,
# MAGIC     dia_mes TINYINT NOT NULL,
# MAGIC
# MAGIC     dia_semana_numero TINYINT NOT NULL
# MAGIC         COMMENT 'Día ISO: lunes 1 a domingo 7',
# MAGIC
# MAGIC     dia_semana_nombre STRING NOT NULL,
# MAGIC
# MAGIC     es_fin_semana BOOLEAN NOT NULL,
# MAGIC     es_inicio_mes BOOLEAN NOT NULL,
# MAGIC     es_fin_mes BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión calendario conformada del observatorio energético'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Periodo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.dim_periodo (
# MAGIC     periodo_key TINYINT NOT NULL
# MAGIC         COMMENT 'Clave del periodo horario entre 1 y 24',
# MAGIC
# MAGIC     numero_periodo TINYINT NOT NULL,
# MAGIC     hora_inicio TINYINT NOT NULL,
# MAGIC     hora_fin TINYINT NOT NULL,
# MAGIC
# MAGIC     hora_inicio_etiqueta STRING NOT NULL,
# MAGIC     hora_fin_etiqueta STRING NOT NULL,
# MAGIC
# MAGIC     periodo_etiqueta STRING NOT NULL,
# MAGIC     rango_horario STRING NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión conformada de los 24 periodos horarios'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Agente

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.dim_agente (
# MAGIC     agente_key BIGINT
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Clave sustituta de la versión histórica del agente',
# MAGIC
# MAGIC     codigo_agente STRING NOT NULL
# MAGIC         COMMENT 'Código natural del agente',
# MAGIC
# MAGIC     nombre_agente STRING NOT NULL,
# MAGIC     nombre_agente_normalizado STRING NOT NULL,
# MAGIC
# MAGIC     actividad_agente STRING NOT NULL,
# MAGIC     actividad_normalizada STRING NOT NULL,
# MAGIC
# MAGIC     numero_version INT NOT NULL,
# MAGIC
# MAGIC     fecha_inicio DATE NOT NULL
# MAGIC         COMMENT 'Inicio inclusivo de vigencia',
# MAGIC
# MAGIC     fecha_fin DATE NOT NULL
# MAGIC         COMMENT 'Fin inclusivo de vigencia',
# MAGIC
# MAGIC     es_actual BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión SCD Tipo 2 de agentes del mercado energético'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Planta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.dim_planta (
# MAGIC     planta_key BIGINT
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Clave sustituta estable de la planta o recurso',
# MAGIC
# MAGIC     codigo_planta STRING NOT NULL
# MAGIC         COMMENT 'Código natural de planta o recurso',
# MAGIC
# MAGIC     nombre_planta STRING NOT NULL
# MAGIC         COMMENT 'Nombre oficial o nombre provisional del recurso inferido',
# MAGIC
# MAGIC     codigo_sic_agente STRING
# MAGIC         COMMENT 'Código SIC del agente asociado en el maestro vigente',
# MAGIC
# MAGIC     cap_efectiva_neta DECIMAL(24,6)
# MAGIC         COMMENT 'Capacidad efectiva neta reportada por la fuente',
# MAGIC
# MAGIC     fpo DATE
# MAGIC         COMMENT 'Fecha de puesta en operación',
# MAGIC
# MAGIC     codigo_sub_area_operativa STRING,
# MAGIC     codigo_area_operativa STRING,
# MAGIC
# MAGIC     tipo_despacho_recurso STRING,
# MAGIC     tipo_clasificacion STRING,
# MAGIC     tipo_generacion STRING,
# MAGIC
# MAGIC     es_registro_inferido BOOLEAN NOT NULL
# MAGIC         COMMENT 'Indica si el recurso fue creado desde una fuente operativa por ausencia en el maestro',
# MAGIC
# MAGIC     origen_registro STRING NOT NULL
# MAGIC         COMMENT 'Fuente que originó inicialmente el miembro',
# MAGIC
# MAGIC     esta_en_maestro_actual BOOLEAN NOT NULL
# MAGIC         COMMENT 'Indica si el código existe actualmente en silver.plantas',
# MAGIC
# MAGIC     fecha_primera_observacion DATE
# MAGIC         COMMENT 'Primera fecha observada en maestro o fuentes operativas',
# MAGIC
# MAGIC     fecha_ultima_observacion DATE
# MAGIC         COMMENT 'Última fecha observada en maestro o fuentes operativas',
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión vigente Tipo 1 de plantas y recursos, incluyendo miembros inferidos'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Embalse

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.dim_embalse (
# MAGIC     embalse_key BIGINT
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Clave sustituta estable del embalse',
# MAGIC
# MAGIC     codigo_embalse STRING NOT NULL,
# MAGIC     nombre_embalse STRING NOT NULL,
# MAGIC     nombre_embalse_normalizado STRING NOT NULL,
# MAGIC
# MAGIC     latitud DECIMAL(10,7),
# MAGIC     longitud DECIMAL(11,7),
# MAGIC
# MAGIC     tipo_coordenada STRING,
# MAGIC     fuente_coordenada STRING,
# MAGIC     estado_geocodificacion STRING,
# MAGIC     consulta_geocodificacion STRING,
# MAGIC
# MAGIC     coordenadas_validas BOOLEAN NOT NULL,
# MAGIC     requiere_revision_manual BOOLEAN NOT NULL,
# MAGIC
# MAGIC     source_file_name STRING,
# MAGIC     source_file_path STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     silver_load_date DATE,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión Tipo 1 de embalses, coordenadas y trazabilidad geográfica'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Bridge Table

# COMMAND ----------

# MAGIC %md
# MAGIC ###Planta-Embalse

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.bridge_planta_embalse (
# MAGIC     planta_embalse_key STRING NOT NULL
# MAGIC         COMMENT 'Clave SHA-256 de la relación planta-embalse',
# MAGIC
# MAGIC     planta_key BIGINT NOT NULL,
# MAGIC     embalse_key BIGINT NOT NULL,
# MAGIC
# MAGIC     codigo_planta STRING NOT NULL,
# MAGIC     codigo_embalse STRING NOT NULL,
# MAGIC
# MAGIC     region STRING,
# MAGIC
# MAGIC     nombre_planta_fuente STRING,
# MAGIC     nombre_reservorio_fuente STRING,
# MAGIC
# MAGIC     tipo_relacion STRING,
# MAGIC     es_principal BOOLEAN NOT NULL,
# MAGIC     permite_atribucion BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fuente_relacion STRING,
# MAGIC     estado_validacion STRING,
# MAGIC
# MAGIC     valido_desde DATE,
# MAGIC     valido_hasta DATE,
# MAGIC
# MAGIC     cantidad_embalses_planta INT NOT NULL,
# MAGIC
# MAGIC     es_relacion_unica BOOLEAN NOT NULL
# MAGIC         COMMENT 'Indica si la planta está vinculada con un solo embalse',
# MAGIC
# MAGIC     requiere_revision_manual BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Puente gobernado entre plantas y embalses'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # FACTS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Generacion Real

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.fact_generacion_real (
# MAGIC     generacion_key STRING NOT NULL
# MAGIC         COMMENT 'Clave determinística de la medición consolidada',
# MAGIC
# MAGIC     fecha_key INT NOT NULL,
# MAGIC     periodo_key TINYINT NOT NULL,
# MAGIC
# MAGIC     planta_key BIGINT NOT NULL,
# MAGIC     agente_key BIGINT NOT NULL,
# MAGIC
# MAGIC     fecha_hora TIMESTAMP NOT NULL,
# MAGIC
# MAGIC     generacion_real_kwh DECIMAL(24,6) NOT NULL,
# MAGIC
# MAGIC     version_seleccionada STRING NOT NULL,
# MAGIC     prioridad_version INT NOT NULL,
# MAGIC
# MAGIC     planta_provenia_de_maestro BOOLEAN NOT NULL
# MAGIC         COMMENT 'Indicador de calidad heredado de Silver antes de resolver miembros inferidos',
# MAGIC
# MAGIC     agente_encontrado_silver BOOLEAN NOT NULL
# MAGIC         COMMENT 'Indica si el agente tenía correspondencia en Silver',
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Generación real horaria consolidada por planta y agente'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Disponibilidad Planta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.fact_disponibilidad_planta (
# MAGIC     disponibilidad_key STRING NOT NULL,
# MAGIC
# MAGIC     fecha_key INT NOT NULL,
# MAGIC     periodo_key TINYINT NOT NULL,
# MAGIC     planta_key BIGINT NOT NULL,
# MAGIC
# MAGIC     fecha_hora TIMESTAMP NOT NULL,
# MAGIC
# MAGIC     disponibilidad_real_kwh DECIMAL(24,6) NOT NULL,
# MAGIC
# MAGIC     version_seleccionada STRING NOT NULL,
# MAGIC     prioridad_version INT NOT NULL,
# MAGIC
# MAGIC     planta_provenia_de_maestro BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Disponibilidad real horaria consolidada por planta o recurso'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Demanda Real

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.fact_demanda_real (
# MAGIC     demanda_key STRING NOT NULL,
# MAGIC
# MAGIC     fecha_key INT NOT NULL,
# MAGIC     periodo_key TINYINT NOT NULL,
# MAGIC     agente_key BIGINT NOT NULL,
# MAGIC
# MAGIC     fecha_hora TIMESTAMP NOT NULL,
# MAGIC     tipo_mercado STRING NOT NULL,
# MAGIC
# MAGIC     demanda_real_kwh DECIMAL(24,6) NOT NULL,
# MAGIC     es_demanda_cero BOOLEAN NOT NULL,
# MAGIC
# MAGIC     version_seleccionada STRING NOT NULL,
# MAGIC     prioridad_version INT NOT NULL,
# MAGIC
# MAGIC     agente_encontrado_silver BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Demanda real horaria consolidada por agente y mercado'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Precio Bolsa

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.fact_precio_bolsa (
# MAGIC     precio_bolsa_key STRING NOT NULL,
# MAGIC
# MAGIC     fecha_key INT NOT NULL,
# MAGIC     periodo_key TINYINT NOT NULL,
# MAGIC
# MAGIC     fecha_hora TIMESTAMP NOT NULL,
# MAGIC
# MAGIC     precio_bolsa_internacional_cop_kwh DECIMAL(24,6),
# MAGIC     precio_bolsa_nacional_cop_kwh DECIMAL(24,6),
# MAGIC     precio_bolsa_tie_cop_kwh DECIMAL(24,6),
# MAGIC
# MAGIC     version_pb_int STRING,
# MAGIC     prioridad_pb_int INT,
# MAGIC
# MAGIC     version_pb_nal STRING,
# MAGIC     prioridad_pb_nal INT,
# MAGIC
# MAGIC     version_pb_tie STRING,
# MAGIC     prioridad_pb_tie INT,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Precios horarios PB_INT, PB_NAL y PB_TIE consolidados en formato ancho'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Energia embalsamada Planta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS observatorio_dev.gold.fact_energia_embalsada_planta (
# MAGIC     energia_embalsada_key STRING NOT NULL,
# MAGIC
# MAGIC     fecha_key INT NOT NULL,
# MAGIC     planta_key BIGINT NOT NULL,
# MAGIC
# MAGIC     fecha_medicion DATE NOT NULL,
# MAGIC
# MAGIC     energia_embalsada_kwh DECIMAL(24,6) NOT NULL,
# MAGIC     es_valor_cero BOOLEAN NOT NULL,
# MAGIC
# MAGIC     version_seleccionada STRING NOT NULL,
# MAGIC     prioridad_version INT NOT NULL,
# MAGIC
# MAGIC     planta_provenia_de_maestro BOOLEAN NOT NULL,
# MAGIC
# MAGIC     fecha_creacion TIMESTAMP NOT NULL,
# MAGIC     fecha_actualizacion TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Snapshot diario de energía embalsada reportada por planta'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'quality' = 'gold'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Validar objetos creados

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN observatorio_dev.gold;
