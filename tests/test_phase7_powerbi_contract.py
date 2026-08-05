import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POWERBI = ROOT / "powerbi" / "Dashboard_observatorio"
MODEL = POWERBI / "Dashboard observatorio_dev.SemanticModel" / "definition"
REPORT = POWERBI / "Dashboard observatorio_dev.Report" / "definition"
TECHNICAL_PAGE = "f7c7a0e1202608050001"
MARKET_PAGE = "f7c7a0e1202608050002"
RESERVOIR_PAGE = "f7c7a0e1202608050003"


def test_powerbi_json_files_are_valid():
    files = list(POWERBI.rglob("*.json"))
    assert files
    for path in files:
        json.loads(path.read_text(encoding="utf-8-sig"))


def test_business_tables_use_stable_serving_contracts():
    expected = {
        "vw_resumen_diario_sistema.tmdl": "kpi_sistema_diario",
        "vw_operacion_diaria_planta.tmdl": "operacion_planta_diaria",
        "vw_generacion_diaria_tipo.tmdl": "generacion_tecnologia_diaria",
        "vw_actualizacion_fuentes.tmdl": "estado_fuentes",
    }
    for filename, view_name in expected.items():
        text = (MODEL / "tables" / filename).read_text(encoding="utf-8")
        assert 'Name="serving"' in text or 'Name = "serving"' in text
        assert view_name in text


def test_market_and_reservoir_tables_use_stable_serving_contracts():
    expected = {
        "demanda_mercado_diaria.tmdl": "demanda_mercado_diaria",
        "energia_embalsada_diaria.tmdl": "energia_embalsada_diaria",
    }
    for filename, view_name in expected.items():
        text = (MODEL / "tables" / filename).read_text(encoding="utf-8")
        assert 'Name="serving"' in text or 'Name = "serving"' in text
        assert view_name in text


def test_comparable_system_measures_do_not_mix_cutoff_dates():
    measures = (MODEL / "tables" / "Medidas.tmdl").read_text(encoding="utf-8")
    for column in (
        "generacion_comparable_gwh",
        "demanda_comparable_gwh",
        "disponibilidad_comparable_gwh",
        "precio_comparable_cop_kwh",
    ):
        assert column in measures
    assert "Fecha de corte comparable" in measures


def test_technical_page_is_registered_and_uses_technical_contracts():
    pages = json.loads((REPORT / "pages" / "pages.json").read_text(encoding="utf-8-sig"))
    assert TECHNICAL_PAGE in pages["pageOrder"]

    page_root = REPORT / "pages" / TECHNICAL_PAGE
    page = json.loads((page_root / "page.json").read_text(encoding="utf-8-sig"))
    assert page["displayName"] == "03 - Operación técnica"

    visual_text = "\n".join(
        path.read_text(encoding="utf-8-sig")
        for path in (page_root / "visuals").glob("*/visual.json")
    )
    for entity in ("pipeline_health", "task_performance", "quality_alerts"):
        assert entity in visual_text


def test_technology_slicer_uses_shared_dimension_without_fixed_selection():
    visual = REPORT / "pages" / "ce457957940445030600" / "visuals" / "89d61406b040b7debb89" / "visual.json"
    payload = json.loads(visual.read_text(encoding="utf-8-sig"))
    projection = payload["visual"]["query"]["queryState"]["Values"]["projections"][0]
    assert projection["queryRef"] == "DimTecnologia.tipo_generacion"
    assert payload["visual"]["objects"]["general"] == []


def test_refresh_model_excludes_legacy_hourly_and_duplicate_plant_queries():
    model = (MODEL / "model.tmdl").read_text(encoding="utf-8")
    relationships = (MODEL / "relationships.tmdl").read_text(encoding="utf-8")
    for legacy_table in ("vw_sistema_horario", "vw_operacion_diaria_planta (2)"):
        assert legacy_table not in model
        assert legacy_table not in relationships


def test_agent_slicer_reads_the_existing_analytics_view():
    agent_table = (MODEL / "tables" / "vw_dim_agente_powerbi.tmdl").read_text(encoding="utf-8")
    assert 'Name="gold_analytics"' in agent_table


def test_plant_fact_only_declares_columns_available_in_serving_contract():
    plant_table = (MODEL / "tables" / "vw_operacion_diaria_planta.tmdl").read_text(encoding="utf-8")
    assert "cap_efectiva_neta" in plant_table
    assert "capacidad_efectiva_neta_mw" not in plant_table


def test_generation_technology_uses_shared_date_dimension_only():
    model = (MODEL / "model.tmdl").read_text(encoding="utf-8")
    relationships = (MODEL / "relationships.tmdl").read_text(encoding="utf-8")
    assert "LocalDateTable_375b8913-a637-4aec-aa65-a71b8c982897" not in model
    assert "LocalDateTable_375b8913-a637-4aec-aa65-a71b8c982897" not in relationships


def test_market_and_reservoir_pages_use_real_contracts_and_shared_date_filter():
    pages = json.loads((REPORT / "pages" / "pages.json").read_text(encoding="utf-8-sig"))
    assert MARKET_PAGE in pages["pageOrder"]
    assert RESERVOIR_PAGE in pages["pageOrder"]

    checks = {
        MARKET_PAGE: ("04 - Demanda y mercado", "demanda_mercado_diaria", "Demanda mercado (TWh)"),
        RESERVOIR_PAGE: ("05 - Embalses y cobertura", "energia_embalsada_diaria", "Energía embalsada última (GWh)"),
    }
    for page_id, (name, entity, measure) in checks.items():
        page_root = REPORT / "pages" / page_id
        page = json.loads((page_root / "page.json").read_text(encoding="utf-8-sig"))
        assert page["displayName"] == name
        visual_text = "\n".join(
            path.read_text(encoding="utf-8-sig")
            for path in (page_root / "visuals").glob("*/visual.json")
        )
        assert entity in visual_text
        assert measure in visual_text
        assert '"Entity":  "DimFecha"' in visual_text


def test_date_dimension_and_summary_slicer_cover_all_business_dates_without_fixed_end_date():
    dim_date = (MODEL / "tables" / "DimFecha.tmdl").read_text(encoding="utf-8")
    for table_name in ("demanda_mercado_diaria", "energia_embalsada_diaria"):
        assert table_name in dim_date

    slicer = REPORT / "pages" / "2d35f50cc70c0b3d126e" / "visuals" / "efc90567611775809d20" / "visual.json"
    payload = json.loads(slicer.read_text(encoding="utf-8-sig"))
    assert "endDate" not in payload["visual"]["objects"]["data"][0]["properties"]
    assert payload["visual"]["objects"]["general"] == []


def test_multiline_measure_expressions_keep_valid_tmdl_indentation():
    lines = (MODEL / "tables" / "Medidas.tmdl").read_text(encoding="utf-8").splitlines()
    inside_expression = False
    for line in lines:
        if line.startswith("\tmeasure ") and line.rstrip().endswith("="):
            inside_expression = True
            continue
        if inside_expression and line.startswith("\t\t") and not line.startswith("\t\t\t"):
            inside_expression = False
        if inside_expression and line.strip():
            assert line.startswith("\t\t\t"), f"Invalid TMDL expression indentation: {line!r}"
