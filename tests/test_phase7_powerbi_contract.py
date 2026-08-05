import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POWERBI = ROOT / "powerbi" / "Dashboard_observatorio"
MODEL = POWERBI / "Dashboard observatorio_dev.SemanticModel" / "definition"
REPORT = POWERBI / "Dashboard observatorio_dev.Report" / "definition"
TECHNICAL_PAGE = "f7c7a0e1202608050001"


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
