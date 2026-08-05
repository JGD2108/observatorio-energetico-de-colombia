param(
    [string]$ProjectRoot = (Join-Path $PSScriptRoot "..\Dashboard_observatorio")
)

$ErrorActionPreference = "Stop"
$reportRoot = Join-Path $ProjectRoot "Dashboard observatorio_dev.Report\definition"
$pagesRoot = Join-Path $reportRoot "pages"
$summaryPage = Join-Path $pagesRoot "2d35f50cc70c0b3d126e"
$plantPage = Join-Path $pagesRoot "ce457957940445030600"
$utf8 = New-Object System.Text.UTF8Encoding($false)

function U([string]$EscapedValue) { return ('"' + $EscapedValue + '"') | ConvertFrom-Json }
function Read-Json([string]$Path) { return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json }
function Write-Json($Value, [string]$Path) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force | Out-Null
    [System.IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth 100) + [Environment]::NewLine), $utf8)
}
function Remove-Property($Value, [string]$Name) { if ($Value.PSObject.Properties[$Name]) { $Value.PSObject.Properties.Remove($Name) } }
function Save-Visual($Visual, [string]$VisualRoot, [string]$Id) {
    $Visual.name = $Id
    Remove-Property $Visual "parentGroupName"
    Write-Json $Visual (Join-Path $VisualRoot "$Id\visual.json")
}

function New-Textbox([string]$VisualRoot, [string]$TemplateId, [string]$Id, [string]$OldText, [string]$NewText, [double]$Y) {
    $template = Join-Path $summaryPage "visuals\$TemplateId\visual.json"
    $raw = Get-Content -LiteralPath $template -Raw -Encoding UTF8
    $visual = ($raw.Replace($OldText, $NewText)) | ConvertFrom-Json
    $visual.position.x = 24; $visual.position.y = $Y; $visual.position.width = 1000
    Save-Visual $visual $VisualRoot $Id
}

function New-Card([string]$VisualRoot, [string]$Id, [string]$Measure, [string]$Label, [double]$X, [string]$Color) {
    $template = Join-Path $summaryPage "visuals\3fa31c6a08ad50c4ae2a\visual.json"
    $templateObject = Read-Json $template
    $oldMeasure = $templateObject.visual.query.queryState.Data.projections[0].field.Measure.Property
    $raw = Get-Content -LiteralPath $template -Raw -Encoding UTF8
    $card = ($raw.Replace($oldMeasure, $Measure)) | ConvertFrom-Json
    $card.position.x = $X; $card.position.y = 104; $card.position.width = 296; $card.position.height = 96
    $card.visual.objects.label[1].properties.text.expr.Literal.Value = "'$Label'"
    $card.visual.objects.value[0].properties.fontColor.solid.color.expr.Literal.Value = "'$Color'"
    Save-Visual $card $VisualRoot $Id
}

function New-DateSlicer([string]$VisualRoot, [string]$Id) {
    $template = Join-Path $summaryPage "visuals\efc90567611775809d20\visual.json"
    $slicer = Read-Json $template
    $slicer.position.x = 1040; $slicer.position.y = 8; $slicer.position.width = 224; $slicer.position.height = 64
    $slicer.visual.objects.general = @()
    Remove-Property $slicer.visual.objects.data[0].properties "endDate"
    Save-Visual $slicer $VisualRoot $Id
}

function New-Line([string]$VisualRoot, [string]$Id, [string]$Measure, [string]$Title, [string]$AxisTitle, [double]$X, [double]$Y, [double]$Width, [double]$Height, [string]$Color) {
    $template = Join-Path $summaryPage "visuals\c276ff2559a9a1e93604\visual.json"
    $templateObject = Read-Json $template
    $oldMeasure = $templateObject.visual.query.queryState.Y.projections[0].field.Measure.Property
    $raw = Get-Content -LiteralPath $template -Raw -Encoding UTF8
    $line = ($raw.Replace($oldMeasure, $Measure)) | ConvertFrom-Json
    $line.position.x = $X; $line.position.y = $Y; $line.position.width = $Width; $line.position.height = $Height
    $line.visual.query.queryState.Y.projections[0].nativeQueryRef = $Measure
    $line.visual.query.queryState.Y.projections[0].displayName = $Measure
    $line.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'$Title'"
    $line.visual.objects.valueAxis[0].properties.titleText.expr.Literal.Value = "'$AxisTitle'"
    $line.visual.objects.dataPoint[0].properties.fill.solid.color.expr.Literal.Value = "'$Color'"
    Save-Visual $line $VisualRoot $Id
}

function New-MeasureProjection([string]$Measure, [string]$Label) {
    return [pscustomobject]@{ field=[pscustomobject]@{Measure=[pscustomobject]@{Expression=[pscustomobject]@{SourceRef=[pscustomobject]@{Entity="Medidas"}}; Property=$Measure}}; queryRef="Medidas.$Measure"; nativeQueryRef=$Label; displayName=$Label }
}
function New-ColumnProjection([string]$Entity, [string]$Property, [string]$Label) {
    return [pscustomobject]@{ field=[pscustomobject]@{Column=[pscustomobject]@{Expression=[pscustomobject]@{SourceRef=[pscustomobject]@{Entity=$Entity}}; Property=$Property}}; queryRef="$Entity.$Property"; nativeQueryRef=$Label; displayName=$Label }
}
function New-Bar([string]$VisualRoot, [string]$Id, [string]$Entity, [string]$Column, [string]$Measure, [string]$Title, [double]$X, [double]$Y, [double]$Width, [double]$Height) {
    $template = Join-Path $plantPage "visuals\b7796958938893a21026\visual.json"
    $bar = Read-Json $template
    $bar.position.x=$X; $bar.position.y=$Y; $bar.position.width=$Width; $bar.position.height=$Height
    $bar.visual.query.queryState.Category.projections=@((New-ColumnProjection $Entity $Column $Column))
    $bar.visual.query.queryState.Y.projections=@((New-MeasureProjection $Measure $Measure))
    Remove-Property $bar.visual.query.queryState "Tooltips"
    $bar.visual.query.sortDefinition.sort=@([pscustomobject]@{field=[pscustomobject]@{Measure=[pscustomobject]@{Expression=[pscustomobject]@{SourceRef=[pscustomobject]@{Entity="Medidas"}};Property=$Measure}};direction="Descending"})
    Remove-Property $bar "filterConfig"
    $bar.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'$Title'"
    Save-Visual $bar $VisualRoot $Id
}
function New-Table([string]$VisualRoot, [string]$Id, [object[]]$Projections, [string]$Title, [double]$X, [double]$Y, [double]$Width, [double]$Height) {
    $template = Join-Path $summaryPage "visuals\ef98f913a7c3029bc636\visual.json"
    $table = Read-Json $template
    $table.position.x=$X; $table.position.y=$Y; $table.position.width=$Width; $table.position.height=$Height
    $table.visual.query.queryState.Values.projections=$Projections
    Remove-Property $table.visual.query "sortDefinition"
    $table.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'$Title'"
    Save-Visual $table $VisualRoot $Id
}
function New-Page([string]$PageId, [string]$DisplayName) {
    return [pscustomobject]@{'$schema'='https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json';name=$PageId;displayName=$DisplayName;displayOption='FitToPage';height=720;width=1280;objects=[pscustomobject]@{background=@([pscustomobject]@{properties=[pscustomobject]@{color=[pscustomobject]@{solid=[pscustomobject]@{color=[pscustomobject]@{expr=[pscustomobject]@{Literal=[pscustomobject]@{Value="'#F5F7FA'"}}}}};transparency=[pscustomobject]@{expr=[pscustomobject]@{Literal=[pscustomobject]@{Value='0D'}}}}})}}
}

$marketId='f7c7a0e1202608050002'; $energyId='f7c7a0e1202608050003'
foreach($id in @($marketId,$energyId)) { New-Item -ItemType Directory -Path (Join-Path $pagesRoot "$id\visuals") -Force | Out-Null }

$marketVisuals=Join-Path $pagesRoot "$marketId\visuals"
New-Textbox $marketVisuals '1c2c795666730bb85922' 'f7c7a0e1202608051101' (U 'Resumen del sistema el\u00e9ctrico') (U 'Demanda y mercado el\u00e9ctrico') 8
New-Textbox $marketVisuals 'd8d1c88d1a5d137095d8' 'f7c7a0e1202608051102' (U 'Operaci\u00f3n, mercado y disponibilidad del SIN') (U 'Demanda regulada, no regulada, picos y cobertura horaria') 62
New-DateSlicer $marketVisuals 'f7c7a0e1202608051103'
New-Card $marketVisuals 'f7c7a0e1202608051201' (U 'Demanda mercado (TWh)') (U 'Demanda acumulada') 16 '#1473E6'
New-Card $marketVisuals 'f7c7a0e1202608051202' (U 'Demanda promedio mercado (MW)') (U 'Demanda promedio diaria') 328 '#079A9E'
New-Card $marketVisuals 'f7c7a0e1202608051203' (U 'Demanda pico mercado (MW)') (U 'Pico diario por mercado') 640 '#F5A000'
New-Card $marketVisuals 'f7c7a0e1202608051204' (U 'D\u00edas demanda completa') (U 'D\u00edas con 24 horas') 952 '#159447'
New-Line $marketVisuals 'f7c7a0e1202608051301' (U 'Demanda mercado (GWh)') (U 'Evoluci\u00f3n diaria de la demanda') 'GWh' 16 216 608 260 '#1473E6'
New-Line $marketVisuals 'f7c7a0e1202608051302' (U 'Demanda pico mercado (MW)') (U 'Pico diario de demanda por mercado') 'MW' 640 216 624 260 '#F5A000'
New-Bar $marketVisuals 'f7c7a0e1202608051401' 'demanda_mercado_diaria' 'tipo_mercado' (U 'Demanda mercado (GWh)') (U 'Demanda acumulada por mercado') 16 492 608 212
New-Table $marketVisuals 'f7c7a0e1202608051402' @((New-ColumnProjection 'demanda_mercado_diaria' 'fecha' 'Fecha'),(New-ColumnProjection 'demanda_mercado_diaria' 'tipo_mercado' (U 'Mercado')),(New-ColumnProjection 'demanda_mercado_diaria' 'demanda_total_gwh' 'Demanda GWh'),(New-ColumnProjection 'demanda_mercado_diaria' 'demanda_pico_mw' 'Pico MW'),(New-ColumnProjection 'demanda_mercado_diaria' 'horas_con_datos' 'Horas')) (U 'Detalle diario por mercado') 640 492 624 212
Write-Json (New-Page $marketId (U '04 - Demanda y mercado')) (Join-Path $pagesRoot "$marketId\page.json")

$energyVisuals=Join-Path $pagesRoot "$energyId\visuals"
New-Textbox $energyVisuals '1c2c795666730bb85922' 'f7c7a0e1202608052101' (U 'Resumen del sistema el\u00e9ctrico') (U 'Energ\u00eda embalsada y cobertura') 8
New-Textbox $energyVisuals 'd8d1c88d1a5d137095d8' 'f7c7a0e1202608052102' (U 'Operaci\u00f3n, mercado y disponibilidad del SIN') (U 'Nivel energ\u00e9tico, variaci\u00f3n diaria y trazabilidad de asignaci\u00f3n') 62
New-DateSlicer $energyVisuals 'f7c7a0e1202608052103'
New-Card $energyVisuals 'f7c7a0e1202608052201' (U 'Energ\u00eda embalsada \u00faltima (GWh)') (U 'Energ\u00eda en embalses') 16 '#1473E6'
New-Card $energyVisuals 'f7c7a0e1202608052202' (U 'Variaci\u00f3n embalses \u00faltima (GWh)') (U 'Variaci\u00f3n diaria') 328 '#159447'
New-Card $energyVisuals 'f7c7a0e1202608052203' (U 'Cobertura directa \u00faltima (%)') (U 'Cobertura de asignaci\u00f3n') 640 '#079A9E'
New-Card $energyVisuals 'f7c7a0e1202608052204' (U 'Plantas con medici\u00f3n \u00faltima') (U 'Plantas con medici\u00f3n') 952 '#F5A000'
New-Line $energyVisuals 'f7c7a0e1202608052301' (U 'Energ\u00eda embalsada \u00faltima (GWh)') (U 'Evoluci\u00f3n de la energ\u00eda embalsada') 'GWh' 16 216 608 260 '#1473E6'
New-Line $energyVisuals 'f7c7a0e1202608052302' (U 'Cobertura directa (%)') (U 'Cobertura de asignaci\u00f3n directa') '%' 640 216 624 260 '#079A9E'
New-Bar $energyVisuals 'f7c7a0e1202608052401' 'energia_embalsada_diaria' 'fecha' (U 'Variaci\u00f3n energ\u00eda diaria (GWh)') (U 'Variaci\u00f3n diaria de energ\u00eda') 16 492 608 212
New-Table $energyVisuals 'f7c7a0e1202608052402' @((New-ColumnProjection 'energia_embalsada_diaria' 'fecha' 'Fecha'),(New-ColumnProjection 'energia_embalsada_diaria' 'energia_total_gwh' (U 'Energ\u00eda GWh')),(New-ColumnProjection 'energia_embalsada_diaria' 'variacion_diaria_gwh' (U 'Variaci\u00f3n GWh')),(New-ColumnProjection 'energia_embalsada_diaria' 'cobertura_asignacion_directa_pct' (U 'Cobertura directa %')),(New-ColumnProjection 'energia_embalsada_diaria' 'plantas_con_medicion' 'Plantas')) (U 'Detalle diario y cobertura') 640 492 624 212
Write-Json (New-Page $energyId (U '05 - Embalses y cobertura')) (Join-Path $pagesRoot "$energyId\page.json")

$pagesPath=Join-Path $pagesRoot 'pages.json'; $pages=Read-Json $pagesPath
foreach($id in @($marketId,$energyId)){if($pages.pageOrder -notcontains $id){$pages.pageOrder=@($pages.pageOrder)+$id}}
Write-Json $pages $pagesPath
Write-Output 'Market and reservoir pages generated.'
