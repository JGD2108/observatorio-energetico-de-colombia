param(
    [string]$ProjectRoot = (Join-Path $PSScriptRoot "..\Dashboard_observatorio")
)

$ErrorActionPreference = "Stop"
$reportRoot = Join-Path $ProjectRoot "Dashboard observatorio_dev.Report\definition"
$pagesRoot = Join-Path $reportRoot "pages"
$sourcePage = Join-Path $pagesRoot "2d35f50cc70c0b3d126e"
$plantPage = Join-Path $pagesRoot "ce457957940445030600"
$pageId = "f7c7a0e1202608050001"
$pageRoot = Join-Path $pagesRoot $pageId
$visualRoot = Join-Path $pageRoot "visuals"
$utf8 = New-Object System.Text.UTF8Encoding($false)

New-Item -ItemType Directory -Path $visualRoot -Force | Out-Null

function Read-Json([string]$Path) {
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

# Windows PowerShell 5.1 reads UTF-8 scripts without BOM using the legacy code
# page. Keep user-facing strings ASCII in this file and decode JSON escapes here.
function U([string]$EscapedValue) {
    return ('"' + $EscapedValue + '"') | ConvertFrom-Json
}

function Write-Json($Value, [string]$Path) {
    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    $json = $Value | ConvertTo-Json -Depth 100
    [System.IO.File]::WriteAllText($Path, $json + [Environment]::NewLine, $utf8)
}

function Remove-Property($Value, [string]$Name) {
    if ($Value.PSObject.Properties[$Name]) {
        $Value.PSObject.Properties.Remove($Name)
    }
}

function Save-Visual($Visual, [string]$Id) {
    $Visual.name = $Id
    Remove-Property $Visual "parentGroupName"
    Write-Json $Visual (Join-Path $visualRoot "$Id\visual.json")
}

function New-Card(
    [string]$Id,
    [string]$Measure,
    [string]$Label,
    [double]$X,
    [string]$Color
) {
    $template = Join-Path $sourcePage "visuals\3fa31c6a08ad50c4ae2a\visual.json"
    $templateCard = Read-Json $template
    $oldMeasure = $templateCard.visual.query.queryState.Data.projections[0].field.Measure.Property
    $raw = Get-Content -LiteralPath $template -Raw -Encoding UTF8
    $raw = $raw.Replace($oldMeasure, $Measure)
    $card = $raw | ConvertFrom-Json
    $card.position.x = $X
    $card.position.y = 104
    $card.position.width = 296
    $card.position.height = 96
    $card.visual.objects.label[1].properties.text.expr.Literal.Value = "'$Label'"
    $card.visual.objects.value[0].properties.fontColor.solid.color.expr.Literal.Value = "'$Color'"
    Save-Visual $card $Id
}

function New-Textbox(
    [string]$TemplateId,
    [string]$Id,
    [string]$OldText,
    [string]$NewText,
    [double]$Y
) {
    $template = Join-Path $sourcePage "visuals\$TemplateId\visual.json"
    $raw = Get-Content -LiteralPath $template -Raw -Encoding UTF8
    $raw = $raw.Replace($OldText, $NewText)
    $visual = $raw | ConvertFrom-Json
    $visual.position.x = 24
    $visual.position.y = $Y
    $visual.position.width = 1180
    Save-Visual $visual $Id
}

New-Textbox "1c2c795666730bb85922" "f7c7a0e1202608050101" (U 'Resumen del sistema el\u00e9ctrico') (U 'Operaci\u00f3n y confiabilidad del pipeline') 8
New-Textbox "d8d1c88d1a5d137095d8" "f7c7a0e1202608050102" (U 'Operaci\u00f3n, mercado y disponibilidad del SIN') (U 'Ejecuciones, rendimiento, frescura y alertas de calidad') 62

New-Card "f7c7a0e1202608050201" (U 'Estado \u00faltima ejecuci\u00f3n') (U '\u00daltima ejecuci\u00f3n') 16 "#159447"
New-Card "f7c7a0e1202608050202" (U 'Duraci\u00f3n \u00faltima ejecuci\u00f3n (min)') (U 'Duraci\u00f3n del pipeline') 328 "#1473E6"
New-Card "f7c7a0e1202608050203" (U 'Tasa \u00e9xito \u00faltimas 10 (%)') (U '\u00c9xito \u00faltimas 10') 640 "#079A9E"
New-Card "f7c7a0e1202608050204" "Alertas abiertas" "Alertas abiertas" 952 "#F5A000"

$lineTemplate = Join-Path $sourcePage "visuals\c276ff2559a9a1e93604\visual.json"
$lineRaw = Get-Content -LiteralPath $lineTemplate -Raw -Encoding UTF8
$lineRaw = $lineRaw.Replace("Precio promedio (COP/kWh)", (U 'Duraci\u00f3n ejecuci\u00f3n (min)'))
$lineRaw = $lineRaw.Replace("DimFecha", "pipeline_health")
$lineRaw = $lineRaw.Replace('"Property": "Date"', '"Property": "started_at"')
$lineRaw = $lineRaw.Replace("DimFecha.Date", "pipeline_health.started_at")
$line = $lineRaw | ConvertFrom-Json
$line.visual.query.queryState.Category.projections[0].field.Column.Expression.SourceRef.Entity = "pipeline_health"
$line.visual.query.queryState.Category.projections[0].field.Column.Property = "started_at"
$line.visual.query.queryState.Category.projections[0].queryRef = "pipeline_health.started_at"
$line.visual.query.queryState.Category.projections[0].nativeQueryRef = "Fecha y hora"
$line.position.x = 16
$line.position.y = 216
$line.position.width = 608
$line.position.height = 260
$line.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'$(U 'Duraci\u00f3n de las ejecuciones recientes')'"
$line.visual.objects.valueAxis[0].properties.titleText.expr.Literal.Value = "'Minutos'"
Save-Visual $line "f7c7a0e1202608050301"

$barTemplate = Join-Path $plantPage "visuals\b7796958938893a21026\visual.json"
$bar = Read-Json $barTemplate
$bar.position.x = 640
$bar.position.y = 216
$bar.position.width = 624
$bar.position.height = 260
$bar.visual.query.queryState.Category.projections = @(
    [pscustomobject]@{
        field = [pscustomobject]@{
            Column = [pscustomobject]@{
                Expression = [pscustomobject]@{ SourceRef = [pscustomobject]@{ Entity = "task_performance" } }
                Property = "task_key"
            }
        }
        queryRef = "task_performance.task_key"
        nativeQueryRef = "Tarea"
        displayName = "Tarea"
        active = $true
    }
)
$bar.visual.query.queryState.Y.projections = @(
    [pscustomobject]@{
        field = [pscustomobject]@{
            Measure = [pscustomobject]@{
                Expression = [pscustomobject]@{ SourceRef = [pscustomobject]@{ Entity = "Medidas" } }
                Property = (U 'Duraci\u00f3n promedio tarea (min)')
            }
        }
        queryRef = "Medidas.$(U 'Duraci\u00f3n promedio tarea (min)')"
        nativeQueryRef = (U 'Duraci\u00f3n promedio (min)')
    }
)
Remove-Property $bar.visual.query.queryState "Tooltips"
$bar.visual.query.sortDefinition.sort = @(
    [pscustomobject]@{
        field = [pscustomobject]@{
            Measure = [pscustomobject]@{
                Expression = [pscustomobject]@{ SourceRef = [pscustomobject]@{ Entity = "Medidas" } }
                Property = (U 'Duraci\u00f3n promedio tarea (min)')
            }
        }
        direction = "Descending"
    }
)
Remove-Property $bar "filterConfig"
$bar.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'$(U 'Tareas con mayor duraci\u00f3n promedio')'"
Save-Visual $bar "f7c7a0e1202608050302"

function New-ColumnProjection([string]$Entity, [string]$Property, [string]$Label) {
    return [pscustomobject]@{
        field = [pscustomobject]@{
            Column = [pscustomobject]@{
                Expression = [pscustomobject]@{ SourceRef = [pscustomobject]@{ Entity = $Entity } }
                Property = $Property
            }
        }
        queryRef = "$Entity.$Property"
        nativeQueryRef = $Label
        displayName = $Label
    }
}

$tableTemplate = Join-Path $sourcePage "visuals\ef98f913a7c3029bc636\visual.json"
$table = Read-Json $tableTemplate
$table.position.x = 16
$table.position.y = 492
$table.position.width = 1248
$table.position.height = 212
$table.visual.query.queryState.Values.projections = @(
    (New-ColumnProjection "quality_alerts" "severity" "Severidad"),
    (New-ColumnProjection "quality_alerts" "status" "Estado"),
    (New-ColumnProjection "quality_alerts" "component" "Componente"),
    (New-ColumnProjection "quality_alerts" "message" "Mensaje"),
    (New-ColumnProjection "quality_alerts" "created_at" "Creada")
)
Remove-Property $table.visual.query "sortDefinition"
$table.visual.objects = [pscustomobject]@{
    columnHeaders = @($table.visual.objects.columnHeaders[0])
    total = @($table.visual.objects.total[0])
    values = @($table.visual.objects.values[0])
}
$table.visual.visualContainerObjects.title[0].properties.text.expr.Literal.Value = "'Alertas de calidad y seguimiento operativo'"
Save-Visual $table "f7c7a0e1202608050401"

$page = [pscustomobject]@{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
    name = $pageId
    displayName = (U '03 - Operaci\u00f3n t\u00e9cnica')
    displayOption = "FitToPage"
    height = 720
    width = 1280
    objects = [pscustomobject]@{
        background = @(
            [pscustomobject]@{
                properties = [pscustomobject]@{
                    color = [pscustomobject]@{
                        solid = [pscustomobject]@{
                            color = [pscustomobject]@{
                                expr = [pscustomobject]@{
                                    Literal = [pscustomobject]@{ Value = "'#F5F7FA'" }
                                }
                            }
                        }
                    }
                    transparency = [pscustomobject]@{
                        expr = [pscustomobject]@{ Literal = [pscustomobject]@{ Value = "0D" } }
                    }
                }
            }
        )
    }
}
Write-Json $page (Join-Path $pageRoot "page.json")

$pagesPath = Join-Path $pagesRoot "pages.json"
$pages = Read-Json $pagesPath
if ($pages.pageOrder -notcontains $pageId) {
    $pages.pageOrder = @($pages.pageOrder) + $pageId
}
Write-Json $pages $pagesPath

Write-Output "Technical page generated: $pageId"
