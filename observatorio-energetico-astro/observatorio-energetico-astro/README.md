# Observatorio Energético de Colombia — Astro

Caso de estudio técnico interactivo construido con **Astro**, **Mermaid**, CSS editorial y JavaScript ligero.

## Qué cambió en esta versión

- Migración de la estructura a componentes Astro.
- Nueva arquitectura Lakehouse con responsabilidades y tecnología por capa.
- Secciones 7 y 8 rediseñadas como dos capítulos completos de EDA:
  - método y scorecard de calidad;
  - hallazgos, severidad, decisiones y acciones.
- Sección 9 reconstruida con Mermaid usando el diseño de `GOLD LOAD/Arquitectura GOLD decisiones.md`.
- Diseño responsive, bilingüe y con modo de sustentación.
- Rediseño editorial orientado a presentación con navegación activa por capítulo.
- Filtros interactivos por dominio, feedback de copiado y progreso de lectura.
- Vista conceptual de Power BI basada en los contratos Gold Analytics implementados.
- Controles de sustentación con título de sección, progreso y navegación por teclado.
- Bloques de código explicados con archivo fuente, pasos, decisión, defensa y limitación.
- Espacio preparado para Power BI.

## Estructura

```text
src/
├── components/
│   ├── CodeStudy.astro
│   ├── MermaidDiagram.astro
│   ├── MetricCard.astro
│   └── SectionHeader.astro
├── layouts/
│   └── BaseLayout.astro
├── pages/
│   └── index.astro
└── styles/
    └── global.css
public/
├── assets/
│   ├── databricks-workflow.png
│   └── favicon.svg
└── site.js
dist/
└── vista estática de respaldo lista para abrir
```

## Ejecutar el proyecto Astro

```bash
npm install
npm run dev
```

Abre la dirección mostrada por Astro, normalmente `http://localhost:4321`.

## Compilar

```bash
npm run build
npm run preview
```

## Abrir la vista estática incluida

La carpeta `dist/` contiene una vista portátil de respaldo. Desde la raíz del proyecto:

```bash
python -m http.server 8000 --directory dist
```

Después abre `http://localhost:8000`.

La versión estática carga Mermaid desde CDN. La versión Astro importa Mermaid como dependencia y lo integra en el bundle al compilar.

## Configurar Power BI

En `src/pages/index.astro`, reemplaza el bloque `dashboard-placeholder` por el `iframe` o botón autenticado correspondiente cuando exista el enlace de Power BI Service.

## Enlaces del autor

- LinkedIn: https://www.linkedin.com/in/jdgdsoft/
- GitHub: https://github.com/JGD2108/observatorio-energetico-de-colombia/tree/main
