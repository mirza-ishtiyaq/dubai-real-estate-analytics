# Power BI

The interactive dashboard for this project is built in **Power BI** and published through **Microsoft Fabric**.

## Semantic Model

The semantic model lives in Microsoft Fabric and connects to the Snowflake data warehouse via DirectQuery. It implements:

- **Star schema** with 5 dimensions and 1 fact table
- **Row-Level Security (RLS)** using a DAX bridge table
- **DAX measures** for KPIs (total value, average price per sqft, transaction counts, etc.)

## Custom Theme

[`Dubai_Real_Estate_Theme.json`](Dubai_Real_Estate_Theme.json) — *(not yet added)* Custom Power BI theme file for consistent visual styling across all report pages.

## Dashboard Screenshots

Screenshots of the published dashboard are available in [`docs/images/`](../docs/images/).

> **Note:** The `.pbix` file is excluded from this repository (see `.gitignore`) because it is a large binary that Git cannot diff or merge. The report is best viewed through the published Fabric workspace.
