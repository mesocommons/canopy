# Catalogue of Life Extended Release

Source: `https://download.checklistbank.org/col/xr_latest_coldp.zip`

COL XR starts with COL Base and programmatically integrates additional checklists. GBIF uses COL XR as its primary occurrence taxonomy.

The processor retains direct plant and fungi usages. It also retains synonym-like usages when their `parentID` points to a plant or fungi usage.

Key output fields:

- `id_raw`: COL XR usage identifier.
- `parent_raw`: hierarchy parent for accepted usages and accepted target for synonym-like usages.
- `status_clean`: normalized accepted, synonym, or problematic status.
- `vernacular`: normalized language and name values.

COL XR identifiers are strings. Do not infer their source from identifier shape.
