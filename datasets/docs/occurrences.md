# GBIF Occurrences Dataset Contract

## 1) Role in the importer
**Occurrences** is the geospatial observation source feeding `geo.py`.

Primary value:
- global point observations for Plantae/Fungi
- habitat grid generation
- centroid selection and elevation enrichment

This dataset is **not fused as taxonomy rows**; it is consumed by the geo stage.

---

## 2) Data flow (what matters)

### Extracts from source
From GBIF snapshot + incremental API downloads:
- occurrence coordinates and species keys (`speciesKey` / `taxonKey` context)
- timestamp-aware incremental updates
- filtered geospatial records (including ingest-time 0/0 exclusion)

### Consumed by geo/distill
- Geo stage uses this parquet to compute:
  - habitat map summaries
  - representative centroids
  - elevation medians
- Distill injects geo JSON into accepted meso rows via `gbif_id` join.

### Extracted but currently dropped downstream
- Most raw occurrence columns are not exposed directly; only aggregated geo products are surfaced.

---

## 3) Trust model
- **Geospatial evidence: HIGH**  
  Core evidence source for distribution geometry.

- **Taxonomy identity: MEDIUM**  
  Relies on GBIF keys and upstream ID alignment.

- **Observation metadata breadth: MEDIUM (usage low)**  
  Rich raw fields exist, but current product is intentionally aggregated.

---

## 4) Edge cases and operational learnings
- Initial bootstrap is very large (~200GB zipped parquet).
- Incremental updates are asynchronous GBIF jobs and require readiness checks.
- GBIF download readiness now uses size stabilization checks to avoid truncated successful downloads.
- `taxonkey` is expected to be numeric. `TRY_CAST` retains NULL or unparseable keys as NULL numeric taxonomy fields, logs their count once, and never falls back to `speciesKey`; downstream numeric joins ignore them.
- The incremental cutoff commits the export request timestamp only after extraction and rolling-parquet persistence complete successfully, including empty deltas.
- Ingest filters out null-island points (`0,0`) early.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Raw occurrence points | Yes | No | No | Input to geo only |
| Habitat aggregates | Derived | No | Yes | Stored in geo parquet/JSON |
| Centroids/elevation | Derived | No | Yes | Joined into meso geo payload |
| Incremental update metadata | Yes | No | Limited | Managed via geo manifest |

---

## 6) Open decisions
1. Decide whether to surface additional occurrence-derived metrics (seasonality, temporal spread).
2. Evaluate adding first-observation year as separate enrichment signal.
