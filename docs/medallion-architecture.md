# Medallion architecture: verified implementation

## Unity Catalog setup

The setup SQL creates catalog `formula1-incr`, schemas `landing`, `bronze`, `silver`, and `gold`, and the external volume `formula1-incr.landing.files`. The incremental orchestration adds the `control` schema.

## Bronze

The project has ingestion notebooks for circuits, races, constructors, drivers, results, and sprints. The reusable bronze helper adds `batch_id` and writes Delta tables using overwrite plus `replaceWhere` for that batch.

## Silver

Silver transformations read Bronze tables, use `p_batch_id` where implemented, normalise names, perform key checks/deduplication, and merge into Silver Delta tables. The exported project includes both a step-by-step and a fully chained results transformation; the fully chained version is the incremental/upsert-oriented one.

## Gold

Gold builds three dimensions (`dim_races`, `dim_constructors`, `dim_drivers`) and `fact_session_results`. The fact transformation unions race results and sprint results and derives performance flags. The included SQL defines a driver-standing view.

## Source fidelity note

This document describes the exported code as-is. It does not claim missing datasets, table counts, quality checks, job dependencies, or outputs that were not present in the exported sources.
