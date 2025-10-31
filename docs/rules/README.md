# EU7-LD ruleset configuration

The EU7-LD reporting engine reads its thresholds and limits from
`src/app/rules/specs/eu7_ld.yaml`. The YAML file acts as the single source of
truth for emissions limits, trip composition requirements, and correction
settings. Updating a value in the YAML automatically propagates to the
rule computations used by the API and HTML reports.

## Updating limits

1. Edit `src/app/rules/specs/eu7_ld.yaml` and replace the `TODO` placeholders
   with the agreed numeric values.
2. Keep units consistent with the `units` block to avoid ambiguous output in
   the generated criteria tables.
3. When adding new limits, mirror the structure used by existing entries so
   the computations automatically surface them in the report tables.
4. Run `pytest` to ensure the EU7-LD unit tests pass with the updated
   configuration.

The engine merges any runtime overrides on top of the YAML spec, which enables
unit tests to stub missing values until official numbers are available.
