SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG
FROM pg_catalog.pg_namespace
WHERE nspname <> 'pg_toast'
  AND (nspname !~ '^pg_temp_'
               OR nspname = (pg_catalog.current_schemas(true))[1])
  AND (nspname !~ '^pg_toast_temp_'
               OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))
  AND (nspname !~ 'tableau*')
  AND (nspname !~ 'pg_*')