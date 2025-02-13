SELECT NULL      AS TABLE_CAT,
       n.nspname AS TABLE_SCHEM,
       c.relname AS TABLE_NAME,
       CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' WHEN true THEN CASE
                 WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind
                  WHEN 'r' THEN 'SYSTEM TABLE'
                  WHEN 'v' THEN 'SYSTEM VIEW'
                  WHEN 'i' THEN 'SYSTEM INDEX'
                  ELSE NULL
END
WHEN n.nspname = 'pg_toast' THEN CASE c.relkind
                  WHEN 'r' THEN 'SYSTEM TOAST TABLE'
                  WHEN 'i' THEN 'SYSTEM TOAST INDEX'
                  ELSE NULL
END
ELSE CASE c.relkind
                  WHEN 'r' THEN 'TEMPORARY TABLE'
                  WHEN 'p' THEN 'TEMPORARY TABLE'
                  WHEN 'i' THEN 'TEMPORARY INDEX'
                  WHEN 'S' THEN 'TEMPORARY SEQUENCE'
                  WHEN 'v' THEN 'TEMPORARY VIEW'
                  ELSE NULL
END
END
WHEN false THEN CASE c.relkind
                 WHEN 'r' THEN 'TABLE'
                 WHEN 'p' THEN 'PARTITIONED TABLE'
                 WHEN 'i' THEN 'INDEX'
                 WHEN 'P' then 'PARTITIONED INDEX'
                 WHEN 'S' THEN 'SEQUENCE'
                 WHEN 'v' THEN 'VIEW'
                 WHEN 'c' THEN 'TYPE'
                 WHEN 'f' THEN 'FOREIGN TABLE'
                 WHEN 'm' THEN 'MATERIALIZED VIEW'
                 ELSE NULL
END
ELSE NULL
END
AS TABLE_TYPE, d.description AS REMARKS,
                 '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,
                '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION
                 FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c
                 LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)
                 WHERE c.relnamespace = n.oid