SELECT n.nspname,
       c.relname,
       a.attname,
       a.atttypid,
       a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
       a.atttypmod,
       a.attlen,
       t.typtypmod,
       a.attnum,
       null                                               as attidentity,
       null                                               as attgenerated,
       pg_catalog.pg_get_expr(def.adbin, def.adrelid)     AS adsrc,
       dsc.description,
       t.typbasetype,
       t.typtype,
       pg_catalog.format_type(a.atttypid, a.atttypmod)    as datatype
FROM pg_catalog.pg_namespace n
         JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
         JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
         JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
         LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum)
         LEFT JOIN pg_catalog.pg_description dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid)
         LEFT JOIN pg_catalog.pg_class dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class')
         LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace = dn.oid AND dn.nspname = 'pg_catalog')
WHERE c.relkind in ('r', 'p', 'v', 'f', 'm')
  and a.attnum > 0
  AND NOT a.attisdropped