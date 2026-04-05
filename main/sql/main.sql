SELECT
    relname AS "Table Name",
    pg_size_pretty(pg_total_relation_size(relid)) AS "Total Size"
FROM
    pg_catalog.pg_statio_user_tables
where relname like 'main%'
ORDER BY
    pg_total_relation_size(relid) DESC;