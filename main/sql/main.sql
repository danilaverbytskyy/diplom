SELECT
    io.relname AS "Название таблицы",
    replace(to_char(st.n_live_tup, 'FM999G999G999G999G999'), ',', '.') AS "Количество строк",
    pg_size_pretty(pg_total_relation_size(io.relid)) AS "Размер таблицы"
FROM
    pg_catalog.pg_statio_user_tables io
JOIN
    pg_catalog.pg_stat_user_tables st ON st.relid = io.relid
WHERE
    io.relname LIKE 'main%'
ORDER BY
    pg_total_relation_size(io.relid) DESC;