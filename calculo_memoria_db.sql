SELECT
    table_schema AS base_de_datos,
    ROUND(SUM(data_length) / 1024 / 1024, 2) AS datos_mb,
    ROUND(SUM(index_length) / 1024 / 1024, 2) AS indices_mb,
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS total_mb
FROM information_schema.tables
GROUP BY table_schema
ORDER BY total_mb DESC;