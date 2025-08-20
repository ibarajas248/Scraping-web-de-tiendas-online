-- repetidos por ean en productos

SELECT


    p.ean,
    p.id,
    p.nombre,
    p.marca
FROM productos p
JOIN (
    SELECT ean
    FROM productos
    WHERE ean IS NOT NULL AND ean <> ''
    GROUP BY ean
    HAVING COUNT(*) > 1
) dup ON p.ean = dup.ean
ORDER BY p.ean, p.id;
