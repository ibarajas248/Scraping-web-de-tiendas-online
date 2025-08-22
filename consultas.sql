-- ultimo precio por producto-tienda-extendido

create view informeCarrefour as
SELECT
		productos.ean,
        productos.nombre,
        productos.marca,
        productos.fabricante,
        productos.categoria,
        productos.subcategoria,
		producto_tienda.sku_tienda,
       producto_tienda.url_tienda,
       producto_tienda.nombre_tienda,

		hp.*

FROM historico_precios hp
JOIN (
    SELECT producto_tienda_id, MAX(capturado_en) AS ult_fecha
    FROM historico_precios
    WHERE tienda_id = 1
    GROUP BY producto_tienda_id
) ult ON hp.producto_tienda_id = ult.producto_tienda_id
     AND hp.capturado_en = ult.ult_fecha

     join producto_tienda on hp.producto_tienda_id=producto_tienda.id
     left join productos on productos.id=producto_tienda.producto_id
WHERE hp.tienda_id = 1;


-- ver cuanto ocupa en memoria


SELECT table_schema AS "Base de Datos",
       ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS "Tamaño (MB)"
FROM information_schema.tables
GROUP BY table_schema;
-- Último precio por producto_tienda

-- Obtiene el último snapshot de precio registrado para cada producto_tienda en una tienda:

SELECT hp.producto_tienda_id,
       hp.precio_lista,
       hp.precio_oferta,
       hp.capturado_en
FROM historico_precios hp
JOIN (
    SELECT producto_tienda_id, MAX(capturado_en) AS ult_fecha
    FROM historico_precios
    WHERE tienda_id = 1
    GROUP BY producto_tienda_id
) ult ON hp.producto_tienda_id = ult.producto_tienda_id
     AND hp.capturado_en = ult.ult_fecha
WHERE hp.tienda_id = 1;


-- Comparar precios actuales entre tiendas
-- Compara el último precio de un mismo producto en todas las tiendas:

SELECT t.nombre AS tienda,
       p.nombre AS producto,
       hp.precio_oferta,
       hp.capturado_en
FROM historico_precios hp
JOIN (
    SELECT producto_tienda_id, MAX(capturado_en) AS ult_fecha
    FROM historico_precios
    GROUP BY producto_tienda_id
) ult ON hp.producto_tienda_id = ult.producto_tienda_id
     AND hp.capturado_en = ult.ult_fecha
JOIN producto_tienda pt ON hp.producto_tienda_id = pt.id
JOIN productos p ON pt.producto_id = p.id
JOIN tiendas t ON pt.tienda_id = t.id
WHERE p.ean = '1234567890123';



-- Promedios de precio por categoría y mes
-- Analiza tendencias de precios:
SELECT p.categoria,
       DATE_FORMAT(hp.capturado_en, '%Y-%m') AS mes,
       AVG(hp.precio_oferta) AS precio_promedio
FROM historico_precios hp
JOIN producto_tienda pt ON hp.producto_tienda_id = pt.id
JOIN productos p ON pt.producto_id = p.id
GROUP BY p.categoria, mes
ORDER BY mes DESC, p.categoria;

-- Detectar cambios de precio
-- Encuentra registros donde el precio cambió respecto al snapshot anterior:
SELECT hp.*
FROM historico_precios hp
JOIN (
    SELECT producto_tienda_id, capturado_en,
           LAG(precio_oferta) OVER (PARTITION BY producto_tienda_id ORDER BY capturado_en) AS precio_anterior
    FROM historico_precios
) cambios
ON hp.producto_tienda_id = cambios.producto_tienda_id
AND hp.capturado_en = cambios.capturado_en
WHERE hp.precio_oferta <> cambios.precio_anterior;




SELECT
    p.id               AS producto_id,
    p.ean,
    p.nombre           AS nombre_producto,
    p.marca,
    p.fabricante,
    p.categoria,
    p.subcategoria,
    t.id               AS tienda_id,
    t.codigo           AS codigo_tienda,
    t.nombre           AS nombre_tienda,
    pt.sku_tienda,
    pt.record_id_tienda,
    pt.url_tienda,
    pt.nombre_tienda,
    hp.precio_lista,
    hp.precio_oferta,
    hp.tipo_oferta,
    hp.promo_tipo,
    hp.promo_texto_regular,
    hp.promo_texto_descuento,
    hp.promo_comentarios,
    hp.capturado_en
FROM producto_tienda pt
JOIN productos p        ON pt.producto_id = p.id
JOIN tiendas t          ON pt.tienda_id = t.id
LEFT JOIN (
    -- subconsulta para tomar solo el último registro por producto_tienda
    SELECT hp1.*
    FROM historico_precios hp1
    JOIN (
        SELECT producto_tienda_id, MAX(capturado_en) AS ult_fecha
        FROM historico_precios
        GROUP BY producto_tienda_id
    ) ult ON hp1.producto_tienda_id = ult.producto_tienda_id
          AND hp1.capturado_en = ult.ult_fecha
) hp ON pt.id = hp.producto_tienda_id
ORDER BY t.nombre, p.nombre;

