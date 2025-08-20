USE analisis_retail;

-- Borra en orden hijo → padre
START TRANSACTION;
DELETE FROM historico_precios;
DELETE FROM producto_tienda;
DELETE FROM productos;
DELETE FROM tiendas;
COMMIT;

-- Resetea AUTO_INCREMENT (opcional)
ALTER TABLE historico_precios AUTO_INCREMENT = 1;
ALTER TABLE producto_tienda   AUTO_INCREMENT = 1;
ALTER TABLE productos         AUTO_INCREMENT = 1;
ALTER TABLE tiendas           AUTO_INCREMENT = 1;

-- Recupera espacio (opcional)
OPTIMIZE TABLE historico_precios, producto_tienda, productos, tiendas;






----------
---- en mysql


USE scrap;

-- Deshabilitar FKs para poder truncar en cualquier orden (o en orden hijo→padre)
SET FOREIGN_KEY_CHECKS = 0;

-- Vaciar tablas (TRUNCATE es más rápido que DELETE y resetea AUTO_INCREMENT)
TRUNCATE TABLE historico_precios;
TRUNCATE TABLE producto_tienda;
TRUNCATE TABLE productos;
TRUNCATE TABLE tiendas;

-- Rehabilitar FKs
SET FOREIGN_KEY_CHECKS = 1;

-- (Opcional) Optimizar tablas; tras TRUNCATE normalmente no hace falta, pero lo dejo por si quieres ejecutarlo
OPTIMIZE TABLE historico_precios, producto_tienda, productos, tiendas;

