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
