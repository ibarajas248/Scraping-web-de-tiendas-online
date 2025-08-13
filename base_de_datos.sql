CREATE DATABASE IF NOT EXISTS analisis_retail
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_general_ci;
USE analisis_retail;

-- =====================================
-- TABLA tiendas (pocas filas)
-- =====================================
CREATE TABLE tiendas (
  id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  codigo VARCHAR(40) NOT NULL,
  nombre VARCHAR(160) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY codigo (codigo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- =====================================
-- TABLA productos (catálogo)
-- =====================================
CREATE TABLE productos (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  ean VARCHAR(32) DEFAULT NULL,
  nombre VARCHAR(360) DEFAULT NULL,
  marca VARCHAR(120) DEFAULT NULL,
  fabricante VARCHAR(240) DEFAULT NULL,
  categoria VARCHAR(120) DEFAULT NULL,
  subcategoria VARCHAR(120) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_ean (ean),
  KEY idx_prod_marca (marca)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- =====================================
-- TABLA producto_tienda (relación producto <-> tienda)
-- =====================================
CREATE TABLE producto_tienda (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  tienda_id SMALLINT UNSIGNED NOT NULL,
  producto_id INT UNSIGNED NOT NULL,
  sku_tienda VARCHAR(80) DEFAULT NULL,
  record_id_tienda VARCHAR(80) DEFAULT NULL,
  url_tienda VARCHAR(512) DEFAULT NULL,
  nombre_tienda VARCHAR(360) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_tienda_sku (tienda_id, sku_tienda),
  UNIQUE KEY uniq_tienda_record (tienda_id, record_id_tienda),
  KEY idx_tienda_prod (tienda_id, producto_id),
  KEY fk_pt_producto (producto_id),
  CONSTRAINT fk_pt_producto FOREIGN KEY (producto_id) REFERENCES productos(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_pt_tienda FOREIGN KEY (tienda_id) REFERENCES tiendas(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- =====================================
-- TABLA historico_precios (muchos registros)
-- =====================================
CREATE TABLE historico_precios (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  tienda_id SMALLINT UNSIGNED NOT NULL,
  producto_tienda_id INT UNSIGNED NOT NULL,
  capturado_en DATETIME(0) NOT NULL,
  precio_lista DECIMAL(10,2) DEFAULT NULL,
  precio_oferta DECIMAL(10,2) DEFAULT NULL,
  tipo_oferta VARCHAR(120) DEFAULT NULL,
  promo_tipo VARCHAR(240) DEFAULT NULL,
  promo_texto_regular VARCHAR(360) DEFAULT NULL,
  promo_texto_descuento VARCHAR(360) DEFAULT NULL,
  promo_comentarios VARCHAR(360) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_snapshot (producto_tienda_id, capturado_en),
  KEY idx_hp_prod_time (producto_tienda_id, capturado_en),
  KEY idx_hp_tienda_time (tienda_id, capturado_en),
  CONSTRAINT fk_hp_pt FOREIGN KEY (producto_tienda_id) REFERENCES producto_tienda(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_hp_tienda FOREIGN KEY (tienda_id) REFERENCES tiendas(id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
