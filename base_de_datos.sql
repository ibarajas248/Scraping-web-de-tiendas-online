-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Servidor: localhost:3310
-- Tiempo de generación: 16-08-2025 a las 17:41:26
-- Versión del servidor: 10.4.28-MariaDB
-- Versión de PHP: 8.2.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de datos: `analisis_retail`
--

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `historico_precios`
--

CREATE TABLE `historico_precios` (
  `id` int(10) UNSIGNED NOT NULL,
  `tienda_id` smallint(5) UNSIGNED NOT NULL,
  `producto_tienda_id` int(10) UNSIGNED NOT NULL,
  `capturado_en` datetime NOT NULL,
  `precio_lista` decimal(10,2) DEFAULT NULL,
  `precio_oferta` decimal(10,2) DEFAULT NULL,
  `tipo_oferta` varchar(120) DEFAULT NULL,
  `promo_tipo` varchar(240) DEFAULT NULL,
  `promo_texto_regular` varchar(360) DEFAULT NULL,
  `promo_texto_descuento` varchar(360) DEFAULT NULL,
  `promo_comentarios` varchar(360) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `productos`
--

CREATE TABLE `productos` (
  `id` int(10) UNSIGNED NOT NULL,
  `ean` varchar(32) DEFAULT NULL,
  `nombre` varchar(360) DEFAULT NULL,
  `marca` varchar(120) DEFAULT NULL,
  `fabricante` varchar(240) DEFAULT NULL,
  `categoria` varchar(120) DEFAULT NULL,
  `subcategoria` varchar(120) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `producto_tienda`
--

CREATE TABLE `producto_tienda` (
  `id` int(10) UNSIGNED NOT NULL,
  `tienda_id` smallint(5) UNSIGNED NOT NULL,
  `producto_id` int(10) UNSIGNED NOT NULL,
  `sku_tienda` varchar(80) DEFAULT NULL,
  `record_id_tienda` varchar(80) DEFAULT NULL,
  `url_tienda` varchar(512) DEFAULT NULL,
  `nombre_tienda` varchar(360) DEFAULT NULL,
  `is_available` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `regiones`
--

CREATE TABLE `regiones` (
  `id` smallint(5) UNSIGNED NOT NULL,
  `nombre` varchar(120) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tiendas`
--

CREATE TABLE `tiendas` (
  `id` smallint(5) UNSIGNED NOT NULL,
  `codigo` varchar(40) NOT NULL,
  `nombre` varchar(160) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tienda_region`
--

CREATE TABLE `tienda_region` (
  `tienda_id` smallint(5) UNSIGNED NOT NULL,
  `region_id` smallint(5) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uniq_snapshot` (`producto_tienda_id`,`capturado_en`),
  ADD KEY `idx_hp_prod_time` (`producto_tienda_id`,`capturado_en`),
  ADD KEY `idx_hp_tienda_time` (`tienda_id`,`capturado_en`);

--
-- Indices de la tabla `productos`
--
ALTER TABLE `productos`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uniq_ean` (`ean`),
  ADD KEY `idx_prod_marca` (`marca`);

--
-- Indices de la tabla `producto_tienda`
--
ALTER TABLE `producto_tienda`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uniq_tienda_sku` (`tienda_id`,`sku_tienda`),
  ADD UNIQUE KEY `uniq_tienda_record` (`tienda_id`,`record_id_tienda`),
  ADD KEY `idx_tienda_prod` (`tienda_id`,`producto_id`),
  ADD KEY `fk_pt_producto` (`producto_id`);

--
-- Indices de la tabla `regiones`
--
ALTER TABLE `regiones`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uniq_region_nombre` (`nombre`);

--
-- Indices de la tabla `tiendas`
--
ALTER TABLE `tiendas`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `codigo` (`codigo`);

--
-- Indices de la tabla `tienda_region`
--
ALTER TABLE `tienda_region`
  ADD PRIMARY KEY (`tienda_id`,`region_id`),
  ADD KEY `fk_tr_region` (`region_id`);

--
-- AUTO_INCREMENT de las tablas volcadas
--

--
-- AUTO_INCREMENT de la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `productos`
--
ALTER TABLE `productos`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `producto_tienda`
--
ALTER TABLE `producto_tienda`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `regiones`
--
ALTER TABLE `regiones`
  MODIFY `id` smallint(5) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tiendas`
--
ALTER TABLE `tiendas`
  MODIFY `id` smallint(5) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  ADD CONSTRAINT `fk_hp_pt` FOREIGN KEY (`producto_tienda_id`) REFERENCES `producto_tienda` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk_hp_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Filtros para la tabla `producto_tienda`
--
ALTER TABLE `producto_tienda`
  ADD CONSTRAINT `fk_pt_producto` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`) ON UPDATE CASCADE,
  ADD CONSTRAINT `fk_pt_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Filtros para la tabla `tienda_region`
--
ALTER TABLE `tienda_region`
  ADD CONSTRAINT `fk_tr_region` FOREIGN KEY (`region_id`) REFERENCES `regiones` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `fk_tr_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;





---------shecduler------

-- 🔓 Asegúrate de que el scheduler de eventos esté activado
-- 🔓 Asegúrate de que el scheduler de eventos esté activado
SET GLOBAL event_scheduler = ON;

-- 📌 Evento que se ejecuta cada 30 minutos
DELIMITER //
CREATE EVENT IF NOT EXISTS ev_fix_precios
ON SCHEDULE EVERY 30 MINUTE
STARTS CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DO
BEGIN
    -- Cambiar precio_lista = 0 a NULL
    UPDATE historico_precios
    SET precio_lista = NULL
    WHERE precio_lista = 0;

    -- Cambiar precio_oferta = 0 a NULL
    UPDATE historico_precios
    SET precio_oferta = NULL
    WHERE precio_oferta = 0;
END//
DELIMITER ;


DELIMITER //
CREATE EVENT IF NOT EXISTS ev_cleanup_historico
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DO
BEGIN
    DECLARE db_size_gb DECIMAL(10,2);

    -- Calcula el tamaño de la BD en GB
    SELECT SUM(data_length + index_length) / (1024*1024*1024)
    INTO db_size_gb
    FROM information_schema.tables
    WHERE table_schema = 'analisis_retail';

    -- Si pesa más de 10 GB → borrar registros viejos
    IF db_size_gb > 10 THEN
        DELETE FROM historico_precios
        WHERE capturado_en < NOW() - INTERVAL 6 MONTH;
    END IF;
END//
DELIMITER ;
