

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
  `id` bigint(20) NOT NULL,
  `tienda_id` int(11) NOT NULL,
  `producto_tienda_id` bigint(20) NOT NULL,
  `capturado_en` datetime NOT NULL,
  `precio_lista` decimal(12,2) DEFAULT NULL,
  `precio_oferta` decimal(12,2) DEFAULT NULL,
  `tipo_oferta` varchar(200) DEFAULT NULL,
  `promo_tipo` varchar(500) DEFAULT NULL,
  `promo_texto_regular` varchar(500) DEFAULT NULL,
  `promo_texto_descuento` varchar(500) DEFAULT NULL,
  `promo_comentarios` varchar(800) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `productos`
--

CREATE TABLE `productos` (
  `id` bigint(20) NOT NULL,
  `ean` varchar(32) DEFAULT NULL,
  `nombre` varchar(600) DEFAULT NULL,
  `marca` varchar(200) DEFAULT NULL,
  `fabricante` varchar(400) DEFAULT NULL,
  `categoria` varchar(200) DEFAULT NULL,
  `subcategoria` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `producto_tienda`
--

CREATE TABLE `producto_tienda` (
  `id` bigint(20) NOT NULL,
  `tienda_id` int(11) NOT NULL,
  `producto_id` bigint(20) NOT NULL,
  `sku_tienda` varchar(100) DEFAULT NULL,
  `record_id_tienda` varchar(100) DEFAULT NULL,
  `url_tienda` varchar(800) DEFAULT NULL,
  `nombre_tienda` varchar(600) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tiendas`
--

CREATE TABLE `tiendas` (
  `id` int(11) NOT NULL,
  `codigo` varchar(50) NOT NULL,
  `nombre` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uniq_snapshot` (`tienda_id`,`producto_tienda_id`,`capturado_en`),
  ADD KEY `idx_cuando` (`capturado_en`),
  ADD KEY `fk_hp_pt` (`producto_tienda_id`);

--
-- Indices de la tabla `productos`
--
ALTER TABLE `productos`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_prod_ean` (`ean`),
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
-- Indices de la tabla `tiendas`
--
ALTER TABLE `tiendas`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `codigo` (`codigo`);

--
-- AUTO_INCREMENT de las tablas volcadas
--

--
-- AUTO_INCREMENT de la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `productos`
--
ALTER TABLE `productos`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `producto_tienda`
--
ALTER TABLE `producto_tienda`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tiendas`
--
ALTER TABLE `tiendas`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `historico_precios`
--
ALTER TABLE `historico_precios`
  ADD CONSTRAINT `fk_hp_pt` FOREIGN KEY (`producto_tienda_id`) REFERENCES `producto_tienda` (`id`),
  ADD CONSTRAINT `fk_hp_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Filtros para la tabla `producto_tienda`
--
ALTER TABLE `producto_tienda`
  ADD CONSTRAINT `fk_pt_producto` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`),
  ADD CONSTRAINT `fk_pt_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
