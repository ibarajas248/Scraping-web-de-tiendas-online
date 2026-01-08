-- MySQL dump 10.13  Distrib 8.0.34, for Win64 (x86_64)
--
-- Host: localhost    Database: scrap
-- ------------------------------------------------------
-- Server version	8.0.36-28

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `historico_precios`
--

DROP TABLE IF EXISTS `historico_precios`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `historico_precios` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `tienda_id` smallint unsigned NOT NULL,
  `producto_tienda_id` int unsigned NOT NULL,
  `capturado_en` datetime NOT NULL,
  `precio_lista` decimal(10,2) DEFAULT NULL,
  `precio_oferta` decimal(10,2) DEFAULT NULL,
  `tipo_oferta` varchar(120) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `promo_tipo` varchar(240) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `promo_texto_regular` varchar(360) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `promo_texto_descuento` varchar(360) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `promo_comentarios` varchar(360) COLLATE utf8mb4_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_snapshot` (`producto_tienda_id`,`capturado_en`),
  KEY `idx_hp_prod_time` (`producto_tienda_id`,`capturado_en`),
  KEY `idx_hp_tienda_time` (`tienda_id`,`capturado_en`),
  KEY `ix_hp_tienda_sku_fecha` (`tienda_id`,`producto_tienda_id`,`capturado_en`),
  KEY `ix_hp_capturado` (`capturado_en`),
  KEY `ix_hp_tienda_fecha` (`tienda_id`,`capturado_en`),
  KEY `idx_hp_tienda_prod_capturado` (`tienda_id`,`producto_tienda_id`,`capturado_en`),
  KEY `idx_hp_capturado` (`capturado_en`),
  KEY `idx_hp_tienda_pt_fecha` (`tienda_id`,`producto_tienda_id`,`capturado_en`),
  KEY `idx_capturado` (`capturado_en`),
  KEY `idx_hist_tienda` (`tienda_id`),
  KEY `idx_tienda_id` (`tienda_id`),
  KEY `idx_hist_pt_tienda_capt` (`producto_tienda_id`,`tienda_id`,`capturado_en`),
  CONSTRAINT `fk_hp_pt` FOREIGN KEY (`producto_tienda_id`) REFERENCES `producto_tienda` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_hp_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=73938430 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `producto_tienda`
--

DROP TABLE IF EXISTS `producto_tienda`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `producto_tienda` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `tienda_id` smallint unsigned NOT NULL,
  `producto_id` int unsigned NOT NULL,
  `sku_tienda` varchar(80) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `record_id_tienda` varchar(80) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `url_tienda` varchar(512) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `nombre_tienda` varchar(360) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `is_available` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_tienda_sku` (`tienda_id`,`sku_tienda`),
  UNIQUE KEY `uniq_tienda_record` (`tienda_id`,`record_id_tienda`),
  KEY `idx_tienda_prod` (`tienda_id`,`producto_id`),
  KEY `fk_pt_producto` (`producto_id`),
  KEY `ix_pt_producto` (`producto_id`),
  KEY `idx_pt_producto` (`producto_id`),
  KEY `idx_pt_sku` (`sku_tienda`),
  KEY `idx_producto_tienda_tienda_sku` (`tienda_id`,`sku_tienda`),
  CONSTRAINT `fk_pt_producto` FOREIGN KEY (`producto_id`) REFERENCES `productos` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_pt_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=73943958 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `productos`
--

DROP TABLE IF EXISTS `productos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `productos` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `ean` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `nombre` varchar(360) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `marca` varchar(120) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `fabricante` varchar(240) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `categoria` varchar(120) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `subcategoria` varchar(120) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `ean_auxiliar` varchar(45) COLLATE utf8mb4_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_ean` (`ean`),
  KEY `idx_prod_marca` (`marca`),
  KEY `idx_prod_categoria` (`categoria`),
  KEY `idx_prod_subcategoria` (`subcategoria`),
  KEY `idx_prod_fabricante` (`fabricante`),
  KEY `idx_prod_ean` (`ean`),
  KEY `idx_productos_nombre` (`nombre`),
  KEY `idx_productos_ean` (`ean`),
  KEY `idx_productos_ean_aux` (`ean_auxiliar`)
) ENGINE=InnoDB AUTO_INCREMENT=836046 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `regiones`
--

DROP TABLE IF EXISTS `regiones`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `regiones` (
  `id` smallint unsigned NOT NULL AUTO_INCREMENT,
  `nombre` varchar(120) COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_region_nombre` (`nombre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tienda_region`
--

DROP TABLE IF EXISTS `tienda_region`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tienda_region` (
  `tienda_id` smallint unsigned NOT NULL,
  `region_id` smallint unsigned NOT NULL,
  PRIMARY KEY (`tienda_id`,`region_id`),
  KEY `fk_tr_region` (`region_id`),
  CONSTRAINT `fk_tr_region` FOREIGN KEY (`region_id`) REFERENCES `regiones` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_tr_tienda` FOREIGN KEY (`tienda_id`) REFERENCES `tiendas` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tiendas`
--

DROP TABLE IF EXISTS `tiendas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tiendas` (
  `id` smallint unsigned NOT NULL AUTO_INCREMENT,
  `codigo` varchar(40) COLLATE utf8mb4_general_ci NOT NULL,
  `nombre` varchar(160) COLLATE utf8mb4_general_ci NOT NULL,
  `ref_tienda` varchar(80) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `provincia` varchar(80) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `sucursal` varchar(160) COLLATE utf8mb4_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `codigo` (`codigo`)
) ENGINE=InnoDB AUTO_INCREMENT=12772 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `vistaCoto`
--

DROP TABLE IF EXISTS `vistaCoto`;
/*!50001 DROP VIEW IF EXISTS `vistaCoto`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vistaCoto` AS SELECT 
 1 AS `ean`,
 1 AS `nombre`,
 1 AS `marca`,
 1 AS `fabricante`,
 1 AS `categoria`,
 1 AS `subcategoria`,
 1 AS `sku_tienda`,
 1 AS `url_tienda`,
 1 AS `nombre_tienda`,
 1 AS `id`,
 1 AS `tienda_id`,
 1 AS `producto_tienda_id`,
 1 AS `capturado_en`,
 1 AS `precio_lista`,
 1 AS `precio_oferta`,
 1 AS `tipo_oferta`,
 1 AS `promo_tipo`,
 1 AS `promo_texto_regular`,
 1 AS `promo_texto_descuento`,
 1 AS `promo_comentarios`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `vista_editable`
--

DROP TABLE IF EXISTS `vista_editable`;
/*!50001 DROP VIEW IF EXISTS `vista_editable`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vista_editable` AS SELECT 
 1 AS `id`,
 1 AS `ean`,
 1 AS `nombre`,
 1 AS `marca`,
 1 AS `fabricante`,
 1 AS `categoria`,
 1 AS `subcategoria`,
 1 AS `ean_auxiliar`,
 1 AS `nombre_tienda`,
 1 AS `url_tienda`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `vistaCoto`
--

/*!50001 DROP VIEW IF EXISTS `vistaCoto`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`userscrap`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `vistaCoto` AS select `productos`.`ean` AS `ean`,`productos`.`nombre` AS `nombre`,`productos`.`marca` AS `marca`,`productos`.`fabricante` AS `fabricante`,`productos`.`categoria` AS `categoria`,`productos`.`subcategoria` AS `subcategoria`,`producto_tienda`.`sku_tienda` AS `sku_tienda`,`producto_tienda`.`url_tienda` AS `url_tienda`,`producto_tienda`.`nombre_tienda` AS `nombre_tienda`,`hp`.`id` AS `id`,`hp`.`tienda_id` AS `tienda_id`,`hp`.`producto_tienda_id` AS `producto_tienda_id`,`hp`.`capturado_en` AS `capturado_en`,`hp`.`precio_lista` AS `precio_lista`,`hp`.`precio_oferta` AS `precio_oferta`,`hp`.`tipo_oferta` AS `tipo_oferta`,`hp`.`promo_tipo` AS `promo_tipo`,`hp`.`promo_texto_regular` AS `promo_texto_regular`,`hp`.`promo_texto_descuento` AS `promo_texto_descuento`,`hp`.`promo_comentarios` AS `promo_comentarios` from (((`historico_precios` `hp` join (select `historico_precios`.`producto_tienda_id` AS `producto_tienda_id`,max(`historico_precios`.`capturado_en`) AS `ult_fecha` from `historico_precios` where (`historico_precios`.`tienda_id` = 2) group by `historico_precios`.`producto_tienda_id`) `ult` on(((`hp`.`producto_tienda_id` = `ult`.`producto_tienda_id`) and (`hp`.`capturado_en` = `ult`.`ult_fecha`)))) join `producto_tienda` on((`hp`.`producto_tienda_id` = `producto_tienda`.`id`))) left join `productos` on((`productos`.`id` = `producto_tienda`.`producto_id`))) where (`hp`.`tienda_id` = 2) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vista_editable`
--

/*!50001 DROP VIEW IF EXISTS `vista_editable`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`userscrap`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `vista_editable` AS select `productos`.`id` AS `id`,`productos`.`ean` AS `ean`,`productos`.`nombre` AS `nombre`,`productos`.`marca` AS `marca`,`productos`.`fabricante` AS `fabricante`,`productos`.`categoria` AS `categoria`,`productos`.`subcategoria` AS `subcategoria`,`productos`.`ean_auxiliar` AS `ean_auxiliar`,`producto_tienda`.`nombre_tienda` AS `nombre_tienda`,`producto_tienda`.`url_tienda` AS `url_tienda` from (`productos` join `producto_tienda` on((`producto_tienda`.`producto_id` = `productos`.`id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-01-08 13:53:30
