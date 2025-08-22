# 🛒 Scrapers Supermercados – Actualización de Base de Datos

Este proyecto contiene los **scripts de scraping** para extraer información de productos desde distintos supermercados online y actualizar automáticamente la base de datos en el **VPS**.

---

## 📂 Estructura de carpetas

Cada carpeta corresponde a un supermercado específico. Dentro de ellas se encuentran los scripts y configuraciones necesarias para la extracción.


---

## ⚙️ Instalación en VPS

Los scripts están montados en el servidor **VPS** en la siguiente ruta:

/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/scrap_tiendas/


Cada carpeta contiene el código correspondiente al supermercado y se ejecuta de forma independiente.

---

## 🗄️ Funcionalidad

- Extraer productos, precios y promociones.
- Mapear códigos internos con EAN.
- Insertar o actualizar registros en la base de datos MySQL.
- Generar archivos de salida (CSV/Excel) si es necesario.

---

## ⏱️ Automatización

La ejecución de los scrapers está programada mediante **cron jobs** en el VPS.  
Cada job puede configurarse para:

- Ejecutarse en días y horarios definidos.
- Seleccionar qué tiendas se scrapean.
- Ejecutar manualmente cuando se requiera.

### ⏱️ Cron Jobs – Scrapers Supermercados

| 🏬 Tienda        | ⏰ Horario de ejecución |
|------------------|------------------------|
| Carrefour        | Cada 15 min (00:00, 00:15, 00:30, …) |
| Alvear           | Todos los días a las 03:00 |
| Cordiez          | Cada 15 min (00:00, 00:15, 00:30, …) |
| Coto             | Cada 15 min (00:00, 00:15, 00:30, …) |
| Día              | Cada 15 min (00:00, 00:15, 00:30, …) |
| Dino             | Todos los días a las 03:00 |
| Disco            | Todos los días a las 04:00 |
| HiperLibertad    | Todos los días a las 04:00 y a las 04:45 |
| Jumbo            | Todos los días a las 07:00 |
| Gallega          | Todos los días a las 05:54 |
| La Coope en Casa | Todos los días a las 06:47 |


---

## 🛠️ Requerimientos principales

- **Python 3.12**
- Librerías necesarias:
  - `requests`
  - `beautifulsoup4`
  - `selenium`
  - `pandas`
  - `mysql-connector-python`
  - `webdriver-manager`
- **MySQL 8.x**
- **VPS Linux** con acceso SSH

---

## 🚀 Ejecución manual

Ejemplo (Carrefour):

```bash
cd /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/scrap_tiendas/1_carrefour
python carrefour_scraper.py
