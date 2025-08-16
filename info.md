
 ✅
Tareas que incluye la propuesta
Archivo fuente
Descripción:
Necesito un software para extraer datos de 19 tiendas online de supermercados (a futuro sumaremos otras) 
 Los productos deberían poder ser variables según la necesidad. Y la opción de bajar todos los productos de
 cada tienda o un listado. Es crucial que se incluya el código EAN de cada producto. Algunas páginas, como Carrefour,
 lo proporcionan, pero otras no, por lo que deberíamos tener tablas de conversión para cada tienda online (que serian editadas una vez para que luego machee 
 con los productos y genere el reporte). Este desarrollo es necesario.
Como opción, poder programar estos scraping eligiendo, las webs, los dias, la hora. 
 Adjunto formato de salida de informacion (son el formatos de archivo). Y esta aplicación se deberia poder ejecutar web,
 hosteada en un VPS (de mi propiedad). Las páginas son las siguientes:

Entregables y
documentación
Documento de especificación de requerimientos.
Scripts de scraping.
Interfaz grafica para la automatización de procesos de
scraping.
Documentación técnica de la solución.
Requerimientos
Desarrollar interfaz gráfica para la creación de jobs
(tareas/procesos) que permitan schedulear la ejecución de
los mismos:
Se debe poder crear un nuevo job y asignarle un nombre.
Se debe poder configurar los días a ejecutarse.
Se debe poder configurar el horario a ejecutarse.
Se debe poder ejecutar manualmente el job.
Se puede editar el job para cambiar días, horarios, tiendas
y/o productos.
Se debe poder habilitar/deshabilitar un job.
Se debe poder seleccionar la/s tienda/s a scrapear desde
una lista.
Se debe poder seleccionar scrapear todo o parcial
(algunos EAN), en caso de seleccionar parcial debe poder
subir un archivo excel con los códigos.
El output deberá ser un archivo por tienda con un formato
a definir, ejemplo: Listado_MarianoMax_20230328.
El archivo de salida debe contener las siguientes columnas:
EAN, Código Interno, Nombre Producto, Categoría,
Subcategoría, Marca, Fabricante, Precio de Lista, Precio de
Oferta, tipo de Ofertay, URL.
10
Debemos tener un script flexible que pueda mapear los
códigos internos a un EAN.
El/los jobs deberán ejecutarse dentro de la VPS.
A continuación se detalla el listado de tiendas y campos a
scrapear.
Hay tiendas que n informan el codigo ean, se debera realizar un mapero (podria ser tablas de conversion donde los codigos ean se agreguen manualmente una unica vez para que queden relacionados)
tiendas:
opcion 1:     orden    web
1    mas online    masonline.com.ar ✅
2    Coto    https://www.Cotodigital3.com.ar/ ✅
3    Dia    https://diaonline.Supermercadosdia.com.ar/ ✅
4    Vea    vea.Com.ar ✅
5    Jumbo    jumbo.Com.ar ✅
6    Carrefour    carrefour.Com.ar ✅
7    Disco    disco.Com.ar✅
8    La Anonima    https://supermercado.Laanonimaonline.com
9    Libertad    www.Hiperlibertad.com.ar
10    Cooperativa obrera    https://www.lacoopeencasa.coop
11    Toledo    https://toledodigital.Com.ar/storeview_jara/
12    La Gallega    https://www.Lagallega.com.ar/login.asp
13    La Reina    https://www.Lareinaonline.com.ar/login.asp
14    Alvear    https://www.Alvearonline.com.ar/#/
15    Comodin    https://www.Comodinencasa.com.ar
16    El Abastecedor    https://www.Elabastecedor.com.ar
17    Kilbel    https://www.Kilbelonline.com
18    Atomo    https://atomoconviene.Com/atomo-ecommerce/
19    Pingüino    https://www.Pinguino.com.ar/web/index.r
20    Josimar    https://www.Josimar.com.ar
21    Cordiez    https://www.Cordiez.com.ar
22    Modo Market    https://www.Modomarket.com
23    Dono    https://www.Dinoonline.com.ar/
24    La Genovesa    https://www.lagenovesadigital.com.ar/Home
25    Dar    https://www.darentucasa.com.ar/
26    Rosental:    https://www.pedidosya.com.ar/restaurantes/rosario/supermercado-dar-centro-2-menu?origin=shop_list
27    El Puente:    http://ofertas.lacteoselpuente.com.ar/#listado-de-precios
28    Yaguar    shop.yaguar.com.ar
29 Rappi https://www.rappi.com.ar/tiendas/tipo/market


Opcion 2:

    Clientes a relevar    web
1    inc s.A. (0007600)    carrefour.com.ar ✅
2    COOP.OBR.Lim.de consumo (0003563)    https://www.lacoopeencasa.coop/ ✅
3    COTO C.I.C.S.A. (0007013)    https://www.cotodigital3.com.ar/ ✅
4    CENCOSUD S.A. (0041496)    https://www.jumbo.com.ar/ ✅
5    MILLAN S.A. (0006038)    https://atomoconviene.com/atomo-ecommerce/
6    la gallega supermercados s.A. (0001764)    www.lagallega.com.ar ✅
7    S.A. IMP.Y Exp.de la pat. (0004017)    supermercado.laanonimaonline.com
8    LIBERTAD S.A. (0005320)    www.hiperlibertad.com.ar✅
9    la reina s.A. (0011318)    https://www.lareinaonline.com.ar/✅
10    dia argentina s.A. (0007583)    https://diaonline.supermercadosdia.com.ar/ ✅
11    D.rosental e hijos saci (0011176)    https://www.pedidosya.com.ar/restaurantes/rosario/supermercado-dar-centro-2-menu?origin=shop_list
12    DINOSAURIO S.A. (0036838)    https://www.dinoonline.com.ar/✅
13    usina lactea el puente s.A.(1) (0007716)    http://ofertas.lacteoselpuente.com.ar/#listado-de-precios
14    ALBERDI S.A.(N) (COMODIN I) (0014674)    https://www.comodinencasa.com.ar/
15    Cyre (5812)    https://www.cordiez.com.ar/
16    Antoniazzi (1133)    www.alvearonline.com.ar
17    supermercados el abastecedor s.A. (0041320)    https://www.elabastecedor.com.ar/index.php
18    la genovesa supermercado s.A. (0025968)    https://www.lagenovesadigital.com.ar/Home
19    SUP.MAY.YAGUAR S.A. (0002404)    shop.yaguar.com.ar
20 Rappi https://www.rappi.com.ar/tiendas/tipo/market


Enviar opciones para 1 y 2 y tiempos de desarrollo de cada una en 2 etapas cada una. La primera es obtener los datos solicitados y armar el reporte, la segunda etapa son las automatizaciones.

Categoría: Programación y Tecnología
Subcategoría: Programación Web
¿Cuál es el alcance del proyecto?: Crear un nuevo sitio personalizado
¿Es un proyecto o una posición?: Un proyecto
Actualmente tengo: Tengo las especificaciones
Disponibilidad requerida: Según se necesite
Roles necesarios: Programador


