# Inicio rápido de Confluent Cloud

Confluent Cloud es una plataforma de streaming de datos completamente gestionada y nativa en la nube, basada en Apache Kafka®. Esta guía rápida te ayuda a comenzar a transmitir datos mostrándote cómo crear un clúster, agregar un tópico y producir datos.

## ¿Por qué usar Confluent Cloud en lugar de administrar Kafka por tu cuenta?

Confluent Cloud elimina la complejidad operativa de Kafka con:

- Escalado automático y confiabilidad
- Seguridad, monitoreo y gestión de esquemas integrados
- Menor costo total de propiedad y desarrollo más rápido

---

## La página Home de la consola

Al iniciar sesión en Confluent Cloud, la página Home muestra widgets para volver a los recursos que más utilizás y para monitorear tus entornos:

| Widget | Descripción |
|--------|-------------|
| **Resumen de recursos** | Muestra cuántos entornos, clústeres, tópicos, conectores, cluster links, compute pools y SQL workspaces tenés. Seleccioná un tipo de recurso para ir a su lista. |
| **Acción requerida** | Lista los recursos que necesitan tu atención, como recursos inactivos. Seleccioná **View all** para ver la lista completa. |
| **Favoritos** | Lista los recursos que marcaste como favoritos (entornos, clústeres, tópicos o conectores) para volver a ellos directamente desde Home. Para agregar un favorito, seleccioná **Add favorite**, buscá y elegí el recurso. Para quitarlo, seleccioná **Edit** y luego **Remove from favorites**. |
| **Visitados recientemente** | Lista los recursos a los que accediste más recientemente, con el tiempo transcurrido desde la visita. Seleccioná **View all** para buscar en todo tu historial. |
| **Descubrir datos** | Muestra los tópicos creados recientemente en un entorno seleccionado. Seleccioná **Explore more** para explorar tus datos en Data Portal. |
| **Data health score** | Muestra una puntuación basada en cuántos de tus tópicos tienen un esquema registrado, y lista los tópicos que no lo tienen. |
| **Blogs** | Muestra publicaciones recientes del blog de Confluent. |

---

## Consideraciones antes de comenzar

- Esta guía rápida te pone en marcha con Confluent Cloud usando un clúster Kafka básico.
- La primera sección muestra cómo usar Confluent Cloud para crear tópicos, y producir y consumir datos hacia y desde el clúster.
- La segunda sección te guía en el uso de Confluent Cloud para Apache Flink® para ejecutar consultas sobre los datos usando sintaxis SQL.
- Los flujos de trabajo asumen que ya contás con un entorno de Confluent Cloud funcionando, que incluye un paquete de Stream Governance.
- Confluent Cloud ofrece una interfaz web, una interfaz de línea de comandos local y APIs REST para administrar tus entornos y recursos.

## Prerrequisitos

- Acceso a Confluent Cloud. Para comenzar de forma gratuita, registrate en la prueba gratuita de Confluent Cloud.
- Conectividad a Internet
- Paquete de Stream Governance

---

## Sección 1: Crear un clúster y agregar un tópico

Esta sección configura un clúster Kafka en Confluent Cloud y muestra cómo producir datos hacia un tópico usando la consola web.

### Paso 1: Crear un clúster Kafka en Confluent Cloud

En este paso creás y lanzás un clúster Kafka básico dentro de tu entorno por defecto.

1. Iniciá sesión en Confluent Cloud en [https://confluent.cloud](https://confluent.cloud).
2. Hacé clic en **+ Add cluster**.
3. En la página **Create cluster**, especificá un nombre para el clúster.
4. En **Cluster type**, seleccioná **Basic**.

   > Este ejemplo crea un clúster Kafka Basic, que soporta disponibilidad en zona única. Para información sobre otros tipos de clúster, consultá la documentación de tipos de clúster de Confluent Cloud.

5. En **Provider and region**, seleccioná un proveedor de nube y una región.
6. Revisá el resumen de costos y seleccioná **Launch cluster**.

   > Si no configuraste un método de pago, agregá uno o ingresá un código promocional antes de poder lanzar el clúster.

Después de que tu clúster se aprovisione, se muestra la página **Cluster Overview**. El tiempo de aprovisionamiento varía según el proveedor de nube y puede tardar algunos minutos.

Ahora podés configurar aplicaciones y datos en tu nuevo clúster.

---

### Paso 2: Crear un tópico Kafka

En este paso creás un tópico Kafka llamado `users` usando la consola web. Un tópico es una unidad de organización de un clúster, y es esencialmente un log de solo apendado.

1. En el menú de navegación, hacé clic en **Topics** y luego en **Create topic**.
2. En el campo **Topic name**, escribí `users` y seleccioná **Create with defaults**.

El tópico `users` queda creado en el clúster Kafka y disponible para productores y consumidores.

> El mensaje de éxito puede sugerirte realizar una acción; continuá con el **Paso 3: Crear un productor de ejemplo**.

---

### Paso 3: Crear un productor de ejemplo

Un conector es un componente enchufable que transmite datos hacia o desde Kafka. Este paso usa el conector **Datagen Source** alojado en Confluent Cloud, que genera datos de ejemplo, para producir datos hacia tu clúster Kafka.

1. En el menú de navegación, seleccioná **Connectors**.
2. En el cuadro de búsqueda, escribí `datagen`.
3. En los resultados, seleccioná el conector **Datagen Source**.

   > Si aparece el cuadro **Launch Sample Data**, seleccioná **Additional configuration**.

4. En el diálogo **Launch Sample Data**, seleccioná la plantilla **Users** y hacé clic en **Additional configuration**.
5. En el diálogo de selección de tópico, seleccioná el tópico `users` que creaste en la sección anterior y hacé clic en **Continue**.
6. En el diálogo de credenciales Kafka, seleccioná **My account** y hacé clic en **Generate API key & download**, luego en **Continue**.

   > Esto crea una API key y un secret que permiten al conector acceder a tu clúster, y los descarga a tu computadora. La key y el secret son necesarios para el conector y también para la Confluent CLI y la ksqlDB CLI.
   >
   > Una API key y su secret asociado aplican al clúster Kafka activo. Si agregás un nuevo clúster, debés crear una nueva API key para productores y consumidores en ese clúster.

7. En la página **Configuration**, seleccioná `JSON_SR` para el formato del valor del registro de salida, seleccioná la plantilla **Users** y hacé clic en **Continue**.
8. Para el tamaño del conector, dejá el slider en el valor por defecto de 1 tarea y hacé clic en **Continue**.
9. En la página **Review and launch**, seleccioná el texto en el campo **Connector name** y reemplazalo por `DatagenSourceConnector_users`.
10. Hacé clic en **Continue** para iniciar el conector.

El estado del nuevo conector debería mostrar **Provisioning** por unos segundos. Cuando el estado cambie a **Running**, el conector está produciendo datos hacia el tópico `users`.

---

### Paso 4: Ver los mensajes

Tu nuevo tópico `users` ya está recibiendo mensajes. Usá la consola de Confluent Cloud para ver los datos.

1. En el menú de navegación, seleccioná **Topics** para mostrar la lista de tópicos de tu clúster.
2. Seleccioná el tópico `users`.
3. En la página de detalle del tópico `users`, seleccioná **Messages** para ver los mensajes que se están produciendo.

---

### Paso 5: Ver el esquema del tópico

Cuando el conector del Paso 3 produce datos hacia el tópico `users`, registra un esquema para el tópico en Schema Registry, la base de Stream Governance. El esquema mantiene los datos del tópico consistentes y compatibles a medida que las aplicaciones evolucionan.

1. En el menú de navegación, seleccioná **Topics** y luego el tópico `users`.
2. En la página de detalle del tópico `users`, hacé clic en **Schema** para ver el esquema registrado para el tópico.

El esquema lista cada campo y su tipo, definiendo la estructura que acuerdan productores y consumidores.

---

### Paso 6: Inspeccionar el flujo de datos

Usá **Stream Lineage** para rastrear el movimiento de los datos a través de tu clúster.

1. Hacé clic en **Lineage** en el menú de navegación.
2. Hacé clic en el nodo **DatagenSourceConnector_users**, que es el conector que creaste en el Paso 3. El panel de detalle muestra gráficos de producción total y otros datos.
3. Cerrá esta vista y seleccioná el nodo del tópico `users`. Hacé clic en **Overview** para ver gráficos de throughput total y otros datos. Hacé clic en las demás pestañas para ver más detalles del tópico:
   - **Schema** (si hay uno definido)
   - **Metadata**
   - **Messages**
4. Hacé clic en la flecha en el borde del canvas para volver a abrir el menú de navegación.

---

### Paso 7: Eliminar recursos (opcional)

Si planeás continuar con la **Sección 2: Consultar datos de streaming con Flink SQL**, saltá este paso. De lo contrario, eliminá los recursos que creaste para evitar cargos inesperados en tu cuenta.

**Eliminar el conector:**
1. En el menú de navegación, seleccioná **Connectors**.
2. Hacé clic en **DatagenSourceConnector_users** y elegí la pestaña **Settings**.
3. Hacé clic en **Delete connector**, ingresá el nombre del conector (`DatagenSourceConnector_users`) y hacé clic en **Confirm**.

**Eliminar el tópico:**
1. En el menú de navegación, hacé clic en **Topics**, seleccioná el tópico `users` y elegí la pestaña **Settings**.
2. Hacé clic en **Delete topic**, ingresá el nombre del tópico (`users`) y seleccioná **Continue**.

**Eliminar el clúster:**
1. En el menú de navegación, seleccioná **Cluster** y elegí la pestaña **Settings**.
2. Hacé clic en **Delete cluster**, ingresá el nombre del clúster y hacé clic en **Continue**.

---

## Sección 2: Consultar datos de streaming con Flink SQL

En esta sección creás un workspace de Flink y escribís consultas sobre el tópico `users` y otros datos de streaming. En la Sección 1 instalaste un conector Datagen que produjo esos datos al tópico `users` en tu clúster de Confluent Cloud.

### Paso 1: Crear un workspace de Flink

Un workspace de Flink es un editor SQL interactivo para escribir y ejecutar consultas sobre datos de streaming. Creá un nuevo workspace para comenzar.

1. Navegá a la página **SQL Workspaces** y en el menú de navegación hacé clic en **Stream processing**.
2. En el dropdown, seleccioná el entorno donde creaste el tópico `users` y el conector Datagen Source.
3. Hacé clic en **Create workspace**.

   > Para experimentar con flujos de datos de ejemplo provistos por Confluent Cloud para Apache Flink, hacé clic en **Try example** para abrir un workspace curado con consultas Flink SQL de ejemplo que podés ejecutar inmediatamente.

Se abre un nuevo workspace con una consulta de ejemplo en el editor de código (celda).

En segundo plano, Confluent Cloud para Apache Flink crea un **compute pool**, que representa los recursos de cómputo usados para ejecutar tus sentencias SQL. Puede tardar un minuto o dos en aprovisionarse. El estado se muestra en la esquina superior derecha del workspace.

---

### Paso 2: Ejecutar sentencias Flink SQL

El compute pool está listo para ejecutar consultas cuando su estado cambia de **Provisioning** a **Running**.

1. Hacé clic en **Run** para enviar la consulta de ejemplo.

   Se envía la sentencia de ejemplo y se muestra información sobre ella, incluyendo su estado y un identificador único. Hacé clic en el nombre de la sentencia para abrir la vista de detalles. Hacé clic en **X** para cerrar esa vista.

   Después de un período de inicialización, los resultados de la consulta se muestran debajo de la celda. La salida debería ser similar a:

   ```
   EXPR$0
   0
   1
   2
   ```

2. Borrá la consulta anterior de la celda y ejecutá la siguiente para inspeccionar el stream `users`.

   Confluent Cloud para Apache Flink registra tablas automáticamente sobre tus tópicos Kafka. La consulta corre contra la tabla `users` con los datos de streaming del tópico subyacente.

   ```sql
   SELECT * FROM users;
   ```

   La salida debería ser similar a:

   ```
   key             registertime  userid regionid gender
   x'557365725f34' 1502088104187 User_4 Region_1 MALE
   x'557365725f32' 1500243991207 User_2 Region_9 FEMALE
   x'557365725f32' 1497969328414 User_2 Region_9 OTHER
   ...
   ```

3. Hacé clic en **Stop** para finalizar la consulta.

   Los datos siguen fluyendo desde el conector Datagen hacia la tabla `users` aunque la consulta SELECT esté detenida.

---

### Paso 3: Enmascarar un campo

Con un compute pool de Flink en ejecución, podés correr consultas SQL sobre tus datos de streaming.

En este paso ejecutás una sentencia Flink SQL para ocultar información personal en el stream `users` y publicar los datos depurados en un nuevo tópico Kafka llamado `users_mask`.

1. Ejecutá la siguiente sentencia para crear una nueva tabla Flink basada en la tabla `users`:

   ```sql
   CREATE TABLE users_mask LIKE users;
   ```

2. Cuando el estado de la sentencia anterior sea **Completed**, ejecutá la siguiente sentencia para inspeccionar el esquema de la tabla `users_mask`:

   ```sql
   DESCRIBE users_mask;
   ```

   La salida debería ser similar a:

   ```
   +--------------+-----------+----------+------------+
   | Column Name  | Data Type | Nullable |   Extras   |
   +--------------+-----------+----------+------------+
   | key          | BYTES     | NULL     | BUCKET KEY |
   | registertime | BIGINT    | NULL     |            |
   | userid       | STRING    | NULL     |            |
   | regionid     | STRING    | NULL     |            |
   | gender       | STRING    | NULL     |            |
   +--------------+-----------+----------+------------+
   ```

3. Ejecutá la siguiente sentencia para iniciar una **consulta persistente** que usa la función `REGEXP_REPLACE` de Flink para enmascarar el valor del campo `gender` y enviar los resultados a la tabla `users_mask`:

   ```sql
   INSERT INTO users_mask SELECT
     `key`,
     registertime,
     userid,
     regionid,
     REGEXP_REPLACE(gender, '(\w)', '*') as gender
   FROM users;
   ```

   La sentencia `INSERT INTO ... FROM SELECT` se ejecuta de forma continua hasta que la detengas manualmente.

4. Hacé clic en el ícono **Add new cell** para crear una nueva celda y ejecutá la siguiente sentencia para inspeccionar las filas de la tabla `users_mask`:

   ```sql
   SELECT * FROM users_mask;
   ```

   La salida debería ser similar a:

   ```
   key             registertime  userid regionid gender
   x'557365725f34' 1488737391835 User_4 Region_5 *****
   x'557365725f34' 1499070045309 User_4 Region_5 *****
   x'557365725f32' 1505447077187 User_2 Region_7 *****
   x'557365725f34' 1505592707164 User_4 Region_2 *****
   ...
   ```

5. Hacé clic en **Stop** para finalizar la sentencia SELECT.

   La sentencia `INSERT INTO` que iniciaste antes continúa enviando datos al tópico `users_mask`.

---

### Paso 4: Ver el Stream Lineage

Tus sentencias Flink SQL son recursos en Confluent Cloud, al igual que tópicos y conectores, por lo que podés verlos en Stream Lineage.

1. En tu workspace, abrí el navegador de catálogo y expandí tu entorno, clúster y **Tables** para encontrar el tópico `users_mask`.
2. Pasá el cursor sobre el tópico `users_mask` y hacé clic en **View topic details**.
3. En el panel de detalles del tópico, bajo **Lineage**, hacé clic en **View full lineage**.
4. Pasá el cursor sobre los nodos en el diagrama de Stream Lineage para ver los detalles del flujo de datos.

---

### Paso 5: Eliminar recursos

Cuando terminés el inicio rápido, eliminá los recursos que creaste para evitar cargos inesperados en tu cuenta.

**Eliminar la consulta persistente:**
1. Navegá a la página de detalles de tu entorno y hacé clic en **Flink**.
2. En la lista de sentencias, encontrá la sentencia con estado **Running**.
3. Seleccioná el checkbox de la sentencia, hacé clic en **Actions** y seleccioná **Delete statement**.
4. En el diálogo de confirmación, escribí `DELETE` y hacé clic en **Confirm**.

**Eliminar el conector:**
1. En el menú de navegación, seleccioná **Connectors**.
2. Hacé clic en **DatagenSourceConnector_users** y elegí la pestaña **Settings**.
3. Hacé clic en **Delete connector**, ingresá el nombre del conector (`DatagenSourceConnector_users`) y hacé clic en **Confirm**.

**Eliminar los tópicos:**
1. En el menú de navegación, hacé clic en **Topics**, seleccioná el tópico `users` y elegí la pestaña **Settings**.
2. Hacé clic en **Delete topic**, ingresá el nombre del tópico (`users`) y hacé clic en **Continue**.
3. Repetí estos pasos con el tópico `users_mask`.

---

## Contenido relacionado

- [Procesamiento de streams con Confluent Cloud para Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html)
- [Conectar sistemas externos a Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/overview.html)
- [Inicio rápido para gestión de esquemas en Confluent Cloud](https://docs.confluent.io/cloud/current/get-started/schema-registry.html)
- [Conectar clientes a Confluent Cloud](https://docs.confluent.io/cloud/current/client-apps/overview.html)
- [Guía original en inglés](https://docs.confluent.io/cloud/current/get-started/index.html)

---

*Apache®, Apache Kafka®, Kafka®, Apache Flink® y los nombres de proyectos de código abierto asociados son marcas registradas de la Apache Software Foundation.*
