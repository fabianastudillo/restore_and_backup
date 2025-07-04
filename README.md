# Diseño e implementaciónde un sistema de gestion de respaldos y recuperación enfocado en la Micro-Red de la Universidad de Cuenca

Este repositorio contiene los scripts en Python diseñados para la adquisición y almacenamiento de datos provenientes de las API 1, 2 y 3 del laboratorio de Micro-Red de la Universidad de Cuenca, mediante el protocolo Modbus TCP/IP y su posterior registro en una base de datos PostgreSQL. Además se incluye los scripts implementados para la generación y carga del respaldo lógico .sql. El script `backup_dbscada.py` es ejecutado por la herramienta Bacula previo a un backup job y el script `restore_dbscada.py` es ejecutado después de un restore job por la misma herramienta.

## 📂 **Docker File**
- Se define los 3 servicios a utilizar: Bacula, PostgreSQL y Grafana.
- Se exponen puertos.
- Se definen volúmenes para persistencia de datos.
- Se definen variables de ambiente.
## 📂 **Scripts incluidos**

### `capture_apis1.py`
- Conecta la APIS1 mediante Modbus TCP/IP.
- Lee registros de distintos bloques de memoria (IFV1, IFV2, IFV3).
- Almacena los datos en tablas correspondientes (`apis1_ifv1`, `apis1_ifv2`, `apis1_ifv3`) de PostgreSQL.
- Intervalo de lectura: 110 ms.

---

### `capture_apis2.py`
- Conecta a cuatro dispositivos:
  - APIS2_PB
  - APIS2_LI
  - APIS2_RDX
  - APIS2_SC
- Lee bloques de registros específicos de cada uno.
- Almacena datos en tablas correspondientes en PostgreSQL.
- Intervalo de lectura: 1000 ms (1 segundo).

---

### `capture_apis3.py`
- Conecta a un controlador de motores (APIS3).
- Lee dos bloques de registros que representan dos motores (Motor1 y Motor2).
- Inserta datos en las tablas `apis3_motor1` y `apis3_motor2`.
- Intervalo de lectura: 500 ms.

---

### `backup_dbscada.py`
- Genera el respaldo lógico de la base de datos de las API 1, 2, 3.
- Como salida se obtiene un archivo .sql

---

### `restore_dbscada.py`
- Carga el respaldo lógico, permite la restauración de la base de datos.

---

## ⚙️ **Requisitos**

- Python 3.12
- Paquetes:
  - `pymodbus`
  - `psycopg2`
- Base de datos PostgreSQL funcionando y accesible.
- Archivo de configuración JSON (`config.json`) con parámetros como:
  ```json
  {
    "db_host": "192.168.x.x",
    "db_name": "date_base_name",
    "db_user": "user",
    "db_password": "password",
    "modbus_ip_apis1": "192.168.x.x",
    "modbus_ip_apis2_pb": "192.168.x.x",
    "modbus_ip_apis2_li": "192.168.x.x",
    "modbus_ip_apis2_rdx": "192.168.x.x",
    "modbus_ip_apis2_sc": "192.168.x.x",
    "modbus_ip_apis3": "192.168.x.x",
    "modbus_port": 502
    "output_file": "/home/administrador/scripts/db_scada.sql"
  }
