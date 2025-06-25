#!/bin/env python3.12

from pymodbus.client import ModbusTcpClient
import psycopg2
from psycopg2 import OperationalError, InterfaceError
from datetime import datetime
import time
import logging
import json

# Configuración de logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

CONFIG_FILE = '/home/administrador/scripts/config.json'

with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)

# Configuración de la base de datos PostgreSQL
DB_CONFIG = {
    'host': config['db_host'],
    'database': config['db_name'],
    'user': config['db_user'],
    'password': config['db_password']
}

# Configuración Modbus
MODBUS_IP_APIS3 = config['modbus_ip_apis3']
MODBUS_PORT = config['modbus_port']

def connect_postgres():
    """Intenta conectar a PostgreSQL y retorna la conexión"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Conexión a PostgreSQL establecida.")
        return conn
    except Exception as e:
        log.error(f"Error al conectar a PostgreSQL: {e}")
        return None

def insert_data(conn, registers, tab):
    try:
        timestamp = datetime.now()
        if tab == "apis3_motor1" or "apis3_motor2":
            insert_query = f"""
            INSERT INTO {tab} (timestamp, Estado_OP_motor, Piloto_filtro, Piloto_exp_gases, Estado_conex, Presion_aceite, Temp_refrigerante, Temp_aceite, Consumo_combustible, Nivel_combustible, V_carga_alternador, V_bat_arranque, Vel_giro_motor, Freq_giro_gen, Compen_I_gen, Fase_rot_gen, Freq_giro_suministro, Compen_I_suministro, Fase_rot_suministro, Freq, Flag_0, Flag_2, V_gen_L1_N, V_gen_L2_N, V_gen_L3_N, V_gen_L1_L2, V_gen_L2_L3, V_gen_L3_L1, I_gen_L1_N, I_gen_L2_N, I_gen_L3_N, I_tierra_gen, P_gen_L1, P_gen_L2, P_gen_L3, V_suministro_L1_N, V_suministro_L2_N, V_suministro_L3_N, V_suministro_L1_L2, V_suministro_L2_L3, V_suministro_L3_L1, I_suministro_L1, I_suministro_L2, I_suministro_L3, I_tierra_suministro, P_suministro_L1, P_suministro_L2, P_suministro_L3, P_Total)
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            data = (timestamp, registers[0], registers[1], registers[2], registers[3], registers[4], registers[5], registers[6], registers[7], registers[8], registers[9]/100, registers[10]/100, registers[11], registers[12]/100, registers[13], registers[14], registers[15]/100, registers[16], registers[17], registers[18]/100, registers[19], registers[20], registers[21]/100, registers[23]/100, registers[25]/100, registers[27]/100, registers[29]/100, registers[31]/100, registers[33]/100, registers[35]/100, registers[37]/100, registers[39]/100, registers[41], registers[43], registers[45], registers[47]/100, registers[49]/100, registers[51]/100, registers[53]/100, registers[55]/100, registers[57]/100, registers[59]/100, registers[61]/100, registers[63]/100, registers[65], registers[67], registers[69], registers[71], registers[73])

        with conn.cursor() as cursor:
            cursor.execute(insert_query, data)
        conn.commit()
    except (InterfaceError, OperationalError) as e:
        log.error(f"Conexión perdida al insertar en {tab}: {e}")
        raise
    except Exception as e:
        log.error(f"Error general al insertar en {tab}: {e}")


def main():
    # Conexión con la base de datos
    conn = connect_postgres()
    # Configuración del cliente Modbus
    apis3 = ModbusTcpClient(host=MODBUS_IP_APIS3, port=MODBUS_PORT, timeout=3)

    try:
        while True:
            # Verifica conexión a PostgreSQL
            if conn is None or conn.closed:
                log.warning("Conexión a PostgreSQL cerrada, intentando reconectar...")
                conn = connect_postgres()
                if conn is None:
                    time.sleep(5)
                    continue

            # Verifica conexión Modbus
            if not apis3.connect():
                log.error("No se pudo conectar al dispositivo Modbus")
                time.sleep(5)
                continue

            try:
                # Leer registros Modbus
                response1 = apis3.read_holding_registers(address=0, count=74)
                response2 = apis3.read_holding_registers(address=94, count=74)

                if response1.isError():
                    log.error(f"Error Modbus: {response1}")
                if response2.isError():
                    log.error(f"Error Modbus: {response2}")
                else:
                    insert_data(conn, response1.registers, "apis3_motor1")
                    insert_data(conn, response2.registers, "apis3_motor2")

                # Esperar antes de la próxima lectura
                time.sleep(1)

            except (InterfaceError, OperationalError) as db_error:
                log.error(f"Error en la lectura/inserción. PostgreSQL podría estar caído: {db_error}")
                conn = None  # Fuerza reconexión
                time.sleep(5)
            except Exception as e:
                log.error(f"Error inesperado: {e}")
                time.sleep(5)

    except KeyboardInterrupt:
        log.info("Deteniendo el script...")

    finally:
        apis3.close()
        if conn and not conn.closed:
            conn.close()
        log.info("Conexiones cerradas")

if __name__ == "__main__":
    main()