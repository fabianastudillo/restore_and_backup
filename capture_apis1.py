#!/usr/bin/env python3.12

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
MODBUS_IP_APIS1 = config['modbus_ip_apis1']
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

        if tab == "apis1_ifv1" or tab == "apis1_ifv2":
            insert_query = f"""
            INSERT INTO {tab} (
                timestamp, Status_Conversor, DC_Voltage_of_Inverter, DC_Current_of_Inverter,
                DC_Power_of_Inverter, Phase_Voltage_R_of_Inverter, Phase_Voltage_S_of_Inverter,
                Phase_Voltage_T_of_Inverter, Line_Voltage_RS_of_Inverter, Line_Voltage_ST_of_Inverter,
                Line_Voltage_TR_of_Inverter, Line_Current_R_of_Inverter, Line_Current_S_of_Inverter,
                Line_Current_T_of_Inverter, Active_Power_Phase_R, Active_Power_Phase_S,
                Active_Power_Phase_T, Reactive_Power_Phase_R, Reactive_Power_Phase_S,
                Reactive_Power_Phase_T, Total_Active_Power, Total_Reactive_Power,
                Total_Apparent_Power, Power_Factor, Freq_System
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (timestamp, registers[0], registers[1]/10, registers[2]/10, registers[3]/10,
                    registers[4]/10, registers[5]/10, registers[6]/10, registers[7]/10, registers[8]/10,
                    registers[9]/10, registers[10]/10, registers[11]/10, registers[12]/10, registers[13],
                    registers[14], registers[15], registers[16], registers[17], registers[18], registers[19],
                    registers[20], registers[21], registers[22], registers[23]/10)

        elif tab == "apis1_ifv3":
            insert_query = f"""
            INSERT INTO {tab} (
                timestamp, Status_Conversor, DC_Voltage_of_Inverter, DC_Current_of_Inverter,
                DC_Power_of_Inverter, Phase_Voltage_R_of_Inverter, Phase_Voltage_S_of_Inverter,
                Phase_Voltage_T_of_Inverter, Line_Voltage_RS_of_Inverter, Line_Voltage_ST_of_Inverter,
                Line_Voltage_TR_of_Inverter, Line_Current_R_of_Inverter, Line_Current_S_of_Inverter,
                Line_Current_T_of_Inverter, Active_Power_Phase_R, Active_Power_Phase_S,
                Active_Power_Phase_T, Reactive_Power_Phase_R, Reactive_Power_Phase_S,
                Reactive_Power_Phase_T, Total_Active_Power, Total_Reactive_Power,
                Total_Apparent_Power, Freq_System
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (timestamp, registers[0], registers[1]/100, registers[2]/100, registers[3]/100,
                    registers[4]/100, registers[5]/100, registers[6]/100, registers[7]/100, registers[8]/100,
                    registers[9]/100, registers[10]/100, registers[11]/100, registers[12]/100, registers[13],
                    registers[14], registers[15], registers[16], registers[17], registers[18],
                    registers[19]/100, registers[20]/100, registers[21]/100, registers[22]/100)

        with conn.cursor() as cursor:
            cursor.execute(insert_query, data)
        conn.commit()
    except (InterfaceError, OperationalError) as e:
        log.error(f"Conexión perdida al insertar en {tab}: {e}")
        raise
    except Exception as e:
        log.error(f"Error general al insertar en {tab}: {e}")

def main():
    conn = connect_postgres()
    client = ModbusTcpClient(MODBUS_IP_APIS1, port=MODBUS_PORT, timeout=3)

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
            if not client.connect():
                log.error("No se pudo conectar al dispositivo Modbus")
                time.sleep(5)
                continue

            try:
                response1 = client.read_holding_registers(address=1, count=24)
                response2 = client.read_holding_registers(address=101, count=24)
                response3 = client.read_holding_registers(address=300, count=23)

                if response1.isError():
                    log.error(f"Error en lectura IFV1: {response1}")
                if response2.isError():
                    log.error(f"Error en lectura IFV1: {response2}")
                if response3.isError():
                    log.error(f"Error en lectura IFV1: {response3}")
                else:
                    if not response1.isError():
                        insert_data(conn, response1.registers, "apis1_ifv1")
                    if not response2.isError():
                        insert_data(conn, response2.registers, "apis1_ifv2")
                    if not response3.isError():
                        insert_data(conn, response3.registers, "apis1_ifv3")

                time.sleep(0.1165)

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
        client.close()
        if conn and not conn.closed:
            conn.close()
        log.info("Conexiones cerradas")

if __name__ == "__main__":
    main()
