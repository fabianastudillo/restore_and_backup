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
MODBUS_IP_APIS2_PB = config['modbus_ip_apis2_pb']
MODBUS_IP_APIS2_LI = config['modbus_ip_apis2_li']
MODBUS_IP_APIS2_RDX = config['modbus_ip_apis2_rdx']
MODBUS_IP_APIS2_SC = config['modbus_ip_apis2_sc']
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
        if tab == "apis2_pb":
            insert_query = f"""
            INSERT INTO {tab} (timestamp, ACTUAL_MODE, STATUS_CONVERSOR, DC_Voltage_of_Inverter, DC_Current_of_Inverter, DC_Power_of_Inverter, Phase_Voltage_R_of_Inverter, Phase_Voltage_S_of_Inverter, Phase_Voltage_T_of_Inverter, Line_Voltage_RS_of_Inverter, Line_Voltage_ST_of_Inverter, Line_Voltage_TR_of_Inverter, Line_Current_R_of_Inverter, Line_Current_S_of_Inverter, Line_Current_T_of_Inverter, Active_Power_Phase_R, Active_Power_Phase_S, Active_Power_Phase_T, Reactive_Power_Phase_R, Reactive_Power_Phase_S, Reactive_Power_Phase_T, Total_Active_Power, Total_Reactive_Power, Total_Apparent_Power, Power_Factor, Freq_System, SOC, VCELL)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            data = (timestamp, registers[0], registers[1], registers[2]/10, registers[3]/10, registers[4]/10, registers[5]/10, registers[6]/10, registers[7]/10, registers[8]/10, registers[9]/10, registers[10]/10, registers[11]/10, registers[12]/10, registers[13]/10, registers[14], registers[15], registers[16], registers[17], registers[18], registers[19], registers[20], registers[21], registers[22], registers[23], registers[24]/10, registers[31], registers[32])

        elif tab == "apis2_li":
            insert_query = f"""
            INSERT INTO {tab} (timestamp, ACTUAL_mode, STATUS_CONVERSOR, DC_Voltage_of_Inverter, DC_Current_of_Inverter, DC_Power_of_Inverter, Phase_Voltage_R_of_Inverter, Phase_Voltage_S_of_Inverter, Phase_Voltage_T_of_Inverter, Line_Voltage_RS_of_Inverter, Line_Voltage_ST_of_Inverter, Line_Voltage_TR_of_Inverter, Line_Current_R_of_Inverter, Line_Current_S_of_Inverter, Line_Current_T_of_Inverter, Active_Power_Phase_R, Active_Power_Phase_S, Active_Power_Phase_T, Reactive_Power_Phase_R, Reactive_Power_Phase_S, Reactive_Power_Phase_T, Total_Active_Power, Total_Reactive_Power, Total_Apparent_Power, Power_Factor, Freq_System, SOC, SOH, Sys_Voltage, Sys_Current, Sys_Temp_Min, Sys_Temp_Max)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            data = (timestamp, registers[23], registers[24], registers[25]/10, registers[26]/10, registers[27]/10, registers[28]/10, registers[29]/10, registers[30]/10, registers[31]/10, registers[32]/10, registers[33]/10, registers[34]/10, registers[35]/10, registers[36]/10, registers[37], registers[38], registers[39], registers[40], registers[41], registers[42], registers[43], registers[44], registers[45], registers[46], registers[47]/10, registers[5]/10, registers[6]/10, registers[7]/10, registers[8]/10, registers[9]/100, registers[10]/100)

        elif tab == "apis2_rdx":
            insert_query = f"""
            INSERT INTO {tab} (timestamp, P_ACT_L1_GRID_GEN_CLUSTER_A, P_ACT_L2_GRID_GEN_CLUSTER_A, P_ACT_L3_GRID_GEN_CLUSTER_A, P_REACT_L1_GRID_GEN_CLUSTER_A, P_REACT_L2_GRID_GEN_CLUSTER_A, P_REACT_L3_GRID_GEN_CLUSTER_A,  P_ACT_L1_GRID_GEN_CLUSTER_B, P_ACT_L2_GRID_GEN_CLUSTER_B, P_ACT_L3_GRID_GEN_CLUSTER_B, P_REACT_L1_GRID_GEN_CLUSTER_B, P_REACT_L2_GRID_GEN_CLUSTER_B, P_REACT_L3_GRID_GEN_CLUSTER_B, SOC, BAT_VOLT_DC_BUS_A, DC_CHARGE_CURR_DC_BUS_A, DC_DISCHARGE_CURR_DC_BUS_A, MAX_CHARGE_VOLT_INV_DC_BUS_A, MAX_DC_DISCHARGE_CURR_INV_DC_BUS_A, BAT_VOLT_DC_BUS_B, DC_CHARGE_CURR_DC_BUS_B, DC_DISCHARGE_CURR_DC_BUS_B, MAX_CHARGE_VOLT_INV_DC_BUS_B, MAX_DC_DISCHARGE_CURR_INV_DC_BUS_B, REDOX_P_TOT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            data = (timestamp, registers[4]/10, registers[5]/10, registers[6]/10, registers[10]/10, registers[11]/10, registers[12]/10, registers[19]/100, registers[20]/10, registers[21]/10, registers[26]/10, registers[27]/10, registers[28]/10, registers[39]/10, registers[41]/10, registers[42]/10, registers[43]/10, registers[44], registers[45], registers[48]/10, registers[49]/10, registers[50]/10, registers[51], registers[52], registers[55]/10)

        elif tab == "apis2_sc":
            insert_query = f"""
            INSERT INTO {tab} (timestamp, Status_conversor, DC_Voltage_of_Inverter, DC_Current_of_Inverter, DC_Power_of_Inverter, Phase_Voltage_R_of_Inverter, Phase_Voltage_S_of_Inverter, Phase_Voltage_T_of_Inverter, Line_Voltage_RS_of_Inverter, Line_Voltage_ST_of_Inverter, Line_Voltage_TR_of_Inverter, Line_Current_R_of_Inverter, Line_Current_S_of_Inverter, Line_Current_T_of_Inverter, Active_Power_Phase_R, Active_Power_Phase_S, Active_Power_Phase_T, Reactive_Power_Phase_R, Reactive_Power_Phase_S, Reactive_Power_Phase_T, Total_Active_Power, Total_Reactive_Power, Total_Apparent_Power, Power_Factor, Freq_System, SOC, VCap)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            data = (timestamp, registers[1], registers[2]/10, registers[3]/10, registers[4]/10, registers[5]/10, registers[6]/10, registers[7]/10, registers[8]/10, registers[9]/10, registers[10]/10, registers[11]/10, registers[12]/10, registers[13]/10, registers[14], registers[15], registers[16], registers[17], registers[18], registers[19], registers[20], registers[21], registers[22], registers[23], registers[24]/10, registers[31]/10, registers[32]/10)

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
    apis2_pb = ModbusTcpClient(host=MODBUS_IP_APIS2_PB, port=MODBUS_PORT, timeout=3)
    apis2_li = ModbusTcpClient(host=MODBUS_IP_APIS2_LI, port=MODBUS_PORT, timeout=3)
    apis2_rdx = ModbusTcpClient(host=MODBUS_IP_APIS2_RDX, port=MODBUS_PORT, timeout=3)
    apis2_sc = ModbusTcpClient(host=MODBUS_IP_APIS2_SC, port=MODBUS_PORT, timeout=3)

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
            if not apis2_pb.connect() and apis2_li.connect() and apis2_rdx.connect() and apis2_sc.connect():
                log.error("No se pudo conectar al dispositivo Modbus")
                time.sleep(5)
                continue

            try:
                # Leer registros Modbus
                response1 = apis2_pb.read_holding_registers(address=0, count=33)
                response2 = apis2_li.read_holding_registers(address=0, count=48)
                response3 = apis2_rdx.read_holding_registers(address=0, count=56)
                response4 = apis2_sc.read_holding_registers(address=0, count=33)

                if response1.isError():
                    log.error(f"Error Modbus: {response1}")
                if response2.isError():
                    log.error(f"Error Modbus: {response2}")
                if response2.isError():
                    log.error(f"Error Modbus: {response3}")
                if response4.isError():
                    log.error(f"Error Modbus: {response4}")
                else:
                    insert_data(conn, response1.registers, "apis2_pb")
                    insert_data(conn, response2.registers, "apis2_li")
                    insert_data(conn, response3.registers, "apis2_rdx")
                    insert_data(conn, response4.registers, "apis2_sc")

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
        apis2_pb.close()
        apis2_li.close()
        apis2_rdx.close()
        apis2_sc.close()
        if conn and not conn.closed:
            conn.close()
        log.info("Conexiones cerradas")

if __name__ == "__main__":
    main()