#!/usr/bin/env python3.12

import os
import json
import sys

# Define la ruta al archivo de configuración JSON
# Puedes ajustar esta ruta si el archivo no está en el mismo directorio
CONFIG_FILE = '/home/administrador/scripts/config.json'

try:
    # Abre y lee el archivo JSON
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)

    # Extrae los datos de configuración
    db_name = config.get('db_name')
    db_user = config.get('db_user')
    output_file = config.get('output_file')

    # Construye la cadena de conexión para pg_dump
    # Usamos comillas dobles alrededor de la cadena de conexión para manejar espacios o caracteres especiales
    connection_string = f'"user={db_user} dbname={db_name}"'

    # Construye el comando completo de pg_dump
    command = f'docker exec postgres pg_dump -d {connection_string} > {output_file}'

    # Ejecuta el comando en el sistema operativo
    result = os.system(command)

    # Verifica el código de salida del comando
    if result == 0:
        print("Copia de seguridad de la base de datos completada con éxito.")
    else:
        print(f"Error: pg_dump falló con código de salida {result}.", file=sys.stderr)
        sys.exit(result) # Sale con el código de error de pg_dump

except FileNotFoundError:
    print(f"Error: El archivo de configuración '{CONFIG_FILE}' no fue encontrado.", file=sys.stderr)
    sys.exit(1)
except json.JSONDecodeError:
    print(f"Error: No se pudo decodificar el archivo JSON '{CONFIG_FILE}'. Verifica el formato.", file=sys.stderr)
    sys.exit(1)
except KeyError as e:
    print(f"Error: Falta la clave '{e}' en el archivo de configuración '{CONFIG_FILE}'.", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}", file=sys.stderr)
    sys.exit(1)

# Si todo fue bien, sale con código de éxito (0)
sys.exit(0)
