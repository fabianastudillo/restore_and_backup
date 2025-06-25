#!/usr/bin/env python3.12

import os
import json
import sys

# Define la ruta al archivo de configuración JSON
CONFIG_FILE = '/home/administrador/scripts/config.json'

try:
    # Abre y lee el archivo JSON
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    # Extrae los datos de configuración
    db_name = config.get('db_name')
    db_user = config.get('db_user')
    output_file = config.get('output_file')
    
    # Construye la cadena de conexión para psql
    connection_string = f'"user={db_user} dbname={db_name}"'
    
    # Se construye el comando de psql
    command = f'docker exec -i postgres psql -d {connection_string} < {output_file}'
    
    result = os.system(command)

    if result == 0:
        print("Recuperacion de la base de datos completada con exito.")
    else:
        print(f"Error: psql falló con código de salida {result}.", file=sys.stderr)
        sys.exit(result)

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
