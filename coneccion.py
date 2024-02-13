import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

def establecer_conexion():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        connection = pyodbc.connect(connection_string)
        print("conectado con la base de datos")
        return connection

    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None

def cerrar_conexion(connection):
    if connection:
        connection.close()
        print("Conexion cerrada.")

