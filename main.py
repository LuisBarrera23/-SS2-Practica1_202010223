import os
from coneccion import establecer_conexion, cerrar_conexion
import pyodbc


def borrar_modelo(connection):
    print("Borrando el modelo...")

    drop_tables_query = """
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'registros')
        DROP TABLE registros;

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '##registros_temporales')
        DROP TABLE ##registros_temporales;

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'pais')
        DROP TABLE pais;

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'anio')
        DROP TABLE anio;
    """

    try:
        cursor = connection.cursor()
        cursor.execute(drop_tables_query)
        connection.commit()
        print("Tablas eliminadas correctamente.")

    except pyodbc.Error as e:
        print(f"Error al eliminar tablas: {str(e)}")
        connection.rollback()



def crear_modelo(connection):
    print("Creando el modelo...")

    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '##registros_temporales')
    BEGIN
        CREATE TABLE ##registros_temporales (
            anio INT,
            altura FLOAT,
            muertes INT,
            danio FLOAT,
            casas_destruidas INT,
            casas_alteradas INT,
            pais NVARCHAR(255)
        );
    END;

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'pais')
    BEGIN
        CREATE TABLE pais (
            id INT IDENTITY(1,1) PRIMARY KEY,
            nombre_pais NVARCHAR(255)
        );
    END;

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'anio')
    BEGIN
        CREATE TABLE anio (
            id INT IDENTITY(1,1) PRIMARY KEY,
            anio INT
        );
    END;

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'registros')
    BEGIN
        CREATE TABLE registros (
            id INT IDENTITY(1,1) PRIMARY KEY,
            anio_id INT,
            altura FLOAT,
            muertes INT,
            danio FLOAT,
            casas_destruidas INT,
            casas_alteradas INT,
            pais_id INT,
            FOREIGN KEY (anio_id) REFERENCES anio(id),
            FOREIGN KEY (pais_id) REFERENCES pais(id)
        );
    END;

    
    """

    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("El modelo fue creado correctamente")

    except pyodbc.Error as e:
        print(f"Error al crear el modelo: {str(e)}")
        connection.rollback()


def extraer_informacion():
    ruta_archivos = input("Ingrese la ruta de los archivos de carga: ")
    print(f"Extrayendo información de los archivos en {ruta_archivos}...")


def cargar_informacion():
    print("Cargando información al modelo...")


def realizar_consultas():
    print("Realizando consultas y guardando resultados en un archivo de texto...")


conexion_db = establecer_conexion()

while True:
    print("MENU:")
    print("1) BORRAR MODELO")
    print("2) CREAR MODELO")
    print("3) EXTRAER INFORMACION")
    print("4) CARGAR INFORMACION")
    print("5) REALIZAR CONSULTAS")
    print("6) Salir")
    opcion = input("SELECCIONE UNA OPCION (1, 2, 3, 4, 5, 6): ")

    if opcion == '1':
        borrar_modelo(conexion_db)
    elif opcion == '2':
        crear_modelo(conexion_db)
    elif opcion == '3':
        extraer_informacion()
    elif opcion == '4':
        cargar_informacion()
    elif opcion == '5':
        realizar_consultas()
    elif opcion == '6':

        cerrar_conexion(conexion_db)
        print("LA EJECUCION DEL PROGRAMA FUE DETENIDA")
        break
    else:
        print("Opción no válida. Por favor, seleccione una opción válida.")
