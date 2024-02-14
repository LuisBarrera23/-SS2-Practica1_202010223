import os
import pyodbc
import pandas as pd
import pyodbc
from coneccion import establecer_conexion, cerrar_conexion
from consultas import menu_consultas


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


def extraer_informacion(connection):
    ruta_archivos = "./datos.csv"
    print(f"Extrayendo información del archivo en {ruta_archivos}...")

    try:
        df = pd.read_csv(ruta_archivos)
        df = df.fillna(0)
        df_relevantes = df[["Year", "Maximum Water Height (m)", "Total Deaths", "Total Damage ($Mil)", "Total Houses Destroyed", "Total Houses Damaged", "Country"]]
        
        
        for index, row in df_relevantes.iterrows():
            anio = row["Year"]
            if anio == 0:
                continue
            altura = row["Maximum Water Height (m)"]
            muertes = row["Total Deaths"]
            danio = row["Total Damage ($Mil)"]
            casas_destruidas = row["Total Houses Destroyed"]
            casas_alteradas = row["Total Houses Damaged"]
            pais = row["Country"]

            insert_query = f"""
                INSERT INTO ##registros_temporales (anio, altura, muertes, danio, casas_destruidas, casas_alteradas, pais)
                VALUES ({anio}, {altura}, {muertes}, {danio}, {casas_destruidas}, {casas_alteradas}, '{pais}');
            """
            # print(insert_query)

            cursor = connection.cursor()
            cursor.execute(insert_query)
            connection.commit()

        print("Datos insertados correctamente en ##registros_temporales.")

    except Exception as e:
        print(f"Error al extraer e insertar datos: {str(e)}")


def cargar_informacion(connection):
    print("Cargando información al modelo...")

    try:
        insert_query_anios = """
            INSERT INTO anio (anio)
            SELECT DISTINCT anio FROM ##registros_temporales
            ORDER BY anio;
        """
        cursor = connection.cursor()
        cursor.execute(insert_query_anios)
        connection.commit()

        print("Años insertados correctamente en la tabla anio.")

        insert_query_paises = """
            INSERT INTO pais (nombre_pais)
            SELECT DISTINCT pais FROM ##registros_temporales;
        """
        cursor.execute(insert_query_paises)
        connection.commit()

        print("Países insertados correctamente en la tabla pais.")

        insert_query_registros = """
            INSERT INTO registros (anio_id,altura,muertes,danio,casas_destruidas,casas_alteradas,pais_id)
            SELECT  (SELECT id FROM anio WHERE anio = ##registros_temporales.anio),
            altura,
            muertes,
            danio,
            casas_destruidas,
            casas_alteradas,
            (SELECT id FROM pais WHERE nombre_pais = ##registros_temporales.pais)
            FROM ##registros_temporales;
        """
        cursor.execute(insert_query_registros)
        connection.commit()

        print("Tabla de registros cargada correctamente")

    except Exception as e:
        print(f"Error al cargar información: {str(e)}")


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
        extraer_informacion(conexion_db)
    elif opcion == '4':
        cargar_informacion(conexion_db)
    elif opcion == '5':
        menu_consultas(conexion_db)
    elif opcion == '6':

        cerrar_conexion(conexion_db)
        print("LA EJECUCION DEL PROGRAMA FUE DETENIDA")
        break
    else:
        print("Opción no válida. Por favor, seleccione una opción válida.")
