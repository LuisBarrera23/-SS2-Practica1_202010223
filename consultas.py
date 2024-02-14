import os
import pyodbc
import pandas as pd
import pyodbc
from prettytable import PrettyTable


def consulta1(connection):
    print("realizando consulta 1")
    try:
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM pais")
        dataPais = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM anio")
        dataAnio = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM registros")
        dataRegistros = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM ##registros_temporales")
        dataTemporales = cursor.fetchone()[0]

        tabla = PrettyTable(["TABLA", "CANTIDAD DE DATOS"])
        tabla.add_row(["PAIS",str(dataPais)])
        tabla.add_row(["ANIO",str(dataAnio)])
        tabla.add_row(["REGISTROS",str(dataRegistros)])
        tabla.add_row(["TEMPORAL",str(dataTemporales)])

        with open("./Consultas/consulta1.txt", "w") as f:
            f.write("SELECT COUNT(*) de todas las tablas:\n")
            f.write(str(tabla))

        print("Consulta 1 realizada con exito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta2(connection):
    print("realizando consulta 2")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT anio.anio, COUNT(registros.id) AS cantidad_tsunamis
            FROM anio
            LEFT JOIN registros ON anio.id = registros.anio_id
            GROUP BY anio.anio
            ORDER BY anio.anio DESC;
        
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["AÑO", "CANTIDAD DE TSUNAMI REGISTRADOS"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta2.txt", "w") as f:
            f.write("CANTIDAD DE TSUNAMIS POR AÑO:\n")
            f.write(str(tabla))

        print("Consulta 2 realizada con exito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")



def consulta3(connection):
    print("Realizando consulta 3")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                p.nombre_pais AS pais,
                STUFF((
                    SELECT ',' + CAST(a.anio AS NVARCHAR(255))
                    FROM registros r
                    INNER JOIN anio a ON r.anio_id = a.id
                    WHERE p.id = r.pais_id
                    ORDER BY a.anio
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS años
            FROM
                pais p
            ORDER BY
                p.nombre_pais;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Año 1", "Año 2", "Año 3", "Año 4", "Año 5"])

        for row in data:
            años = list(map(int, row[1].split(','))) if row[1] else []
            años.extend([" "] * (5 - len(años)))
            tabla.add_row([row[0]] + años[:5])

        with open("./Consultas/consulta3.txt", "w") as f:
            f.write("AÑOS DE TSUNAMIS POR PAÍS:\n")
            f.write(str(tabla))

        print("Consulta 3 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")





def consulta4(connection):
    print("realizando consulta 4")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                pais.nombre_pais AS pais,
                CAST(AVG(registros.danio) AS DECIMAL(10, 3)) AS promedio_total_damage
            FROM
                pais
            LEFT JOIN
                registros ON pais.id = registros.pais_id
            GROUP BY
                pais.nombre_pais
            ORDER BY
                promedio_total_damage DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Promedio Total Damage"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta4.txt", "w") as f:
            f.write("Promedio de Total Damage por País:\n")
            f.write(str(tabla))

        print("Consulta 4 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")



def consulta5(connection):
    print("Realizando consulta 5")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            WITH RankingMuertes AS (
                SELECT
                    pais.nombre_pais,
                    SUM(registros.muertes) AS total_muertes
                FROM
                    pais
                LEFT JOIN
                    registros ON pais.id = registros.pais_id
                GROUP BY
                    pais.nombre_pais
            )
            SELECT TOP 5
                nombre_pais AS Pais,
                total_muertes AS "Total de Muertes"
            FROM
                RankingMuertes
            ORDER BY
                total_muertes DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Total de Muertes"])

        for row in data:
            tabla.add_row(row)
        with open("./Consultas/consulta5.txt", "w") as f:
            f.write("TOP 5 DE PAÍSES CON MÁS MUERTES:\n")
            f.write(str(tabla))

        print("Consulta 5 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta6(connection):
    print("Realizando consulta 6")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT TOP 5
                anio.anio AS Año,
                SUM(registros.muertes) AS "Total de Muertes"
            FROM
                anio
            LEFT JOIN
                registros ON anio.id = registros.anio_id
            GROUP BY
                anio.anio
            ORDER BY
                "Total de Muertes" DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["Año", "Total de Muertes"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta6.txt", "w") as f:
            f.write("TOP 5 DE AÑOS CON MÁS MUERTES:\n")
            f.write(str(tabla))

        print("Consulta 6 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta7(connection):
    print("Realizando consulta 7")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT TOP 5
                anio.anio AS Año,
                COUNT(registros.id) AS "Cantidad de Tsunamis"
            FROM
                anio
            LEFT JOIN
                registros ON anio.id = registros.anio_id
            GROUP BY
                anio.anio
            ORDER BY
                "Cantidad de Tsunamis" DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["Año", "Cantidad de Tsunamis"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta7.txt", "w") as f:
            f.write("TOP 5 DE AÑOS CON MÁS TSUNAMIS:\n")
            f.write(str(tabla))

        print("Consulta 7 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta8(connection):
    print("Realizando consulta 8")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT TOP 5
                pais.nombre_pais AS País,
                SUM(registros.casas_destruidas) AS "Total de Casas Destruidas"
            FROM
                pais
            LEFT JOIN
                registros ON pais.id = registros.pais_id
            GROUP BY
                pais.nombre_pais
            ORDER BY
                "Total de Casas Destruidas" DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Total de Casas Destruidas"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta8.txt", "w") as f:
            f.write("TOP 5 DE PAÍSES CON MÁS CASAS DESTRUIDAS:\n")
            f.write(str(tabla))

        print("Consulta 8 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta9(connection):
    print("Realizando consulta 9")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT TOP 5
                pais.nombre_pais AS País,
                SUM(registros.casas_alteradas) AS "Total de Casas Dañadas"
            FROM
                pais
            LEFT JOIN
                registros ON pais.id = registros.pais_id
            GROUP BY
                pais.nombre_pais
            ORDER BY
                "Total de Casas Dañadas" DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Total de Casas Dañadas"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta9.txt", "w") as f:
            f.write("TOP 5 DE PAÍSES CON MÁS CASAS DAÑADAS:\n")
            f.write(str(tabla))

        print("Consulta 9 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def consulta10(connection):
    print("Realizando consulta 10")
    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                pais.nombre_pais AS País,
                CAST(AVG(registros.altura) AS DECIMAL(10, 3)) AS "Promedio Altura Máxima del Agua"
            FROM
                pais
            LEFT JOIN
                registros ON pais.id = registros.pais_id
            GROUP BY
                pais.nombre_pais
            ORDER BY
                "Promedio Altura Máxima del Agua" DESC;
        """)
        data = cursor.fetchall()

        tabla = PrettyTable(["País", "Promedio Altura Máxima del Agua"])

        for row in data:
            tabla.add_row(row)

        with open("./Consultas/consulta10.txt", "w") as f:
            f.write("PROMEDIO DE ALTURA MÁXIMA DEL AGUA POR PAÍS:\n")
            f.write(str(tabla))

        print("Consulta 10 realizada con éxito")

    except pyodbc.Error as e:
        print(f"Error al realizar las consultas: {str(e)}")


def menu_consultas(connection):
    while True:
        print("MENU CONSULTAS:")
        print("1) TABLAS DEL MODELO")
        print("2) CANTIDAD DE TSUNAMIS POR AÑO")
        print("3) TSUNAMIS POR PAIS Y AÑOS")
        print("4) PROMEDIO DE TOTAL DAMAGE POR PAIS")
        print("5) TOP 5 PAISES CON MAS MUERTES")
        print("6) TOP 5 AÑOS CON MAS MUERTES")
        print("7) TOP 5 DE AÑOS QUE MAS TSUNAMIS HAN TENIDO")
        print("8) TOP 5 DE PAISES CON MAYOR NUMERO DE CASAS DESTRUIDAS")
        print("9) TOP 5 DE PAISES CON MAYOR NUMERO DE CASAS DAÑADAS")
        print("10) PROMEDIO DE ALTURA MAXIMA DEL AGUA POR CADA PAIS")
        print("11) REGRESAR AL MENU PRINCIPAL")
        opcion = input(
            "SELECCIONE UNA OPCION (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11): ")

        if opcion == '1':
            consulta1(connection)
        elif opcion == '2':
            consulta2(connection)
        elif opcion == '3':
            consulta3(connection)
        elif opcion == '4':
            consulta4(connection)
        elif opcion == '5':
            consulta5(connection)
        elif opcion == '6':
            consulta6(connection)
        elif opcion == '7':
            consulta7(connection)
        elif opcion == '8':
            consulta8(connection)
        elif opcion == '9':
            consulta9(connection)
        elif opcion == '10':
            consulta10(connection)
        elif opcion == '11':
            break
        else:
            print("Opción no válida. Por favor, seleccione una opción válida.")
