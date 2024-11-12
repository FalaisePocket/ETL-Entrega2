import psycopg2
from datetime import datetime

def entregasPorDia():
    # Conexión a la base de datos origen y destino
    def connect_to_db(database_name):
        return psycopg2.connect(
            host="localhost",
            database=database_name,
            user="alejandro",
            password="Alejo1193",
            options="-c client_encoding=UTF8"
        )

    try:
        conn_origen = connect_to_db("rapidosyfuriosos")
        cur_origen = conn_origen.cursor()

        # Consultar los datos necesarios de la base de datos de origen
        query = """
            SELECT 
                ms.id AS key_servicio,
                cu.cliente_id AS key_cliente,
                cu.ciudad_id AS key_ciudad,
                cu.sede_id AS key_sede,
                ms.fecha_solicitud AS key_fecha,
                es_inicio.fecha AS fecha_iniciado,
                es_inicio.hora AS hora_iniciado,
                es_terminado.fecha AS fecha_terminado,
                es_terminado.hora AS hora_terminado
            FROM 
                mensajeria_servicio ms
            JOIN clientes_usuarioaquitoy cu ON ms.usuario_id = cu.id
            LEFT JOIN mensajeria_estadosservicio es_inicio ON ms.id = es_inicio.servicio_id AND es_inicio.estado_id = 1
            LEFT JOIN mensajeria_estadosservicio es_terminado ON ms.id = es_terminado.servicio_id AND es_terminado.estado_id = 6
            WHERE 
                ms.activo = true;
        """

        # Ejecutar la consulta
        cur_origen.execute(query)
        resultados = cur_origen.fetchall()

        if not resultados:
            print("No se encontraron registros.")
        else:
            print(f"Se encontraron {len(resultados)} registros.")

        # Conexión a la base de datos destino
        conn_destino = connect_to_db("etl")
        cur_destino = conn_destino.cursor()

        # Crear la tabla de hechos 'entregas_completadas_por_dia'
        create_table_query = """
        CREATE TABLE IF NOT EXISTS entregas_completadas_por_dia (
            key_servicio INTEGER,
            key_cliente INTEGER,
            key_ciudad INTEGER,
            key_sede INTEGER,
            key_fecha DATE,
            tiempo_entrega INTERVAL
        );
        """
        print("Creando la tabla de hechos...")
        cur_destino.execute(create_table_query)

        # Insertar los registros obtenidos
        insert_query = """
        INSERT INTO entregas_completadas_por_dia (
            key_servicio, key_cliente, key_ciudad, key_sede, key_fecha, tiempo_entrega
        )
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        for row in resultados:
            try:
                fecha_iniciado = row[5]  # Fecha de "Iniciado"
                hora_iniciado = row[6]  # Hora de "Iniciado"
                fecha_terminado = row[7]  # Fecha de "Terminado"
                hora_terminado = row[8]  # Hora de "Terminado"

                if fecha_iniciado and hora_iniciado and fecha_terminado and hora_terminado:
                    tiempo_iniciado = datetime.combine(fecha_iniciado, hora_iniciado)
                    tiempo_terminado = datetime.combine(fecha_terminado, hora_terminado)
                    tiempo_entrega = tiempo_terminado - tiempo_iniciado
                else:
                    tiempo_entrega = None

                cur_destino.execute(insert_query, (row[0], row[1], row[2], row[3], row[4], tiempo_entrega))

            except Exception as e:
                print(f"Error al insertar el registro {row[0]}: {e}")

        conn_destino.commit()
        print("Datos transferidos con éxito.")

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        # Cerrar las conexiones
        if conn_origen:
            cur_origen.close()
            conn_origen.close()
        if conn_destino:
            cur_destino.close()
            conn_destino.close()

# Llamar a la función
entregasPorDia()
