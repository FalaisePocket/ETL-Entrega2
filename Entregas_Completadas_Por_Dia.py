import pandas as pd
from config import connect_databases

def entregasPordia():
    db_op, db_etl = connect_databases()
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
    # Extraer datos
    entregas = pd.read_sql_query(query, db_op)

    # Convertir fechas y horas a formatos manejables, manejando valores nulos
    entregas['fecha_iniciado'] = pd.to_datetime(entregas['fecha_iniciado'], errors='coerce')
    entregas['fecha_terminado'] = pd.to_datetime(entregas['fecha_terminado'], errors='coerce')

    def safe_to_timedelta(col):
        return pd.to_timedelta(col, errors='coerce')

    entregas['hora_iniciado'] = safe_to_timedelta(entregas['hora_iniciado'].astype(str))
    entregas['hora_terminado'] = safe_to_timedelta(entregas['hora_terminado'].astype(str))

    # Calcular tiempo de entrega y convertir a segundos
    entregas['tiempo_entrega'] = entregas.apply(
        lambda row: (
            (row['fecha_terminado'] + row['hora_terminado']) - 
            (row['fecha_iniciado'] + row['hora_iniciado'])
        ).total_seconds() if pd.notnull(row['fecha_iniciado']) and pd.notnull(row['hora_iniciado']) and
                            pd.notnull(row['fecha_terminado']) and pd.notnull(row['hora_terminado']) else None,
        axis=1
    )

    # Seleccionar columnas necesarias
    entregas = entregas[['key_servicio', 'key_cliente', 'key_ciudad', 'key_sede', 'key_fecha', 'tiempo_entrega']]

    # Cargar en la base de datos
    entregas.to_sql('FactEntregasPorDia', db_etl, if_exists='replace', index=False)
    print("FactEntregasPorDia cargado correctamente.")
