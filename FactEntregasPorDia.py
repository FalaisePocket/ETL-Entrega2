import pandas as pd
from config import connect_databases

def format_tiempo_entrega(seconds):
    """Convierte segundos en un formato legible de días, horas, minutos y segundos."""
    if pd.isnull(seconds):
        return None
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    result = []
    if days > 0:
        result.append(f"{int(days)} días")
    if hours > 0:
        result.append(f"{int(hours)} horas")
    if minutes > 0:
        result.append(f"{int(minutes)} minutos")
    if seconds > 0:
        result.append(f"{int(seconds)} segundos")
    return ", ".join(result)

def entregasPordia():
    try:
        # Conectar a las bases de datos
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

        # Convertir fechas y horas a formatos manejables
        entregas['fecha_iniciado'] = pd.to_datetime(entregas['fecha_iniciado'], errors='coerce')
        entregas['fecha_terminado'] = pd.to_datetime(entregas['fecha_terminado'], errors='coerce')

        def safe_to_timedelta(col):
            return pd.to_timedelta(col, errors='coerce')

        entregas['hora_iniciado'] = safe_to_timedelta(entregas['hora_iniciado'].astype(str))
        entregas['hora_terminado'] = safe_to_timedelta(entregas['hora_terminado'].astype(str))

        # Calcular tiempo de entrega en segundos
        entregas['tiempo_entrega'] = entregas.apply(
            lambda row: (
                (row['fecha_terminado'] + row['hora_terminado']) - 
                (row['fecha_iniciado'] + row['hora_iniciado'])
            ).total_seconds() if pd.notnull(row['fecha_iniciado']) and pd.notnull(row['hora_iniciado']) and
                                pd.notnull(row['fecha_terminado']) and pd.notnull(row['hora_terminado']) else None,
            axis=1
        )

        # Formatear tiempo de entrega en un formato legible
        entregas['tiempo_entrega_legible'] = entregas['tiempo_entrega'].apply(format_tiempo_entrega)

        # Calcular entregas completadas por día
        entregas_completadas = (
            entregas.groupby('key_fecha')
            .size()
            .reset_index(name='entregas_completadas')
        )

        # Unir datos
        entregas = entregas.merge(entregas_completadas, on='key_fecha', how='left')

        # Seleccionar columnas necesarias
        entregas = entregas[['key_servicio', 'key_cliente', 'key_ciudad', 'key_sede', 'key_fecha', 'tiempo_entrega_legible', 'entregas_completadas']]

        # Cargar en la base de datos
        entregas.to_sql('FactEntregasPorDia', db_etl, if_exists='replace', index=False)
        print("FactEntregasPorDia cargado correctamente.")

    except Exception as e:
        print(f"Error durante el proceso: {e}")

# Bloque principal
if __name__ == "__main__":
    entregasPordia()
