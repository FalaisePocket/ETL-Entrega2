import pandas as pd
from config import connect_databases

def cargar_dim_mensajero():
    db_op, db_etl = connect_databases()
    
    query = """
        SELECT
            mensajero.id AS mensajero_id,
            COUNT(servicio.cliente_id) AS numero_servicios
        FROM clientes_mensajeroaquitoy AS mensajero
        LEFT JOIN clientes_mensajeroaquitoy_clientes AS servicio
            ON mensajero.id = servicio.mensajeroaquitoy_id
        GROUP BY mensajero.id;
    """
    mensajero = pd.read_sql_query(query, db_op)
    mensajero.to_sql('DimMensajero', db_etl, if_exists='replace', index=False)
    print("DimMensajero cargado correctamente")
