import pandas as pd
from config import connect_databases

def entregaPorEstado():
    db_op, db_etl = connect_databases()

    estado = pd.read_sql_query('SELECT * FROM public.mensajeria_estado', db_op)
    estados_servicio = pd.read_sql_query('SELECT * FROM public.mensajeria_estadosservicio', db_op)
    tipo_servicio = pd.read_sql_query('SELECT * FROM public.mensajeria_tiposervicio', db_op)
    servicio = pd.read_sql_query('SELECT * FROM public.mensajeria_servicio', db_op)

    # Transformación
    estados_servicio = estados_servicio.rename(columns={'id': 'estados_servicio_id'})
    servicio = servicio.rename(columns={'id': 'servicio_id'})
    estado = estado.rename(columns={'id': 'estado_id', 'nombre': 'estado_nombre'})
    tipo_servicio = tipo_servicio.rename(columns={'id': 'tipo_servicio_id', 'nombre': 'tipo_servicio'})

    fact_table = estados_servicio.merge(servicio, on='servicio_id', how='left')
    fact_table = fact_table.merge(tipo_servicio, on='tipo_servicio_id', how='left')
    fact_table = fact_table.merge(estado, on='estado_id', how='left')

    fact_table = fact_table[['estados_servicio_id', 'servicio_id', 'tipo_servicio', 'estado_nombre', 'fecha', 'hora']]
    fact_table.to_sql('FactEntregaEstados', db_etl, if_exists='replace', index=False)
    print("FactEntregaEstados cargado correctamente.")
