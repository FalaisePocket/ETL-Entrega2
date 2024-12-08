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
    
    ######insertar fecha y hora en la dimension
    fecha_hora=fact_table[['fecha','hora']].copy()
    # Asegurarse de que la columna 'fecha' sea de tipo datetime
    fecha_hora['fecha'] = pd.to_datetime(fecha_hora['fecha'])

    # Añadir columna 'dia_semana' con el nombre del día
    fecha_hora['dia_semana'] = fecha_hora['fecha'].dt.day_name()

    # Añadir columna 'mes' con el nombre del mes
    fecha_hora['mes'] = fecha_hora['fecha'].dt.month_name()

    fecha_hora.to_sql('DimFecha',db_etl,if_exists='append', index=False)


    fecha_hora_con_ids = pd.read_sql('SELECT * FROM public."DimFecha"', db_etl)

    fact_table= fact_table.merge(fecha_hora_con_ids, on=['fecha','hora'],how='left')
    fact_table=fact_table.drop(columns=['fecha','hora','dia_semana','mes'])
    

    result = fact_table.pivot_table(
        index=['servicio_id', 'tipo_servicio'],  # Agrupar por servicio_id y tipo_servicio
        columns='estado_nombre',               # Usar los valores de estado_nombre como columnas
        values='fecha_id',                         # Usar los valores de hora
        aggfunc='first'                        # En caso de duplicados, tomar el primero
    ).reset_index()

    # Renombrar las columnas para mayor claridad
    result.columns.name = None  # Eliminar el nombre del índice de las columnas
    result.rename(columns={
        'Iniciado': 'hora_iniciado',
        'Con mensajero Asignado': 'hora_Asignado',
        'Recogido por mensajero': 'hora_recogido',
        'Entregado en destino':'hora_entregado',
        'Terminado completo': 'hora_finalizado'
    }, inplace=True)
    
    result=result[['servicio_id','tipo_servicio','hora_iniciado','hora_Asignado','hora_recogido','hora_entregado','hora_finalizado', 'Con novedad',]]

    '''
    
    #servicio_id, estado_nombre, fecha, hora
    factablepivot=fact_table.pivot(index='servicio_id', columns='estado_nombre', values='hora')
    ##id??,servicio_id, mensajero_id,tipo_servicio, cliente_id
    newtable=fact_table.drop(columns=['id','estado_nombre','fecha','hora']).drop_duplicates(subset=['servicio_id'])

    fact_table = pd.merge(factablepivot, newtable, on='servicio_id')
    fact_table.reset_index(drop=True, inplace=True)
'''

    result.to_sql('FactEntregaEstados', db_etl, if_exists='replace', index=False)
    print("FactEntregaEstados cargado correctamente.")
