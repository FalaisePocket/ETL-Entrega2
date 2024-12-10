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
    print(fact_table.columns)
    fact_table = fact_table[['estados_servicio_id','mensajero_id', 'servicio_id', 'tipo_servicio', 'estado_nombre', 'fecha', 'hora']]
    



    fact_table_copy= fact_table[['servicio_id','estado_nombre','fecha','hora']].copy()
    fact_table_copy['hora'] = fact_table_copy['hora'].astype(str).str.split('.').str[0]
    fact_table_copy['fecha_hora'] = pd.to_datetime(fact_table_copy['fecha'].astype(str) + ' ' + fact_table_copy['hora'].astype(str))
    fact_table_copy=fact_table_copy.drop(columns=['fecha','hora'])

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

    fact_table= fact_table.merge(fecha_hora_con_ids, on=['fecha','hora'],how='inner')

    
    #
    fact_table=fact_table.drop(columns=['fecha','hora','dia_semana','mes'])
    
    

    result = fact_table.pivot_table(
        index=['servicio_id', 'tipo_servicio','mensajero_id'],  # Agrupar por servicio_id y tipo_servicio
        columns='estado_nombre',               # Usar los valores de estado_nombre como columnas
        values='fecha_id',                         # Usar los valores de hora
        aggfunc='first'                        # En caso de duplicados, tomar el primero
    ).reset_index()

    # Renombrar las columnas para mayor claridad
    result.columns.name = None  # Eliminar el nombre del índice de las columnas
    result.rename(columns={
        'Iniciado': 'hora_iniciado_id',
        'Con mensajero Asignado': 'hora_asignado_id',
        'Recogido por mensajero': 'hora_recogido_id',
        'Entregado en destino':'hora_entregado_id',
        'Terminado completo': 'hora_finalizado_id',
        'Con novedad':'hora_novedad_id'
    }, inplace=True)
    
    fact_table_copy=fact_table_copy.pivot_table(
        index=['servicio_id'],  # Agrupar por servicio_id y tipo_servicio
        columns='estado_nombre',               # Usar los valores de estado_nombre como columnas
        values='fecha_hora',                         # Usar los valores de hora
        aggfunc='first'                        # En caso de duplicados, tomar el primero
    ).reset_index()
    

    fact_table_copy.rename(columns={
        'Iniciado': 'hora_iniciado',
        'Con mensajero Asignado': 'hora_asignado',
        'Recogido por mensajero': 'hora_recogido',
        'Entregado en destino':'hora_entregado',
        'Terminado completo': 'hora_finalizado',
        'Con novedad':'hora_novedad'
    }, inplace=True)

  

    # Calcular las diferencias como timedelta
    fact_table_copy['tiempo_iniciado_asignado'] = pd.to_datetime(fact_table_copy['hora_asignado'], errors='coerce') - pd.to_datetime(fact_table_copy['hora_iniciado'], errors='coerce')
    fact_table_copy['tiempo_asignado_recogido'] = pd.to_datetime(fact_table_copy['hora_recogido'], errors='coerce') - pd.to_datetime(fact_table_copy['hora_asignado'], errors='coerce')
    fact_table_copy['tiempo_recogido_entregado'] = pd.to_datetime(fact_table_copy['hora_entregado'], errors='coerce') - pd.to_datetime(fact_table_copy['hora_recogido'], errors='coerce')
    fact_table_copy['tiempo_entregado_finalizado'] = pd.to_datetime(fact_table_copy['hora_finalizado'], errors='coerce') - pd.to_datetime(fact_table_copy['hora_entregado'], errors='coerce')

    # Formatear los timedelta, manejando valores NaT
    def format_timedelta(td):
        if pd.isnull(td):  # Si es NaT, devolver un valor predeterminado
            return "NaT"
        else:
                return f"{td.days} days {td.components.hours:02}:{td.components.minutes:02}:{td.components.seconds:02}"

    fact_table_copy['tiempo_iniciado_asignado'] = fact_table_copy['tiempo_iniciado_asignado'].apply(format_timedelta)
    fact_table_copy['tiempo_asignado_recogido'] = fact_table_copy['tiempo_asignado_recogido'].apply(format_timedelta)
    fact_table_copy['tiempo_recogido_entregado'] = fact_table_copy['tiempo_recogido_entregado'].apply(format_timedelta)
    fact_table_copy['tiempo_entregado_finalizado'] = fact_table_copy['tiempo_entregado_finalizado'].apply(format_timedelta)
    fact_table_copy=fact_table_copy.drop(columns=['hora_iniciado','hora_asignado','hora_recogido','hora_entregado','hora_finalizado'])

    
    result=result.merge(fact_table_copy, on= 'servicio_id',how='left')
    print(result.shape)
    servicio.rename(columns={'id':'servicio_id'})
    servicio=servicio[['servicio_id','cliente_id']]
    result=result.merge(servicio, on='servicio_id',how='left' )
    print(result.shape)
    
    print(result.columns)


    result=result[['servicio_id','tipo_servicio','mensajero_id','cliente_id','hora_iniciado_id','hora_asignado_id','hora_recogido_id','hora_entregado_id','hora_finalizado_id', 
                   'hora_novedad_id','tiempo_iniciado_asignado','tiempo_asignado_recogido','tiempo_recogido_entregado','tiempo_entregado_finalizado']]



    result.to_sql('FactEntregaEstados', db_etl, if_exists='replace', index=False)
    print("FactEntregaEstados cargado correctamente.")
