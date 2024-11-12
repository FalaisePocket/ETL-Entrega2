import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import connect_databases



def entregaPorEstado():
    db_op, db_etl = connect_databases()



    #Extract


    ###mensajeria_estado???
    ##mensajeria_estadoservicio
    ##mensajeria_tiposervicio????
    ##mensajeria_servicio


    mensajeria_estado =  pd.read_sql_query('SELECT * FROM public.mensajeria_estado', db_op)
    mensajeria_estadosservicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_estadosservicio', db_op)
    mensajeria_tiposervicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_tiposervicio', db_op)
    mensajeria_servicio =  pd.read_sql_query('SELECT * FROM public.mensajeria_servicio', db_op)
    mensajeria_usuarioaquitoy = pd.read_sql_query( 'SELECT * FROM public.clientes_usuarioaquitoy' ,db_op)
    sede= pd.read_sql_query( 'SELECT * FROM public.sede' ,db_op)



    ###drops
    mensajeria_estadosservicio=mensajeria_estadosservicio[['id'
                                                        ,'fecha','hora',
                                                        'estado_id','servicio_id'
                                                        ]]
    mensajeria_servicio=mensajeria_servicio[['id','cliente_id','mensajero_id',
                                            'tipo_servicio_id']]
    mensajeria_tiposervicio=mensajeria_tiposervicio[['id', 'nombre',]]
    mensajeria_estado=mensajeria_estado[['id','nombre']]



    mensajeria_usuarioaquitoy=mensajeria_usuarioaquitoy[['id','cliente_id','sede_id','ciudad_id']]
    sede= sede[['sede_id','nombre']]

    ####renames
    mensajeria_tiposervicio = mensajeria_tiposervicio.rename(columns={'id': 'tipo_servicio_id','nombre': 'tipo_servicio'})
    mensajeria_servicio= mensajeria_servicio.rename(columns={'id': 'servicio_id'})
    mensajeria_estadosservicio=mensajeria_estadosservicio.rename(columns={'id':'estadosservicios_id'})
    mensajeria_estado=mensajeria_estado.rename(columns={'id':'estado_id','nombre': 'estado_nombre'})
    mensajeria_usuarioaquitoy=mensajeria_usuarioaquitoy.rename(columns={'id':'usuarioaquitoy_id'})
    sede=sede.rename(columns={'nombre':'sede'})


    facttable=mensajeria_estadosservicio.merge(
        mensajeria_servicio,
        left_on='servicio_id',
        right_on='servicio_id',
        how='left')

    facttable=facttable.merge(
        mensajeria_tiposervicio,
        left_on='tipo_servicio_id',
        right_on='tipo_servicio_id',
        how='left'   
    )

    facttable=facttable.merge(
        mensajeria_estado,
        left_on='estado_id',
        right_on='estado_id',
        how='left'
    )



    facttable=facttable.rename(columns={'estadosservicios_id':'id'})
    facttable=facttable[['id','servicio_id','mensajero_id',
                        'tipo_servicio','estado_nombre',
                        'fecha','hora','cliente_id']]

    '''mensajeria_usuarioaquitoy=mensajeria_usuarioaquitoy.merge(
        sede,
        left_on='sede_id',
        right_on='sede_id',
        how='left'
    )

    facttable=facttable.merge(
        mensajeria_usuarioaquitoy,
        left_on='cliente_id',
        right_on='cliente_id',
        how='left'
    )


    facttable=facttable[['id','servicio_id','mensajero_id',
                        'tipo_servicio','estado_nombre',
                        'fecha','hora','cliente_id','sede_id','ciudad_id']]

    '''
    print(facttable.columns)

    #usuarioaquitoy



    facttable.to_sql('Fact_entrega_estados', db_etl, if_exists='replace', index=False)

