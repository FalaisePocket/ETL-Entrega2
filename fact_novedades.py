import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import connect_databases

def novedades():
    db_op, db_etl = connect_databases()

    novedad = pd.read_sql_query('SELECT * FROM public.mensajeria_novedadesservicio', db_op)
    tipo_novedad= pd.read_sql_query('SELECT * FROM public.mensajeria_tiponovedad', db_op)


    #####################Transform###########################################
    #########################################################################



    # Drop columns

    novedad = novedad[['id', 'fecha_novedad','tipo_novedad_id','descripcion', 'mensajero_id',]]

    tipo_novedad=tipo_novedad.rename(columns={'id': 'tipo_novedad_id','nombre': 'tipo_novedad'})

    facttable=novedad.merge(
        tipo_novedad,
        left_on='tipo_novedad_id',
        right_on='tipo_novedad_id',
        how='left'
    )

    facttable=novedad.drop(columns='tipo_novedad_id')
    ##añadir estas fechas a la dimension novedad

    print(facttable.columns)
    # Rename Columns



    #############################Load######################################
    #######################################################################
    facttable.to_sql('Fact_novedades', db_etl, if_exists='replace',index=False)


