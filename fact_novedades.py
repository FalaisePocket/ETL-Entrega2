import pandas as pd
from config import connect_databases

def novedades():
    db_op, db_etl = connect_databases()

    novedades = pd.read_sql_query('SELECT * FROM public.mensajeria_novedadesservicio', db_op)
    tipo_novedad = pd.read_sql_query('SELECT * FROM public.mensajeria_tiponovedad', db_op)

    # Transformación
    tipo_novedad = tipo_novedad.rename(columns={'id': 'tipo_novedad_id', 'nombre': 'tipo_novedad'})
    fact_table = novedades.merge(tipo_novedad, on='tipo_novedad_id', how='left')

    fact_table = fact_table[['id', 'fecha_novedad', 'tipo_novedad', 'descripcion', 'mensajero_id']]
    fact_table.to_sql('FactNovedades', db_etl, if_exists='replace', index=False)
    print("FactNovedades cargado correctamente.")
