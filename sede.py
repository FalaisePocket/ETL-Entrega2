import pandas as pd
from config import connect_databases

def cargar_dim_sede():
    db_op, db_etl = connect_databases()
    
    sede = pd.read_sql_query('SELECT sede_id, nombre FROM public.sede', db_op)
    sede.to_sql('DimSede', db_etl, if_exists='replace', index=False)
    print("DimSede cargado correctamente")
