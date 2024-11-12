import pandas as pd
from config import connect_databases

def cargar_dim_ciudad():
    db_op, db_etl = connect_databases()
    
    ciudad = pd.read_sql_query('SELECT ciudad_id, nombre FROM public.ciudad', db_op)
    ciudad.to_sql('DimCiudad', db_etl, if_exists='replace', index=False)
    print("DimCiudad cargado correctamente")
