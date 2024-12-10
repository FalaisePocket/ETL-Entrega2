import pandas as pd
from config import connect_databases

def cargar_dim_cliente():
    db_op, db_etl = connect_databases()
    cliente = pd.read_sql_query('SELECT * FROM public.cliente', db_op)
    ciudad = pd.read_sql_query('SELECT * FROM public.ciudad', db_op)
    cliente_ciudad = cliente.merge(ciudad, on='ciudad_id', how='left')
    cliente_ciudad = cliente_ciudad.drop(columns='ciudad_id')
    cliente_ciudad.to_sql('DimCliente', db_etl, if_exists='replace', index=False)
    print("DimCliente cargado correctamente.")
