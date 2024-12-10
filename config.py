from sqlalchemy import create_engine

def connect_databases():
    # Configuración de las bases de datos
    db_op = create_engine('postgresql+psycopg2://Andres:invitado@localhost/PROYECT_DB')
    db_etl = create_engine('postgresql+psycopg2://Andres:invitado@localhost/etl')
    
    return db_op, db_etl
