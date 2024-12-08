from sqlalchemy import create_engine

def connect_databases():
    db_op = create_engine('postgresql+psycopg2://Dani:invitado@localhost/rapidosyfuriosos')
    db_etl = create_engine('postgresql+psycopg2://Dani:invitado@localhost/etl')
    return db_op, db_etl
