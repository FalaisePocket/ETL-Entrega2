from sqlalchemy import create_engine

def connect_databases():
    usuario='Dani'
    password='invitado'
    db_op = create_engine(f'postgresql+psycopg2://{usuario}:{password}@localhost/rapidosyfuriosos')
    db_etl = create_engine(f'postgresql+psycopg2://{usuario}:{password}@localhost/etl')
    return db_op, db_etl
