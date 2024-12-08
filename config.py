from sqlalchemy import create_engine

def connect_databases():
    db_op = create_engine('postgresql+psycopg2://alejandro:Alejo1193@localhost/rapidosyfuriosos')
    db_etl = create_engine('postgresql+psycopg2://alejandro:Alejo1193@localhost/etl')
    return db_op, db_etl
