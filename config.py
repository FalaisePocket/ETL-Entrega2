from sqlalchemy import create_engine

# Función para conectar a las bases de datos de origen y destino usando SQLAlchemy
def connect_databases():
    # Conexión a la base de datos de origen (por ejemplo, PostgreSQL)
    db_op = create_engine(
        'postgresql+psycopg2://Dani:invitado@localhost/rapidosyfuriosos'
    )

    # Conexión a la base de datos de destino
    db_etl = create_engine(
        'postgresql+psycopg2://Dani:invitado@localhost/etl'
    )

    return db_op, db_etl
