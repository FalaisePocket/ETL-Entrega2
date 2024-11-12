from sqlalchemy import create_engine
import psycopg2

##SI no funciona el import intentar con la funcion comentada y eliminar la otra funcion

# Función para conectar a las bases de datos de origen y destino usando SQLAlchemy
'''def connect_databases():
    # Conexión a la base de datos de origen (por ejemplo, PostgreSQL)
    db_op = create_engine(
        'postgresql+psycopg2://Dani:invitado@localhost/rapidosyfuriosos'
    )

    # Conexión a la base de datos de destino
    db_etl = create_engine(
        'postgresql+psycopg2://Dani:invitado@localhost/etl'
    )

    return db_op, db_etl
'''

def connect_databases():
    # Conexión a la base de datos de origen (por ejemplo, PostgreSQL)
    db_op = psycopg2.connect(
        host="localhost",
        database="rapidosyfuriosos",  # Nombre de la base de datos de origen
        user="Dani",             # Usuario de la base de datos de origen
        password="invitado"      # Contraseña del usuario
    )

    # Conexión a la base de datos de destino (puede ser otra base de datos, dependiendo de tu configuración)
    db_etl = psycopg2.connect(
        host="localhost",
        database="etl",  # Nombre de la base de datos de destino
        user="Dani", # Usuario de la base de datos de destino
        password="invitado"  # Contraseña del usuario
    )

    return db_op, db_etl

