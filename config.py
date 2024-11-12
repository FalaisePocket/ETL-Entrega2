from sqlalchemy import create_engine
import psycopg2

# Función para conectar a las bases de datos de origen y destino usando SQLAlchemy
def connect_databases():
    try:
        # Conexión a la base de datos de origen
        db_op = create_engine(
            'postgresql+psycopg2://alejandro:Alejo1193@localhost/rapidosyfuriosos'
        )
        print("Conexión a la base de datos 'rapidosyfuriosos' exitosa.")

        # Conexión a la base de datos de destino
        db_etl = create_engine(
            'postgresql+psycopg2://alejandro:Alejo1193@localhost/etl'
        )
        print("Conexión a la base de datos 'etl' exitosa.")
    
    except Exception as e:
        print(f"Error en la conexión: {e}")
        db_op, db_etl = None, None

    return db_op, db_etl
