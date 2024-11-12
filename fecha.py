import psycopg2
from datetime import datetime

# Conexión a la base de datos
def connect_to_db(dbname):
    return psycopg2.connect(
        host="localhost",
        dbname=dbname,
        user="alejandro",
        password="Alejo1193"
    )

# Conectar a la base de datos de destino
conn_destino = connect_to_db("etl")
cur_destino = conn_destino.cursor()

# Crear la tabla de dimensión 'fecha_hora' (almacena fechas y horas)
create_dim_fecha_query = """
CREATE TABLE IF NOT EXISTS fecha_hora (
    key_fecha_hora SERIAL PRIMARY KEY,
    fecha DATE,
    hora TIME
);
"""

# Ejecutar la creación de la tabla
cur_destino.execute(create_dim_fecha_query)

# Confirmar los cambios
conn_destino.commit()

# Cerrar la conexión
cur_destino.close()
conn_destino.close()

print("Dimensión fecha_hora creada con éxito.")
