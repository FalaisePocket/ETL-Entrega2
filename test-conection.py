from config import connect_databases

try:
    db_op, db_etl = connect_databases()
    print("Conexión a las bases de datos exitosa")
except Exception as e:
    print(f"Error conectando a las bases de datos: {e}")
