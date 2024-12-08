import pandas as pd
from config import connect_databases
from sqlalchemy import create_engine, Column, Integer, Date, Time
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

def cargar_dim_fecha():
    db_op, db_etl = connect_databases()

    base=sqlalchemy.orm.declarative_base()

    class FechaHora(base):
        __tablename__ = 'DimFecha'  # Nombre de la tabla

        fecha_id = Column(Integer, primary_key=True, autoincrement=True)  # ID autoincremental
        fecha = Column(Date, nullable=False)  # Columna de fecha
        hora = Column(Time, nullable=False)  # Columna de hora

    base.metadata.create_all(db_etl)
    # Crear un DataFrame vacío con las columnas deseadas
    #fecha = pd.DataFrame(columns=['fecha_id', 'fecha', 'hora'])
    
    # Insertar la tabla en la base de datos
    #fecha.to_sql('DimFecha', db_etl, if_exists='replace', index=False)

    
    print("DimFecha cargado correctamente.")
