import pandas as pd
from config import connect_databases
from sqlalchemy import Column, Integer, Date, Time, String
from sqlalchemy.orm import declarative_base  # Cambiar la importación aquí

# Crear la base
base = declarative_base()

class FechaHora(base):
    __tablename__ = 'DimFecha'  # Nombre de la tabla

    fecha_id = Column(Integer, primary_key=True, autoincrement=True)  # ID autoincremental
    fecha = Column(Date)  # Columna de fecha
    hora = Column(Time)  # Columna de hora
    dia_semana = Column(String)  # Día de la semana
    mes = Column(String)  # Mes

def cargar_dim_fecha():
    db_op, db_etl = connect_databases()

    # Eliminar la tabla si existe y crearla de nuevo
    base.metadata.drop_all(db_etl, tables=[FechaHora.__table__])

    # Crear la tabla
    base.metadata.create_all(db_etl)

    print("DimFecha cargado correctamente.")
