from cliente import cargar_dim_cliente
from mensajero import cargar_dim_mensajero
from sede import cargar_dim_sede
from ciudad import cargar_dim_ciudad
from Entregas_Completadas_Por_Dia import entregasPordia

from Entregas_Completadas_por_Hora import entregasPorHora
from fact_entregas_estado_etl import entregaPorEstado
from fact_novedades import novedades

def ejecutar_etl():
    cargar_dim_cliente()
    cargar_dim_mensajero()
    cargar_dim_sede()
    cargar_dim_ciudad()
    entregasPordia()
    entregasPorHora()
    entregaPorEstado()
    novedades()

    print("Proceso ETL completado.")

if __name__ == "__main__":
    ejecutar_etl()
