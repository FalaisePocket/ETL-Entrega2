# ProyectoFinal_ADA :rocket:

## Descripción :page_facing_up:

### Sistema de Analítica de Datos para una Empresa de Mensajería

La empresa de mensajería "Rápidos y Furiosos" busca optimizar su servicio mediante un sistema de análisis de datos que ayude en la toma de decisiones. Este sistema tiene como objetivo proporcionar información detallada sobre las operaciones de mensajería, incluyendo el rendimiento de los mensajeros, la eficiencia en las entregas, y las novedades durante el proceso de envío.

### Objetivo:

El sistema permite a la empresa:
- Analizar el número de servicios solicitados por cliente y mes.
- Determinar la eficiencia de los mensajeros (quiénes son los que más servicios prestan).
- Identificar las sedes que solicitan más servicios.
- Medir el tiempo promedio de entrega desde la solicitud hasta el cierre del servicio.
- Analizar los tiempos de espera por cada fase del servicio.
- Identificar las novedades más comunes que afectan los tiempos de entrega.

## Instrucciones para ejecutar el código :computer:

1. **Instalar Python:**

   Si no tienes Python instalado, puedes descargarlo desde el sitio web oficial de Python:
   - [Descargar Python](https://www.python.org/downloads/)

   Asegúrate de añadir Python al PATH durante la instalación para poder ejecutarlo desde cualquier ubicación en la línea de comandos.

2. **Clonar el repositorio:**

   Si aún no has clonado el repositorio en tu máquina local, puedes hacerlo utilizando Git:

   ```bash
   git clone https://github.com/FalaisePocket/ETL-Entrega2.git

3. **Instalar los requerimientos:**
```bash
pip install -r requirements.txt
```

4. **Tener la bodega de datos y una base de datos vacia:**
```bash
  https://drive.google.com/drive/u/2/folders/1vBmqLXuHC5oSoVFLbU8b6L9hBJ-sKqZ1
```
5. **Editar los archivos locales con tus datos:**
   - renombra 'config_copy.yaml' a config.yaml y edita tus datos
   - edita 'config.py' de esta forma: ```
                                     db_op = create_engine('postgresql+psycopg2://@usuario:@contraseña@localhost/@Nombre-de-base-datos')
                                     db_etl = create_engine('postgresql+psycopg2://@usuario:@contraseña@localhost/@Nombre-de-base-datos-etl')
                                      ```
     
## Estructura del Proyecto :file_folder:

El proyecto está compuesto por los siguientes archivos:

- `etl.py`: Carga y transformación de datos desde las bases de datos.
- `preguntas.ipynb`: Visualización de los resultados en gráficos.



## Resultados :bar_chart:

1. **Promedio de tiempo de entrega**: El sistema calcula el tiempo promedio entre las fases del servicio.
2. **Ranking de novedades**: Identifica las novedades más comunes que afectan a los servicios.
3. **Visualizaciones**: Muestra gráficos sobre los tiempos de espera, las fases del servicio y las novedades.

## Contribuyentes :busts_in_silhouette:

- [Zamorano Arango, Alejandro](https://github.com/AlejoZA) - 201941088
- [Valencia Ñañez, Victor Daniel](https://github.com/FalaisePocket) - 202026608
- [Mosquera Zapata, Wilson Andres](https://github.com/andresengineer) - 202182116
  


## Institución :mortar_board:

- Universidad: Universidad del Valle, Cali, Colombia.
- Curso: Introducción a la ciencia de datos
- Profesor: Oswaldo Solarte Pabón
- Semestre: 2024-02
