import pandas as pd
from sqlalchemy import create_engine

def filtrar_por_fechas(dataframe, columna_fecha, fecha_inicio, fecha_fin):#FernandoSanchez
     dataframe['orderDate'] = pd.to_datetime(dataframe['orderDate'])
     fecha_inicio = pd.to_datetime(fecha_inicio)
     fecha_fin = pd.to_datetime(fecha_fin)
     mask = (dataframe[columna_fecha] >= fecha_inicio) & (dataframe[columna_fecha] <= fecha_fin)#FernandoSanchez
     return dataframe.loc[mask] #FernandoSanchez aqui con el loc filtramos por la etiqueta mask para obtener cuales cumplen con la condicion de la etiqueta y devolverlos

def generar_reporte(dataframe, filas, columnas, valores, funcion_agrupadora):#FernandoSanchez
    pivot_table = pd.pivot_table(dataframe, values=valores, index=filas, columns=columnas, aggfunc=funcion_agrupadora, fill_value=0)
    return pivot_table

def escribir_en_postgres(dataframe, nombre_tabla, engine, if_exists='fail'):#FernandoSanchez aqui generamos la escritura en la DB
    dataframe.to_sql(nombre_tabla, engine, if_exists=if_exists, index=False)

