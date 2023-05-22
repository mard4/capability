import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import *
import numpy as np
import pyathena
from pyathena import connect
from pyathena.pandas.util import as_pandas
from pyathena.pandas.cursor import PandasCursor

# # STRUMENTO OTTICO TIPO 2
dati = pd.read_csv(r'file.csv')
# si converte il formato da 12 ore a 24 ore
dati['timestamp'] = pd.to_datetime(dati['DateTimeRec']).dt.strftime('%Y-%m-%d %H:%M:%S')
dati['timestamp'] = pd.to_datetime(dati.pop('DateTimeRec'), format='%m/%d/%Y %I:%M:%S %p')

#decido di concentrarmi solo su una ricetta per evitare gli outlier quindi mantengo la ricetta n 54
# sarà necessario rifiltrare i dati rilevati dallo strumento ottico di tipo2 in base alla data e ora
mask_recipe = (dati_filt['SAP'] == 37)
dati_ric = dati_filt.loc[mask_recipe]

# search columns that contains Thickness value
colNames = dati_ric.columns[dati_ric.columns.str.contains(pat = 'Thickness')] 
thickness_S2 = dati_ric[[colNames]]
thickness_S2.to_excel('thickness_mis37_strumento2.xlsx')

# # STRUMENTO TIPO 1
query = """
    SELECT * FROM x JOIN y
         on a.m = b.idvariable JOIN z on a.d = c.item
    WHERE x.year='2023' and x.month='01' and x.day='20' and x.d = 57455 and a.hour='06'"""
# Execution of the query and result stored in a pandas dataframe
cursor = connection.cursor(PandasCursor)
df = cursor.execute(query).as_pandas() 
# Remember to close the Athena connection
connection.close()
# i dati rilevati dallo strumento di Tipo 1 sono rilevati sulla base dell'orario UTC+1, 
# quindi è necessario implementare un'ora in più
df ['timestamp_utc'] = pd.to_datetime(df['ts'].astype(str).str[:10], unit='s', origin='unix')
df ['timestamp'] = df ['timestamp_utc'].apply(lambda x: x+timedelta(hours=1))
# seleziona tutte le righe che contengono lo spessore
df_thickness = df[df['variabledescription'].str.contains("Thickness")]
# seleziona solo le colonne di interesse
df_filt = df_thickness[['variabledescription','v','timestamp']]
# crea una pivot perchè in questo caso i dati sono indicati nelle righe non nelle colonne
pivot_width = df_filt.pivot_table(index='timestamp', 
                                  columns='variabledescription', 
                                  values='v', 
                                  aggfunc='first').reset_index().fillna('')
# filtro per ricetta (timestamp dato da dati strumento ottico 2 qui non sono disponibili)
start_date = '01-20-2023 07:00:00'
end_date = '01-20-2023 07:42:00'
mask = (pivot_width['timestamp'] > start_date) & (pivot_width['timestamp'] <= end_date)
s1_thick = pivot_width.loc[mask]
s1_thick.to_excel('thickness_mis37.xlsx')

