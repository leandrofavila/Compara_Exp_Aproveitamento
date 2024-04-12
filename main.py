from Con_DB import DB
from Dispara_Email import DisparaEmail
import pandas as pd

db = DB()


carregamentos = [436100]#db.carregamentos()

df = ''
temp_df_list = []
for car in carregamentos:
    #print(car)
    df = db.EXPS(car)
    for itens in df.iterrows():
        temp_df = db.disponibilidade_estoque(itens[1]['COD_ITEM'])
        temp_df_list.append(temp_df)

    temp_df_combined = pd.concat(temp_df_list, ignore_index=True)
    result_df = pd.merge(df, temp_df_combined, on='COD_ITEM', how='inner')
    disp = DisparaEmail(result_df, car)
    disp.dispara_email()
    #print(result_df.to_string())
