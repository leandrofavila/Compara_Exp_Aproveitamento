from Con_DB import DB
from Dispara_Email import DisparaEmail
import pandas as pd

db = DB()
carregamentos = db.carregamentos()

old_car = pd.read_csv('lista_carregamentos.csv')

print('***APROVEITAMENTO ESTOQUE***')

df = ''
temp_df_list = []
for car in carregamentos:
    if car not in old_car['CARREGAMENTO'].tolist():
        print(car)
        df = db.EXPS(car)
        for itens in df.iterrows():
            temp_df = db.disponibilidade_estoque(itens[1]['COD_ITEM'])
            temp_df_list.append(temp_df)

        if not df.empty:
            temp_df_combined = pd.concat(temp_df_list, ignore_index=True)
            result_df = pd.merge(df, temp_df_combined, on='COD_ITEM', how='inner')
            disp = DisparaEmail(result_df, car)
            disp.dispara_email()

    if db.rodado(car):
        with open('lista_carregamentos.csv', 'a') as car_lis:
            car_lis.write('\n')
            car_lis.write(str(car))
