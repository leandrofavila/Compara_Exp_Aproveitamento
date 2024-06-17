import pandas as pd
from Con_DB import DB
from time import time
import os
import pyautogui
from Dispara_Email import DisparaEmail
from colorama import Fore, Style


def medir_tempo():
    start_time = time()

    def finalizar_tempo(func):
        end_time = time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        print("Tempo {}: {:0>2}:{:0>2}:{:05.2f}".format(str(func), int(hours), int(minutes), seconds))
    return finalizar_tempo


def filtra_df(df):
    for idx, line in df.iterrows():
        try:
            os.startfile("T:\\06_Desenhos_PDF\\" + str(line['COD_ITEM']) + ".pdf", 'open')
            if 'SLD_SEMEL' in df.columns:
                os.startfile("T:\\06_Desenhos_PDF\\" + str(line['SLD_SEMEL'][0]) + ".pdf", 'open')

            sld_pos = line['SLD_POSITIVO'] if 'SLD_POSITIVO' in df.columns else None
            qtde = line['QTDE']
            sld_seml = list(line['SLD_SEMEL']) if 'SLD_SEMEL' in df.columns else '     '
            confirm_botao = pyautogui.confirm(f'Saldo Positivo = {sld_pos}'
                                              f'\nNecessidade = {qtde} '
                                              f'\nCod Semelhante = {sld_seml[0]}'
                                              f'\nSaldo do semelhante = {sld_seml[3]}'
                                              f'\nCod Mascara = {sld_seml[1]}'
                                              f'\nDesc Mascara = {sld_seml[2]}', buttons=['USAR', 'NÃO USAR'])
            df.drop(idx, inplace=True) if confirm_botao == 'NÃO USAR' else next
            continue
        except FileNotFoundError:
            print('Não encontrado na pasta dos PDFs', line)
    return df


'''
df_demanda_pedido Data frame com todos os itens que estarão no carregamento ainda não gerado
'''
db = DB()
df_demanda_pedido = db.W_F3IGI_TESTE()
df_demanda_pedido.insert(3, 'SLD_POSITIVO', None)
df_demanda_pedido.insert(4, 'MASC_SLD', None)
df_sem_saldo = []
#print('DATAFRAME DEMANDA EM UM PEDIDO')
#print(df_demanda_pedido.to_string())

'''
Itera pelo data frame faz o calculo(qtde de itens em ordens abertas + o saldo hoje - a demanda caso o resultado seja
maior que zero é adicionado a coluna SLD_POSITIVO) caso contrario adiciona-se no dataframe para procura de semelhantes
(df_sem_saldo)
'''
medir_tempo_execucao = medir_tempo()
for idx, row in df_demanda_pedido.iterrows():
    #É passado somente o código do item intecionalemnte para que retorne se há saldo dobrando em qualquer mascará
    df_saldo_positivo_fil = db.disponibilidade_estoque(row['COD_ITEM'])
    if not df_saldo_positivo_fil.empty:
        df_demanda_pedido.at[idx, 'SLD_POSITIVO'] = df_saldo_positivo_fil.iloc[0]['OPS_ABERTAS'] + \
                                                df_saldo_positivo_fil.iloc[0]['SALDO_EST'] - \
                                                df_saldo_positivo_fil.iloc[0]['DEMANDA']
        df_demanda_pedido.at[idx, 'MASC_SLD'] = df_saldo_positivo_fil.iloc[0]['MASC']
    else:
        linha = {
            'COD_ITEM': row['COD_ITEM'],
            'MASC': row['MASC'],
            'DESC_TECNICA': row['DESC_TECNICA'],
            'QTDE': row['QTDE']
                 }
        df_sem_saldo.append(linha)

df_sem_saldo = pd.DataFrame(df_sem_saldo)
df_demanda_pedido = df_demanda_pedido[df_demanda_pedido['SLD_POSITIVO'].notna()].reset_index(drop=True)
df_demanda_pedido = filtra_df(df_demanda_pedido)
df_demanda_pedido = df_demanda_pedido[df_demanda_pedido['MASC'] != df_demanda_pedido['MASC_SLD']]
print('\n--DATAFRAME DO QUE HÁ SALDO POSITIVO PARA APROVEITAMENTO E O CODIGO DO ITEM É IGUAL')
print(df_demanda_pedido.to_string() if not df_demanda_pedido.empty else 'Não há saldo para os itens do carregamento')
medir_tempo_execucao('separar saldo direto')
'''
Agora para cada item do df_sem_saldo procura-se seus semelhantes e verifica mais uma vez se há saldo positivo
caso sim adiciona a um novo data frame
'''

dim_lis = ', '.join(map(str, df_sem_saldo['COD_ITEM'].tolist()))
dim_lis = db.dimencao_peca(dim_lis)
df_sem_saldo = pd.merge(df_sem_saldo, dim_lis, on='COD_ITEM', how='left')
df_demanda_pedido.insert(4, 'SEMELHANTES', None)
dic_semelhantes = {}

#print('\n--ITENS QUE SOBRARAM COM SUAS DIMENSÕES')
#print(df_sem_saldo.to_string())

'''
Procura por semelhantes e adiciona como uma lista na coluna SEMELHANTES
'''
medir_tempo_execucao = medir_tempo()
for _, row in df_sem_saldo.iterrows():
    grp_modifi = row['DESC_TECNICA'].split()
    grp_modifi = f'{grp_modifi[0]} {grp_modifi[1]}'
    #print(row['X'], row['Y'], row['Z'], 10, grp_modifi)
    semel_searched = db.semelhantes(row['X'], row['Y'], row['Z'], 1, grp_modifi)
    if isinstance(semel_searched, pd.DataFrame) and not semel_searched.empty:
        dic_semelhantes[row['COD_ITEM']] = semel_searched['COD_ITEM'].tolist()
    else:
        dic_semelhantes[row['COD_ITEM']] = False

for item_origem, item_semel in dic_semelhantes.items():
    df_sem_saldo['SEMELHANTE'] = df_sem_saldo['COD_ITEM'].map(dic_semelhantes)
medir_tempo_execucao('listar semelhantes')

#print('--DATAFRAME COM SUAS LISTAS DE SEMELHANTES')
#print(df_sem_saldo.to_string())


'''
Verifica se há saldo de algum semelhante e adiciona ao DF
'''
medir_tempo_execucao = medir_tempo()

dic_sld_semel = {}
for idx, row in df_sem_saldo.iterrows():
    sml = row['SEMELHANTE']
    if sml is not None:
        for item in sml:
            sld_semel = db.disponibilidade_estoque(item)
            sld_semel = pd.DataFrame(sld_semel)
            sld_semel['SLD_POS_SEMEL'] = sld_semel['SALDO_EST'] + sld_semel['OPS_ABERTAS'] - sld_semel['DEMANDA']
            if not sld_semel.empty:
                semel_lis = sld_semel.iloc[0].tolist()
                dic_sld_semel[row['COD_ITEM']] = [semel_lis[0], semel_lis[2], semel_lis[3], semel_lis[7]]
    else:
        continue
for item_origem, item_semel in dic_semelhantes.items():
    df_sem_saldo['SLD_SEMEL'] = df_sem_saldo['COD_ITEM'].map(dic_sld_semel)

df_sld_semel = df_sem_saldo[pd.notna(df_sem_saldo["SLD_SEMEL"])]
df_sld_semel = df_sld_semel.drop(columns=['X', 'Y', 'Z', 'SEMELHANTE'])
df_sld_semel = filtra_df(df_sld_semel)

print('\n--DATAFRAME ONDE PEÇAS SEMELHANTES TÊM SALDO')
#print(df_sld_semel.to_string())

df = pd.concat([df_demanda_pedido, df_sld_semel], axis=0, ignore_index=True)
df = df.drop(columns='SEMELHANTES')
df['QTDE'] = df['QTDE'].astype(int)
df['SLD_SEMEL'] = df['SLD_SEMEL'].fillna(False)
#print(df.to_string())
medir_tempo_execucao('verificar saldo semelhantes')

if not df.empty:
    disp = DisparaEmail(df)
    disp.dispara_email()
else:
    print(Fore.RED + Style.BRIGHT + 'Não há nada pra ver aqui, pode continuar o baile.')
