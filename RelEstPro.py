import pandas as pd
from Con_DB import DB
from time import time

tot_time = time()

'''
df_demanda_pedido Data frame com todos os itens que estarão no carregamento ainda não gerado
'''
db = DB()
df_demanda_pedido = db.W_F3IGI_TESTE()
df_demanda_pedido.insert(3, 'SLD_POSITIVO', None)
df_sem_saldo = []
print('DATAFRAME DEMANDA EM UM PEDIDO')
print(df_demanda_pedido.to_string())

'''
Itera pelo data frame faz o calculo(qtde de itens em ordens abertas + o saldo hoje - a demanda caso o resultado seja
maior que zero é adicionado a coluna SLD_POSITIVO) caso contrario adiciona-se no dataframe para procura de semelhantes
(df_sem_saldo)
'''
for idx, row in df_demanda_pedido.iterrows():
    #É passado somente o código do item intecionalemnte para que retorne se há saldo dobrando em qualquer mascará
    df_saldo_positivo_fil = db.disponibilidade_estoque(row['COD_ITEM'])
    if not df_saldo_positivo_fil.empty:
        df_demanda_pedido.at[idx, 'SLD_POSITIVO'] = df_saldo_positivo_fil.iloc[0]['OPS_ABERTAS'] + \
                                                df_saldo_positivo_fil.iloc[0]['SALDO_EST'] - \
                                                df_saldo_positivo_fil.iloc[0]['DEMANDA']
    else:
        linha = {
            'COD_ITEM': row['COD_ITEM'],
            'MASC_FILHO': row['MASC_FILHO'],
            'DESC_FILHO': row['DESC_FILHO'],
            'QTDE_FILHO': row['QTDE_FILHO']
                 }
        df_sem_saldo.append(linha)

df_sem_saldo = pd.DataFrame(df_sem_saldo)
df_demanda_pedido = df_demanda_pedido[df_demanda_pedido['SLD_POSITIVO'].notna()].reset_index(drop=True)
print('DATAFRAME DO QUE HÁ SALDO POSITIVO PARA APROVEITAMENTO')
print(df_demanda_pedido.to_string())

'''
Agora para cada item do df_sem_saldo procura-se seus semelhantes e verifica mais uma vez se há saldo positivo 
caso sim adiciona a um novo data frame  
'''
dim_lis = ', '.join(map(str, df_sem_saldo['COD_ITEM'].tolist()))
dim_lis = db.dimencao_peca(dim_lis)
df_sem_saldo = pd.merge(df_sem_saldo, dim_lis, on='COD_ITEM', how='left')
df_demanda_pedido.insert(4, 'SEMELHANTES', None)
dic_semelhantes = {}


'''
Procura por semelhantes e adiciona como uma lista na coluna SEMELHANTES
'''
for idx, row in df_sem_saldo.iterrows():
    grp_modifi = row['DESC_FILHO'].split()
    grp_modifi = f'{grp_modifi[0]} {grp_modifi[1]}'
    #print(row['X'], row['Y'], row['Z'], 10, grp_modifi)
    semel_searched = db.semelhantes(row['X'], row['Y'], row['Z'], 1, grp_modifi)
    dic_semelhantes[row['COD_ITEM']] = semel_searched['COD_ITEM'].tolist() if isinstance(semel_searched, pd.DataFrame) else None

for item_origem, item_semel in dic_semelhantes.items():
    df_sem_saldo['SEMELHANTE'] = df_sem_saldo['COD_ITEM'].map(dic_semelhantes)



'''
Verifica se há saldo de algum semelhante e adiciona ao DF
'''
dic_sld_semel = {}
for idx, row in df_sem_saldo.iterrows():
    for item in row['SEMELHANTE']:
        sld_semel = db.disponibilidade_estoque(item)
        sld_semel = pd.DataFrame(sld_semel)
        sld_semel['SLD_POS_SEMEL'] = sld_semel['SALDO_EST'] + sld_semel['OPS_ABERTAS'] - sld_semel['DEMANDA']
        if not sld_semel.empty:
            semel_lis = sld_semel.iloc[0].tolist()
            dic_sld_semel[row['COD_ITEM']] = [semel_lis[0], semel_lis[2], semel_lis[3], semel_lis[7]]

for item_origem, item_semel in dic_semelhantes.items():
    df_sem_saldo['SLD_SEMEL'] = df_sem_saldo['COD_ITEM'].map(dic_sld_semel)

df_sld_semel = df_sem_saldo[pd.notna(df_sem_saldo["SLD_SEMEL"])]
df_sld_semel = df_sld_semel.drop(columns=['X', 'Y', 'Z', 'SEMELHANTE'])

print('DATAFRAME ONDE PEÇAS SEMELHANTES TEM SALDO')
print(df_sld_semel.to_string())


end = time()
hours, rem = divmod(end - tot_time, 3600)
minutes, seconds = divmod(rem, 60)
print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
