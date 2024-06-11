import cx_Oracle
import pandas as pd


class DB:
    def __init__(self):
        self.db_connection = None

    @staticmethod
    def get_connection():
        dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
        connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
        cur = connection.cursor()
        return cur

    def carregamentos(self):
        cur = self.get_connection()
        cur.execute(
            r"SELECT CARREGAMENTO FROM FOCCO3I.TSRENGENHARIA_CARREGAMENTOS WHERE CARREGAMENTO > 436000 "
            r"ORDER BY CARREGAMENTO DESC "
        )
        return [x[0] for x in cur.fetchall()]


    def EXPS(self, car):
        cur = self.get_connection()
        cur.execute(
            r"SELECT DISTINCT TIT.COD_ITEM, TOR.NUM_ORDEM, TOR.QTDE, TIT.DESC_TECNICA, CAR.CARREGAMENTO, TOR.TMASC_ITEM_ID, MASC.MASCARA "
            r"FROM FOCCO3I.TORDENS TOR "
            r"INNER JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC      ON TOR.ID = VINC.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR  ON VINC.CARERGAM_ID = CAR.ID "
            r"INNER JOIN FOCCO3I.TITENS_PLANEJAMENTO PLA          ON PLA.ID = TOR.ITPL_ID "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP                  ON EMP.ID = PLA.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TITENS TIT                       ON TIT.ID = EMP.ITEM_ID "
            r"INNER JOIN FOCCO3I.TMASC_ITEM MASC                  ON MASC.ID = TOR.TMASC_ITEM_ID "
            r"WHERE CAR.CARREGAMENTO = "+str(car)+" "
            #r"AND TIT.DESC_TECNICA LIKE '%EXP' "
        )
        car_exps = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "NUM_ORDEM", "QTDE", "DESC_TECNICA",
                                                         "CARREGAMENTO", "MASC_ID", "MASCARA"])
        car_exps["QTDE"] = car_exps["QTDE"].astype(int)
        cur.close()
        return car_exps


    def rodado(self, car):
        cur = self.get_connection()
        cur.execute(
            r"SELECT TOR.ID "
            r"FROM FOCCO3I.TORDENS TOR "
            r"INNER JOIN FOCCO3I.TORDENS_VINC_ITPDV VINC          ON VINC.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TITENS_PDV ITPDV                 ON ITPDV.ID = VINC.ITPDV_ID "
            r"INNER JOIN FOCCO3I.TPEDIDOS_VENDA PDV               ON PDV.ID = ITPDV.PDV_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT                  ON ROT.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TOPERACAO OP                     ON OP.ID = ROT.OPERACAO_ID "
            r"LEFT JOIN FOCCO3I.TORDENS_MOVTO MOV                 ON MOV.TORDEN_ROT_ID = ROT.ID "
            r"LEFT JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC       ON TOR.ID = VINC.ORDEM_ID "
            r"LEFT JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR   ON VINC.CARERGAM_ID = CAR.ID "
            r"WHERE CAR.carregamento = "+str(car)+" "
        )
        linhas = cur.fetchall()
        return len(linhas) > 0



    def disponibilidade_estoque(self, cod_item=None):
        cur = self.get_connection()
        if cod_item:
            c_cod_item = "WHERE TIT.COD_ITEM = "+str(cod_item)+" "
        else:
            c_cod_item = ''
        cur.execute(
            r"SELECT  TIT.COD_ITEM, TIT.DESC_TECNICA, MASC.ID MASCARA,MASC.MASCARA MASC_DESC, "
            r"(SELECT COALESCE(SUM(CASE WHEN PER.TIPO_ORD_DEM IN ('DC', 'DD') THEN PER.QTDE ELSE 0 END), 0)  "
            r"        FROM FOCCO3I.TPERFIL_ITENS PER  "
            r"        WHERE PER.ITPL_ID = PLA.ID "
            r"        AND (PER.TMASC_ITEM_ID = MASC.ID OR MASC.ID IS NULL))AS DEMANDA, "
            r"(SELECT COALESCE(SUM(CASE WHEN PER.TIPO_ORD_DEM IN ('OFA', 'OFM') THEN PER.QTDE ELSE 0 END), 0) "
            r"        FROM FOCCO3I.TPERFIL_ITENS PER "
            r"        WHERE PER.ITPL_ID = PLA.ID "
            r"        AND (PER.TMASC_ITEM_ID = MASC.ID OR MASC.ID IS NULL)) AS OPS, "
            r"FOCCO3I.MAN_EST_RETORNA_SALDO(1, TIT.ID, NULL, SYSDATE, NULL, MASC.ID, NULL, NULL, NULL, NULL, 1, 1) AS SALDO_EST "
            r"FROM FOCCO3I.TITENS_PLANEJAMENTO PLA "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP  ON EMP.ID = PLA.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TITENS TIT       ON TIT.ID = EMP.ITEM_ID "
            r"LEFT JOIN FOCCO3I.TMASC_ITEM MASC   ON MASC.ITEMPR_ID = EMP.ID "
            r""+c_cod_item+""
        )
        disp_estq = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "DESC_TECNICA", "MASC", "DESC_MASC", "DEMANDA",
                                                          "OPS_ABERTAS",  "SALDO_EST"])
        disp_estq = disp_estq[(disp_estq[["DEMANDA", "OPS_ABERTAS",  "SALDO_EST"]] != 0).any(axis=1)]
        disp_estq = disp_estq[(disp_estq["OPS_ABERTAS"] + disp_estq["SALDO_EST"] - disp_estq["DEMANDA"]) > 0]
        cur.close()
        return disp_estq


    def W_F3IGI_TESTE(self):
        cur = self.get_connection()
        cur.execute(
            r"SELECT TIT_FIL.COD_ITEM,  "
            r"TES.TMASC_ITEM_ID_FILHO,  "
            r"TIT_FIL.DESC_TECNICA AS DESC_FILHO,  "
            r"SUM(TES.QTDE_FILHO) "
            r"FROM FOCCO3I.W_F3IGI_TESTE TES "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP_PAI      ON EMP_PAI.ID = TES.ITEMPR_ID_PAI "
            r"INNER JOIN FOCCO3I.TITENS TIT_PAI           ON TIT_PAI.ID = EMP_PAI.ITEM_ID "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP_FIL      ON EMP_FIL.ID = TES.ITEMPR_ID_FILHO "
            r"INNER JOIN FOCCO3I.TITENS TIT_FIL           ON TIT_FIL.ID = EMP_FIL.ITEM_ID "
            r"INNER JOIN FOCCO3I.TITENS_PLANEJAMENTO TPL  ON TPL.ITEMPR_ID = EMP_PAI.ID "
            r"INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG    ON ENG.ITEMPR_ID = EMP_FIL.ID "
            r"WHERE TPL.FANTASMA = 0 "
            r"AND ENG.TP_ITEM = 'F' "
            r"GROUP BY TIT_FIL.COD_ITEM, TES.TMASC_ITEM_ID_FILHO, TIT_FIL.DESC_TECNICA "
        )
        rel_estrutura = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "MASC", "DESC_TECNICA",
                                                              "QTDE"])
        rel_estrutura['MASC'] = rel_estrutura['MASC'].astype('Int64')
        return rel_estrutura


    def semelhantes(self, x, y, z, variacao, grp_modf):
        ct = 0
        cur = self.get_connection()
        cur.execute(
            r"SELECT * "
            r"FROM ( "
            r"    SELECT "
            r"        ENG.COD_ITEM, "
            r"        (SELECT CASE "
            r"                    WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) "
            r"                    ELSE 0 "
            r"                END AS MED_X "
            r"         FROM FOCCO3I.TITENS_PDM PDM "
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_X%' "
            r"         AND PDM.ITEM_ID = EMPF.ITEM_ID "            
            r"         FETCH FIRST ROW ONLY "            
            r"        ) AS MED_X, " 
            r"        (SELECT CASE " 
            r"                    WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) "
            r"                    ELSE 0 "
            r"                END AS MED_Y "
            r"         FROM FOCCO3I.TITENS_PDM PDM "
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_Y%' "
            r"         AND PDM.ITEM_ID = EMPF.ITEM_ID "
            r"         FETCH FIRST ROW ONLY "
            r"        ) AS MED_Y, "
            r"        (SELECT CASE "
            r"                    WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) "
            r"                    ELSE 0 "
            r"                END AS MED_Z "
            r"         FROM FOCCO3I.TITENS_PDM PDM "            
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_Z%' "
            r"         AND PDM.ITEM_ID = EMPF.ITEM_ID "            
            r"         FETCH FIRST ROW ONLY "            
            r"        ) AS MED_Z "            
            r"    FROM FOCCO3I.TITENS_EMPR EMP " 
            r"    INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG ON ENG.ITEMPR_ID_ITEM_BASE = EMP.ID "
            r"    INNER JOIN FOCCO3I.TITENS_EMPR EMPF ON EMPF.ID = ENG.ITEMPR_ID "
            r"    INNER JOIN FOCCO3I.TITENS TIT ON TIT.ID = EMPF.ITEM_ID "
            r"    WHERE TIT.SIT = 1 "
            r"    AND TIT.DESC_TECNICA LIKE '%"+grp_modf+"%' "
            r") "
            r"WHERE SQRT( "
            r"    LEAST( "
            r"        POWER(NVL(MED_X, 0) - "+str(x)+", 2) + POWER(NVL(MED_Y, 0) - "+str(y)+", 2) + POWER(NVL(MED_Z, 0) - "+str(z)+", 2), " 
            r"        POWER(NVL(MED_X, 0) - "+str(x)+", 2) + POWER(NVL(MED_Y, 0) - "+str(z)+", 2) + POWER(NVL(MED_Z, 0) - "+str(y)+", 2), "
            r"        POWER(NVL(MED_X, 0) - "+str(y)+", 2) + POWER(NVL(MED_Y, 0) - "+str(x)+", 2) + POWER(NVL(MED_Z, 0) - "+str(z)+", 2), "
            r"        POWER(NVL(MED_X, 0) - "+str(y)+", 2) + POWER(NVL(MED_Y, 0) - "+str(z)+", 2) + POWER(NVL(MED_Z, 0) - "+str(x)+", 2), "
            r"        POWER(NVL(MED_X, 0) - "+str(z)+", 2) + POWER(NVL(MED_Y, 0) - "+str(x)+", 2) + POWER(NVL(MED_Z, 0) - "+str(y)+", 2), "
            r"        POWER(NVL(MED_X, 0) - "+str(z)+", 2) + POWER(NVL(MED_Y, 0) - "+str(y)+", 2) + POWER(NVL(MED_Z, 0) - "+str(x)+", 2) "
            r"    ) "
            r") < "+str(variacao)+" "
            r"FETCH FIRST 10 ROWS ONLY "
        )
        semelhantes = pd.DataFrame(cur.fetchall(), columns=['COD_ITEM', 'X', 'Y', 'Z'])
        if ct < 5:
            if semelhantes.empty:
                ct += 1
                DB.semelhantes(self, x, y, z, variacao + 20, grp_modf)
            else:
                return semelhantes
        else:
            return semelhantes
        cur.close()




    def dimencao_peca(self, cod_item):
        cur = self.get_connection()
        cur.execute(
            r"SELECT *   "
            r"FROM ( "
            r"    SELECT  "
            r"        TIT.COD_ITEM,  "
            r"        (SELECT MAX(PDM.CONTEUDO_ATRIBUTO) "
            r"         FROM FOCCO3I.TITENS_PDM PDM "
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_X%' AND PDM.ITEM_ID = EMP.ITEM_ID) AS MED_X, "
            r"        (SELECT MAX(PDM.CONTEUDO_ATRIBUTO) "
            r"         FROM FOCCO3I.TITENS_PDM PDM "
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_Y%' AND PDM.ITEM_ID = EMP.ITEM_ID) AS MED_Y, "
            r"        (SELECT MAX(PDM.CONTEUDO_ATRIBUTO) "
            r"         FROM FOCCO3I.TITENS_PDM PDM "
            r"         INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"         WHERE ATR.DESCRICAO LIKE '%MEDIDA_Z%' AND PDM.ITEM_ID = EMP.ITEM_ID) AS MED_Z "
            r"    FROM FOCCO3I.TITENS_EMPR EMP "
            r"    INNER JOIN FOCCO3I.TITENS TIT ON TIT.ID = EMP.ITEM_ID "
            r"    WHERE TIT.SIT = 1 "
            r"    AND TIT.COD_ITEM IN ("+str(cod_item)+")) "
        )

        dimencao_peca = pd.DataFrame(cur.fetchall(), columns=['COD_ITEM', 'X', 'Y', 'Z'])
        dimencao_peca[['X', 'Y', 'Z']] = dimencao_peca[['X', 'Y', 'Z']].replace({None: 0, 'NaN': 0})
        dimencao_peca[['X', 'Y', 'Z']] = dimencao_peca[['X', 'Y', 'Z']].apply(lambda x: x.astype(str).str.replace(',', '.')).astype(float)
        dimencao_peca[['X', 'Y', 'Z']] = dimencao_peca[['X', 'Y', 'Z']].round(0).astype(int)
        cur.close()
        return dimencao_peca
