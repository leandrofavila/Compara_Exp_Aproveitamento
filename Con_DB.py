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
            r"AND TIT.DESC_TECNICA LIKE '%EXP' "
        )
        car_exps = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "NUM_ORDEM", "QTDE", "DESC_TECNICA",
                                                         "CARREGAMENTO", "MASC_ID", "MASCARA"])
        car_exps["QTDE"] = car_exps["QTDE"].astype(int)
        cur.close()
        return car_exps


    def dim_itens(self, cod_item, dimensao, variacao):
        #print(cod_item, dimensao, variacao)
        if variacao is None:
            variacao = 0

        if dimensao is None:
            com_dim = ''
        else:
            com_dim = (
                r"WHERE (TABELA.MED_X BETWEEN "
                r""+str(dimensao)+" - "+str(variacao)+" AND "+str(dimensao)+" + "+str(variacao)+") "
                r"OR (TABELA.MED_Y BETWEEN "
                r""+str(dimensao)+" - "+str(variacao)+" AND "+str(dimensao)+" + "+str(variacao)+") "
                r"OR (TABELA.MED_Z BETWEEN "+str(dimensao)+" - "+str(variacao)+" AND "+str(dimensao)+" + "+str(variacao)+") "
            )
        print(com_dim)
        cur = self.get_connection()
        cur.execute(
            r"SELECT *  FROM ( "
            r"SELECT ENG.COD_ITEM, TIT.DESC_TECNICA, "
            r"         (SELECT TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO,',', '.')) "
            r"            FROM FOCCO3I.TITENS_PDM PDM "
            r"            INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"            WHERE ATR.DESCRICAO LIKE '%MEDIDA_X%' AND PDM.ITEM_ID = EMPF.ITEM_ID) AS MED_X, "
            r"         (SELECT TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO,',', '.')) "
            r"            FROM FOCCO3I.TITENS_PDM PDM "
            r"            INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"            WHERE ATR.DESCRICAO LIKE '%MEDIDA_Y%' AND PDM.ITEM_ID = EMPF.ITEM_ID) AS MED_Y, "
            r"         (SELECT TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO,',', '.')) "
            r"            FROM FOCCO3I.TITENS_PDM PDM "
            r"            INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID "
            r"            WHERE ATR.DESCRICAO LIKE '%MEDIDA_Z%' AND PDM.ITEM_ID = EMPF.ITEM_ID) AS MED_Z "
            r"FROM FOCCO3I.TITENS_EMPR EMP "
            r"INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG ON ENG.ITEMPR_ID_ITEM_BASE = EMP.ID "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMPF ON EMPF.ID = ENG.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TITENS TIT ON TIT.ID = EMPF.ITEM_ID "
            r"WHERE EMP.COD_ITEM = "+str(cod_item)+") TABELA "
            r""+com_dim+" "
        )

        dim_itens = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "DESC_TECNICA", "MED_X", "MED_Y", "MED_Z"])
        cur.close()
        return dim_itens

    def disponibilidade_estoque(self, cod_item):
        cur = self.get_connection()
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
            r"FOCCO3I.MAN_EST_RETORNA_SALDO(1, TIT.ID, '6', SYSDATE, NULL, MASC.ID, NULL, NULL, NULL, NULL, 1, 1) AS SALDO_EST "
            r"FROM FOCCO3I.TITENS_PLANEJAMENTO PLA "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP  ON EMP.ID = PLA.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TITENS TIT       ON TIT.ID = EMP.ITEM_ID "
            r"LEFT JOIN FOCCO3I.TMASC_ITEM MASC   ON MASC.ITEMPR_ID = EMP.ID "
            r"WHERE TIT.COD_ITEM = "+str(cod_item)+" "
        )

        disp_estq = pd.DataFrame(cur.fetchall(), columns=["COD_ITEM", "DESC_TECNICA", "MASC", "DESC_MASC", "DEMANDA",
                                                          "OPS_ABERTAS",  "SALDO_EST"])
        disp_estq = disp_estq[(disp_estq[["DEMANDA", "OPS_ABERTAS",  "SALDO_EST"]] != 0).any(axis=1)]
        disp_estq = disp_estq[(disp_estq["OPS_ABERTAS"] + disp_estq["SALDO_EST"] - disp_estq["DEMANDA"]) > 0]
        cur.close()
        return disp_estq
