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
