import cx_Oracle
import pandas as pd



def get_connection():
    dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
    connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
    cur = connection.cursor()
    return cur


def disponibilidade_estoque():
    cur = get_connection()
    query = """
        SELECT *  
        FROM (
            SELECT 
                ENG.COD_ITEM,
                (SELECT CASE 
                            WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) 
                            ELSE 0 
                        END AS MED_X
                 FROM FOCCO3I.TITENS_PDM PDM
                 INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID
                 WHERE ATR.DESCRICAO LIKE '%MEDIDA_X%' 
                 AND PDM.ITEM_ID = EMPF.ITEM_ID
                 FETCH FIRST ROW ONLY
                ) AS MED_X,
                (SELECT CASE 
                            WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) 
                            ELSE 0 
                        END AS MED_Y
                 FROM FOCCO3I.TITENS_PDM PDM
                 INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID
                 WHERE ATR.DESCRICAO LIKE '%MEDIDA_Y%' 
                 AND PDM.ITEM_ID = EMPF.ITEM_ID
                 FETCH FIRST ROW ONLY
                ) AS MED_Y,
                (SELECT CASE 
                            WHEN REGEXP_LIKE(PDM.CONTEUDO_ATRIBUTO, '^-?[0-9]+(\,[0-9]+)?$') THEN TO_NUMBER(REPLACE(PDM.CONTEUDO_ATRIBUTO, ',', '.')) 
                            ELSE 0 
                        END AS MED_Z
                 FROM FOCCO3I.TITENS_PDM PDM
                 INNER JOIN FOCCO3I.TATRIBUTOS ATR ON ATR.ID = PDM.ATRIBUTO_ID
                 WHERE ATR.DESCRICAO LIKE '%MEDIDA_Z%' 
                 AND PDM.ITEM_ID = EMPF.ITEM_ID
                 FETCH FIRST ROW ONLY
                ) AS MED_Z
            FROM FOCCO3I.TITENS_EMPR EMP
            INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG ON ENG.ITEMPR_ID_ITEM_BASE = EMP.ID
            INNER JOIN FOCCO3I.TITENS_EMPR EMPF ON EMPF.ID = ENG.ITEMPR_ID
            INNER JOIN FOCCO3I.TITENS TIT ON TIT.ID = EMPF.ITEM_ID
            WHERE TIT.SIT = 1
            AND TIT.DESC_TECNICA LIKE '%PAINEL LATERAL%'
        )
        WHERE SQRT(
            LEAST(
                POWER(NVL(MED_X, 0) - 75, 2) + POWER(NVL(MED_Y, 0) - 486, 2) + POWER(NVL(MED_Z, 0) - 1955, 2),
                POWER(NVL(MED_X, 0) - 75, 2) + POWER(NVL(MED_Y, 0) - 1955, 2) + POWER(NVL(MED_Z, 0) - 486, 2),
                POWER(NVL(MED_X, 0) - 486, 2) + POWER(NVL(MED_Y, 0) - 75, 2) + POWER(NVL(MED_Z, 0) - 1955, 2),
                POWER(NVL(MED_X, 0) - 486, 2) + POWER(NVL(MED_Y, 0) - 1955, 2) + POWER(NVL(MED_Z, 0) - 75, 2),
                POWER(NVL(MED_X, 0) - 1955, 2) + POWER(NVL(MED_Y, 0) - 75, 2) + POWER(NVL(MED_Z, 0) - 486, 2),
                POWER(NVL(MED_X, 0) - 1955, 2) + POWER(NVL(MED_Y, 0) - 486, 2) + POWER(NVL(MED_Z, 0) - 75, 2)
            )
        ) < 20
    """
    cur.execute(query)
    disp_estq = pd.DataFrame(cur.fetchall())
    cur.close()
    return disp_estq

print(disponibilidade_estoque())


