import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import math
from email.mime.application import MIMEApplication


class DisparaEmail:
    def __init__(self, df, car):
        self.df = df
        self.car = car


    def dispara_email(self):
        msg = MIMEMultipart()
        #anexa os pdfs de cada item se existir
        for idx, arch in self.df.iterrows():
            file_path = "T:\\06_Desenhos_PDF\\" + str(arch['COD_ITEM']) + ".pdf"
            with open(file_path, "rb") as f:
                pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
                pdf_attachment.add_header('content-disposition', 'attachment', filename=str(arch['COD_ITEM']) + ".pdf")
                msg.attach(pdf_attachment)

        message = self.trata_email()
        password = "srengld21v3l1"
        msg['From'] = "ldeavila@sr.ind.br"
        recipients = ["ldeavila@sr.ind.br", "wesley@sr.ind.br"]#, "producao@sr.ind.br", "qualidade@sr.ind.br", "expedicao@sr.ind.br"]
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = "Aproveitamento Estoque"
        msg.attach(MIMEText(message, 'plain'))
        server = smtplib.SMTP('10.40.3.12: 465')
        server.starttls()
        server.login(msg['From'], password)
        server.sendmail(msg['From'], recipients, msg.as_string())
        server.quit()
        return "job's done"


    def trata_email(self):
        df = self.df
        nlis = f"{self.sauda()}.\nPara carregamento {str(self.car)} temos os itens:\n"
        for _, vals in df.iterrows():
            nlis += (f"\t - {vals['COD_ITEM']}[{vals['MASC_ID']}] {vals['QTDE']} "
                     f"{'unidades' if vals['QTDE'] > 1 else 'unidade'} "
                     f"porem há saldo do mesmo item com a mascara {str(vals['MASC']).rjust(6)}-"
                     f"{str(vals['DESC_MASC']).ljust(42)} quantidade {vals['SALDO_EST']}"
                     f" no almox 6.\n"
                     )

        print(nlis)
        return nlis


    @staticmethod
    def sauda():
        currentTime = datetime.datetime.now()
        if currentTime.hour < 12:
            return 'Bom dia'
        elif 12 <= currentTime.hour <= 18:
            return 'Boa tarde'
        else:
            return 'Boa noite'
