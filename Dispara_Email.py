import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from email.mime.application import MIMEApplication
import os


class DisparaEmail:
    def __init__(self, df):
        self.df = df


    def dispara_email(self):
        msg = MIMEMultipart()
        #anexa os pdfs de cada item se existir
        for _, arch in self.df.iterrows():
            file_path = "T:\\06_Desenhos_PDF\\" + str(arch['COD_ITEM']) + ".pdf"
            if os.path.exists(file_path):
                try:
                    with open(file_path, "rb") as f:
                        pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
                        pdf_attachment.add_header('content-disposition', 'attachment', filename=str(arch['COD_ITEM']) + ".pdf")
                        msg.attach(pdf_attachment)
                except Exception as e:
                    print(f"Erro ao anexar o arquivo {file_path}: {e}")
            else:
                print(f"Arquivo não encontrado: {file_path}")

        message = self.trata_email()
        password = "srengld21v3l1"
        msg['From'] = "ldeavila@sr.ind.br"
        recipients = ["ldeavila@sr.ind.br"]#, "wesley@sr.ind.br"]#, "qualidade@sr.ind.br", "producao@sr.ind.br", "ricardo@sr.ind.br", "qualidade@sr.ind.br", "expedicao@sr.ind.br"]
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = f"Aproveitamento Estoque Ped."
        msg.attach(MIMEText(message, 'plain'))
        server = smtplib.SMTP('10.40.3.12: 465')
        server.starttls()
        server.login(msg['From'], password)
        server.sendmail(msg['From'], recipients, msg.as_string())
        server.quit()
        return "job's done"


    def trata_email(self):
        df = self.df
        nlis = f"{self.sauda()}.\nPara pedido {'pedido tal'} temos os itens:\n"
        for _, vals in df.iterrows():
            if vals['SLD_SEMEL'] is False:
                nlis += (
                    f"\t -{str(vals['COD_ITEM']).ljust(6)}"
                    f"[{str(vals['MASC']).ljust(6)}] "
                    f"{str(vals['QTDE']).ljust(3)} "
                    f"{('unidades' if vals['QTDE'] > 1 else 'unidade').ljust(8)} "
                    f"porem há saldo do mesmo item com a mascara {str(int(vals['MASC_SLD'])).rjust(6)} - "
                    f"{str(int(vals['SLD_POSITIVO'])).rjust(3)} und.\n"
                    )
            else:
                nlis += "\nE para os itens a seguir temos itens semelhantes que gostaríamos de validar também:\n"
                print(vals['SLD_SEMEL'])
                lis_seml = vals['SLD_SEMEL']
                seml = list(lis_seml)
                nlis += (
                    f"\t - {str(vals['COD_ITEM']).ljust(6)}"
                    f"[{str(vals['MASC']).ljust(6)}] "
                    f"{str(vals['QTDE']).ljust(3)} "
                    f"{('unidades' if vals['QTDE'] > 1 else 'unidade').ljust(8)} "
                    f"porem há saldo do item {str(seml[0]).rjust(6)}[{str(seml[1]).rjust(6)}] {str(seml[2]).rjust(6)} -"
                    f"{str(seml[3]).rjust(3)} und.\n"
                    )
            
        nlis += '\nAmanda pode por favor verificar se realmente existem estes itens fisicamente? e se estão disponíveis' \
                ' para utilizarmos neste próximo carregamento?\n \nAt.te;'
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
