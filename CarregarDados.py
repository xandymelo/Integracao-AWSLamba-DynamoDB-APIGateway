import json
from datetime import date, datetime
import requests
import boto3


def lambda_handler(event, context):
    s3 = boto3.resource(
    service_name='s3',
    region_name='sa-east-1',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secrect-access-key') #conectando ao bucket S3
    data_url = date.today()
    data_hora = datetime.now().strftime('%Y-%m-%d %Hh%Mm%Ss') #Pegando a data de hoje
    linhas = []
    arq = open('/tmp/arq.txt','w+')
    #pegando os dados da API de meteorologia
    response = requests.get("https://apitempo.inmet.gov.br/estacao/dados/{}".format(data_url))
    #convertendo para formato json
    response_json = response.json()
    #caso os dados sejam de Pernambuco, enviar para o AWS Kinesis Stream
    for estacao in response_json:

        if estacao['UF'] == 'PE':
            #pegando somente as variáveis que irei usar
            resp =  """ "CODIGO_ESTACAO": "{}",
    "NOME_ESTACAO": "{}",
    "LATITUDE": "{}",
    "LONGITUDE": "{}",
    "HORARIO_COLETA": "{}",
    "PRESSAO_ATM_MAX": "{}",
    "RADIACAO_GLOBAL": "{}",
    "PRESSAO_ATM_INS": "{}",
    "PRESSAO_ATM_MIN": "{}",
    "PRECIPITACAO_TOT": "{}",
    "DATA_ATUAL": "{}" """.format(estacao['CD_ESTACAO'], estacao['DC_NOME'], estacao['VL_LATITUDE'], estacao['VL_LONGITUDE'], 
            estacao['HR_MEDICAO'], estacao['PRE_MAX'], estacao['RAD_GLO'], estacao['PRE_INS'], estacao['PRE_MIN'],
            estacao['CHUVA'], str(data_url))
            resp = "{" + resp + "}\n"
            linhas.append(resp)
            linhas.append('divisao')
    arq.writelines(linhas)
    arq.close()
    nome = "{}.txt".format(data_hora)
    s3.Bucket('dadosanalytic').upload_file(Filename="/tmp/arq.txt", Key=nome)   

        
    return 'Dados enviados para o S3 com sucesso!'


