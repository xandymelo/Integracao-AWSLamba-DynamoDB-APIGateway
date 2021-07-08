import json
from datetime import date
import boto3

def lambda_handler(event, context):
    # TODO implement
    data = date.today()
    client = boto3.resource("dynamodb")
    table = client.Table("DadosAPI")
    dados = table.scan()['Items']
    pont = False
    for dado in dados:
        if dado['Data'] == str(data):
            pont = True
    if pont == False:
        return"Ainda não temos dados para data de hoje!"
    else:
        #pegar os dados do dynamo
        respo = table.get_item(
        Key={
            'Data': str(data)
            }
        )
        item = respo['Item']
        pressao_atm_max = item['PRESSAO_ATM_MAX']
        codigo_estacao_pama = item['CODIGO_ESTACAO_PAMA']
        nome_estacao_pama = item['NOME_ESTACAO_PAMA']
        latitude_pama = item['LATITUDE_PAMA']
        longitude_pama = item['LONGITUDE_PAMA']
        horario_coleta_pama = item['HORARIO_COLETA_PAMA']
        return { 'CODIGO_ESTACAO': codigo_estacao_pama,
        'NOME_ESTACAO': nome_estacao_pama,
        'LATITUDE': latitude_pama,
        'LONGITUDE': longitude_pama,
        'HORARIO_COLETA': horario_coleta_pama,
        'VALOR_OBSERVADO': pressao_atm_max }
