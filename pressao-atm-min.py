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
        pressao_atm_min = item['PRESSAO_ATM_MIN']
        codigo_estacao_pami = item['CODIGO_ESTACAO_PAMI']
        nome_estacao_pami = item['NOME_ESTACAO_PAMI']
        latitude_pami = item['LATITUDE_PAMI']
        longitude_pami = item['LONGITUDE_PAMI']
        horario_coleta_pami = item['HORARIO_COLETA_PAMI']
        return { 'CODIGO_ESTACAO': codigo_estacao_pami,
        'NOME_ESTACAO': nome_estacao_pami,
        'LATITUDE': latitude_pami,
        'LONGITUDE': longitude_pami,
        'HORARIO_COLETA': horario_coleta_pami,
        'VALOR_OBSERVADO': pressao_atm_min }
