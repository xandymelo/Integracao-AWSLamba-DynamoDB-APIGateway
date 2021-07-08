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
        precipitacao_total = item['PRECIPITACAO_TOT']
        return {'VALOR_OBSERVADO': precipitacao_total}
