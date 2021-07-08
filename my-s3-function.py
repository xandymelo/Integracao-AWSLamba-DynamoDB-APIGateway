import json
import urllib.parse
import boto3
from datetime import date
import decimal

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    data = date.today()
    client = boto3.resource("dynamodb")
    table = client.Table("DadosAPI")
    dados = table.scan()['Items']
    pont = False
    for dado in dados:
        if dado['Data'] == str(data):
            pont = True
    if pont == False:
        #criar um novo dado e adicionar ao Dynamo
        #PAMA = Pressao Atm. Máxima
        #PAMI = Pressão Atm. Mínima
        #PAIN = Pressão Atm. Instantânea
        table.put_item(
            Item={
            'Data': str(data),
            'PRECIPITACAO_TOT': 'None',
            'RADIACAO_GLOBAL': 'None',
            'CODIGO_ESTACAO_PAMA': 'None',
            'NOME_ESTACAO_PAMA': 'None',
            'LATITUDE_PAMA': 'None',
            'LONGITUDE_PAMA': 'None',
            'HORARIO_COLETA_PAMA': 'None',
            'PRESSAO_ATM_MAX': 'None',
            'CODIGO_ESTACAO_PAMI': 'None',
            'NOME_ESTACAO_PAMI': 'None',
            'LATITUDE_PAMI': 'None',
            'LONGITUDE_PAMI': 'None',
            'HORARIO_COLETA_PAMI': 'None',
            'PRESSAO_ATM_MIN': 'None',
            'PRESSAO_ATM_INS': 'None'
            
            }
        )
    #pegar os dados do dynamo para comparar com o do S3
    respo = table.get_item(
    Key={
        'Data': str(data)
        }
    )
    item = respo['Item']
    if item['PRESSAO_ATM_INS'] == 'None':
        valor = 0
        qtd_de_valores = 0
    else:
        valor, qtd_de_valores = item['PRESSAO_ATM_INS'].split(",")
        valor = float(valor)
    
    

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    response = None
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    #variáveis precipitacao_total
    if item['PRECIPITACAO_TOT'] == 'None':
        precipitacao_total = 0
    else:
        precipitacao_total = float(item['PRECIPITACAO_TOT'])
    #variáveis radiacao_global
    if item['RADIACAO_GLOBAL'] == 'None':
        radiacao_global = 0
    else:
        radiacao_global = float(item['RADIACAO_GLOBAL'])
    #variáveis Pressao Atm. Máxima
    if item['PRESSAO_ATM_MAX'] == "None":
        pressao_atm_max = float("-inf")
    else:
        pressao_atm_max = float(item['PRESSAO_ATM_MAX'])
    codigo_estacao_pama = 'None'
    latitude_pama = 'None'
    longitude_pama = 'None'
    horario_coleta_pama = 'None'
    nome_estacao_pama = 'None'
    #variáveis Pressao Atm. Minima
    if item['PRESSAO_ATM_MIN'] == "None":
        pressao_atm_min = float("inf")
    else:
        pressao_atm_min = float(item['PRESSAO_ATM_MIN'])
    codigo_estacao_pami = 'None'
    latitude_pami = 'None'
    longitude_pami = 'None'
    horario_coleta_pami = 'None'
    nome_estacao_pami = 'None'
    #variáveis Pressao Atm. Instantânea
    pressao_atm_ins = 0
    if response != None:
        arq = response['Body'].read().decode()
        arq = arq.split("divisao")
        arq.pop()
        for arquivos in arq:
            arquivos_json = json.loads(arquivos)
            #precipitacao total
            if arquivos_json['PRECIPITACAO_TOT'] != 'None':
                precipitacao_total += float(arquivos_json['PRECIPITACAO_TOT'])
            #radiacao global
            if arquivos_json['RADIACAO_GLOBAL'] != 'None':
                radiacao_global += float(arquivos_json['RADIACAO_GLOBAL'])
            #Pressão Atm. Máxima
            if arquivos_json['PRESSAO_ATM_MAX'] != 'None':
                if pressao_atm_max < float(arquivos_json['PRESSAO_ATM_MAX']):
                    pressao_atm_max = float(arquivos_json['PRESSAO_ATM_MAX'])
                    codigo_estacao_pama = arquivos_json['CODIGO_ESTACAO']
                    latitude_pama = arquivos_json['LATITUDE']
                    longitude_pama = arquivos_json['LONGITUDE']
                    horario_coleta_pama = arquivos_json['HORARIO_COLETA']
                    nome_estacao_pama = arquivos_json['NOME_ESTACAO']
                
            #Pressão Atm. Mínima
            if arquivos_json['PRESSAO_ATM_MIN'] != 'None':
                if pressao_atm_min > float(arquivos_json['PRESSAO_ATM_MIN']):
                    pressao_atm_min = float(arquivos_json['PRESSAO_ATM_MIN'])
                    codigo_estacao_pami = arquivos_json['CODIGO_ESTACAO']
                    latitude_pami = arquivos_json['LATITUDE']
                    longitude_pami = arquivos_json['LONGITUDE']
                    horario_coleta_pami = arquivos_json['HORARIO_COLETA']
                    nome_estacao_pami = arquivos_json['NOME_ESTACAO']
            #Pressão Atm. Instantânea
            if arquivos_json['PRESSAO_ATM_INS'] != 'None':
                if valor == 'None':
                    valor = float(arquivos_json['PRESSAO_ATM_INS'])
                else:
                    valor += float(arquivos_json['PRESSAO_ATM_INS'])
                qtd_de_valores = int(qtd_de_valores) + 1
        pressao_atm_ins = "{},{}".format(valor,qtd_de_valores)
    #atualizar dado e modificar no Dynamo
    #update precipitacao_total
    table.update_item(
    Key={
        "Data": str(data)
    },
    UpdateExpression= "SET PRECIPITACAO_TOT = :val1",
    ExpressionAttributeValues={
        ':val1': str(precipitacao_total)
    })
    
    #update radiacao_global
    table.update_item(
    Key={
        'Data': str(data)
    },
    UpdateExpression='SET RADIACAO_GLOBAL = :val1',
    ExpressionAttributeValues={
        ':val1': str(radiacao_global)
    })
    #UPDATE DATA DA PRESSAO ATM. MÁXIMA
    #update CODIGO_ESTACAO_PAMA
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET CODIGO_ESTACAO_PAMA = :val1',
    ExpressionAttributeValues={
        ':val1': codigo_estacao_pama
    })
    #update NOME_ESTACAO_PAMA
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET NOME_ESTACAO_PAMA = :val1',
    ExpressionAttributeValues={
        ':val1': nome_estacao_pama
    })
    #update LATITUDE_PAMA
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET LATITUDE_PAMA = :val1',
    ExpressionAttributeValues={
        ':val1': latitude_pama
    })
    #update LONGITUDE_PAMA
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET LONGITUDE_PAMA = :val1',
    ExpressionAttributeValues={
        ':val1': longitude_pama
    })
    #update HORARIO_COLETA_PAMA
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET HORARIO_COLETA_PAMA = :val1',
    ExpressionAttributeValues={
        ':val1': horario_coleta_pama
    })
    #update PRESSAO_ATM_MAX
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET PRESSAO_ATM_MAX = :val1',
    ExpressionAttributeValues={
        ':val1': str(pressao_atm_max)
    })
    #UPDATE DATA DA PRESSAO ATM. MINIMA
    #update CODIGO_ESTACAO_PAMI
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET CODIGO_ESTACAO_PAMI = :val1',
    ExpressionAttributeValues={
        ':val1': codigo_estacao_pami
    })
    #update NOME_ESTACAO_PAMI
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET NOME_ESTACAO_PAMI = :val1',
    ExpressionAttributeValues={
        ':val1': nome_estacao_pami
    })
    #update LATITUDE_PAMI
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET LATITUDE_PAMI = :val1',
    ExpressionAttributeValues={
        ':val1': latitude_pami
    })
    #update LONGITUDE_PAMI
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET LONGITUDE_PAMI = :val1',
    ExpressionAttributeValues={
        ':val1': longitude_pami
    })
    #update HORARIO_COLETA_PAMI
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET HORARIO_COLETA_PAMI = :val1',
    ExpressionAttributeValues={
        ':val1': horario_coleta_pami
    })
    #update PRESSAO_ATM_MIN
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET PRESSAO_ATM_MIN = :val1',
    ExpressionAttributeValues={
        ':val1': str(pressao_atm_min)
    })
    #update pressao_atm_ins
    table.update_item(
    Key={
        'Data': '{}'.format(data)
    },
    UpdateExpression='SET PRESSAO_ATM_INS = :val1',
    ExpressionAttributeValues={
        ':val1': pressao_atm_ins
    })
