import base64
import functions_framework
import os
import json
from google.cloud import bigquery

project_id = os.environ['PROJECT']
bq_dataset_silver = os.environ['DATASET_SILVER']
bq_dataset_gold = os.environ['DATASET_GOLD']
silver_table = os.environ['TABLE_SILVER']
gold_table = os.environ['TABLE_GOLD']

bigquery_client = bigquery.Client()
gold_table_ref = bigquery_client.dataset(bq_dataset_gold).table(gold_table)
silver_table_ref = bigquery_client.dataset(bq_dataset_silver).table(silver_table)

def mapTransformer(mapId):
    maps = {
        "11": "Grieta del Invocador",
        "12": "Abismo de los Lamentos"
    }
    return maps[str(mapId)]

def platformIdTransformer(platformId):
    platforms = {
       'EUW1': "Europa West",
       'NA1': "Norte América",
       'KR': "Corea",
       'LA1': "Latinoamerica Norte",
       'LA2': "Latinoamerica Sur",
       'BR1': "Brasil",
    }
    return platforms[str(platformId)]

def queueIdTransformer(queueId):
    queues = {
       '400': "5v5 Draft Pick games",
       '410': "5v5 Ranked Dynamic games",
       '420': "5v5 Ranked Solo games",
       '430': "5v5 Blind Pick games",
       '440': "5v5 Ranked Flex games",
       '450': "5v5 ARAM games"
    }
    return queues[str(queueId)]

def transform_data(matchData):
    matchJson = matchData

    gold_data = {
        "matchId": matchJson['matchId'],
        "userId": matchJson['userId'],
        "gameVersion": matchJson['gameVersion'],
        "mapId": mapTransformer(matchJson['mapId']),
        "championId": matchJson['championId'],
        "championName": matchJson['championName'],
        "championImage": "url/champion.com",
        "item0Id": matchJson.get('item0', 0),
        "item0Name": "Infinity Edge",
        "item0Image": "url/item0.com",
        "item1Id": matchJson.get('item1', 0),
        "item1Name": "Infinity Edge",
        "item1Image": "url/item1.com",
        "item2Id": matchJson.get('item2', 0),
        "item2Name": "Guardian Angel",
        "item2Image": "url/item2.com",
        "item3Id": matchJson.get('item3', 0),
        "item3Name": "Kraken Slayer",
        "item3Image": "url/item3.com",
        "item4Id": matchJson.get('item4', 0),
        "item4Name": "Infinity Edge",
        "item4Image": "url/item4.com",
        "item5Id": matchJson.get('item5', 0),
        "item5Name": "Infinity Edge",
        "item5Image": "url/item5.com",
        "item6Id": matchJson.get('item6', 0),
        "item6Name": "Luden's Tempest",
        "item6Image": "url/item6.com",
        "role": matchJson.get('role', ''),
        "win": matchJson['win'],
        "platformId": platformIdTransformer(matchJson['platformId']),
        "queueId": matchJson['queueId'],
        "queueName": queueIdTransformer(matchJson['queueId'])
    }
    return gold_data

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    event_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    print("Message content: ", event_data)

    try:
        # Crear una tabla temporal para evitar problemas con el buffer de streaming
        temp_table_id = f"{project_id}.{bq_dataset_silver}.temp_table"
        query = f"""
            CREATE OR REPLACE TABLE `{temp_table_id}` AS
            SELECT *
            FROM `{project_id}.{bq_dataset_silver}.{silver_table}`
            WHERE uploadGold = FALSE and queueId is not null
        """
        bigquery_client.query(query).result()
        
        # Leer datos de la tabla temporal
        query = f"""
            SELECT *
            FROM `{temp_table_id}`
        """
        
        query_job = bigquery_client.query(query)
        query_result = query_job.result()
        matches = [dict(row) for row in query_result]

        for matchData in matches:
            gold_data = transform_data(matchData)
            matchId_gold = matchData['matchId']

            # Insertar datos en la tabla gold
            errors = bigquery_client.insert_rows_json(gold_table_ref, [gold_data])
            if errors:
                print(f"Encountered errors while inserting rows: {errors}")
            else:
                print(f"Inserted {len(gold_data)} rows into {gold_table}")

            # Actualizar uploadGold a true en la tabla silver
            update_query = f"""
                UPDATE `{project_id}.{bq_dataset_silver}.{silver_table}`
                SET uploadGold = TRUE
                WHERE matchId = '{matchId_gold}' and queueId is not null
            """
            update_job = bigquery_client.query(update_query)
            update_job.result()  # Wait for the job to complete

        # Borrar la tabla temporal
        bigquery_client.delete_table(temp_table_id)
        
        return "Success", 200

    except Exception as e:
        print(f"Error during processing: {e}")
        return str(e), 500
