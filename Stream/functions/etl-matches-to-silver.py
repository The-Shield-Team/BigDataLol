import base64
import functions_framework
import os
import json
from google.cloud import bigquery
import functions_framework

project_id = os.environ['PROJECT']
bq_dataset_bronze = os.environ['DATASET_BRONZE']
bq_dataset_silver = os.environ['DATASET_SILVER']
bronze_table = os.environ['TABLE_BRONZE']
silver_table = os.environ['TABLE_SILVER']

bigquery_client = bigquery.Client()
bronze_table_ref = bigquery_client.dataset(bq_dataset_bronze).table(bronze_table)
silver_table_ref = bigquery_client.dataset(bq_dataset_silver).table(silver_table)

def transform_data(match):
    participants = match['info']['participants']
    silver_data = []

    for participant in participants:
        silver_data.append({
            "userId": participant['puuid'],
            "matchId": match['matchId'],
            "gameId": match['info']['gameId'],
            "gameVersion": match['info']['gameVersion'],
            "mapId": match['info']['mapId'],
            "championId": participant['championId'],
            "championName": participant['championName'],
            "item0": participant.get('item0', 0),
            "item1": participant.get('item1', 0),
            "item2": participant.get('item2', 0),
            "item3": participant.get('item3', 0),
            "item4": participant.get('item4', 0),
            "item5": participant.get('item5', 0),
            "item6": participant.get('item6', 0),
            "role": participant.get('role', ''),
            "win": participant['win'],
            "platformId": match['platformId'],
            "queueId": match['queueId'],
            "championIdBanned": participant.get('championIdBanned', 0)
        })

    return silver_data

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    event_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    evenData = json.loads(event_data)
    print("Message content: ", evenData)

    try:
        # Query to get matches with uploadSilver as false
        query = f"""
            SELECT *
            FROM `{project_id}.{bq_dataset_bronze}.{bronze_table}`
            WHERE uploadSilver = FALSE
        """
        
        query_job = bigquery_client.query(query)
        matches = query_job.result()
        print("---- QUERY RESULTS ----")
        print(matches)
        print("---- END RESULTS ----")
        
        for match in matches:
            silver_data = transform_data(match)
            
            # Insert data into silver_stream_matches
            errors = bigquery_client.insert_rows_json(silver_table_ref, silver_data)
            if errors:
                print(f"Encountered errors while inserting rows: {errors}")
            else:
                print(f"Inserted {len(silver_data)} rows into {silver_table}")

            # Update uploadSilver to true in the bronze table
            update_query = f"""
                UPDATE `{project_id}.{bq_dataset_bronze}.{bronze_table}`
                SET uploadSilver = TRUE
                WHERE matchId = '{match['matchId']}'
            """
            update_job = bigquery_client.query(update_query)
            update_job.result()  # Wait for the job to complete

        return "Success", 200

    except Exception as e:
        print(f"Error during processing: {e}")
        return str(e), 500
