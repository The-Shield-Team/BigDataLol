import os
import json
from google.cloud import bigquery
import base64
import functions_framework
from datetime import datetime, timezone

project_id = os.environ['PROJECT']
bq_dataset = os.environ['DATASET']
bq_table = os.environ['TABLE']

bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset(bq_dataset)

def convert_timestamp_to_readable(timestamp):
    # Convert the timestamp to seconds
    timestamp_seconds = timestamp / 1e9  # Assuming the timestamp is in nanoseconds
    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    
    # Convert the datetime object to the desired format
    readable_format = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'
    
    return readable_format

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    try:
        # Decode the Pub/Sub message
        event_data = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        evenData = json.loads(event_data)
        print("Message content: ", evenData)

        table_ref = dataset_ref.table(bq_table)
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('matchId', 'STRING'),
                bigquery.SchemaField('metadata', 'JSON'),
                bigquery.SchemaField('info', 'JSON'),
                bigquery.SchemaField('date', 'TIMESTAMP')
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Prepare the row to be inserted
        row = {
            'matchId': evenData.get('matchId'),
            'metadata': json.dumps(evenData.get('metadata', {})),
            'info': json.dumps(evenData.get('info', {})),
            'date': convert_timestamp_to_readable(evenData.get('date'))
        }

        # Print the final JSON content for debugging
        print("Final JSON content:\n", row)

        # Load data from a JSON string into BigQuery
        load_job = bigquery_client.load_table_from_json(
            [row],  # Notice the list of rows
            table_ref,
            job_config=job_config
        )
        
        print('Starting job {}'.format(load_job.job_id))
        load_job.result()  # Wait for the load job to complete.
        print('Job finished.')
        destination_table = bigquery_client.get_table(table_ref)
        print('Loaded {} rows.'.format(destination_table.num_rows))
    except Exception as e:
        print(f'Error during load: {e}')
