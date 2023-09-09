from google.cloud import bigquery
from google.cloud import pubsub_v1
import base64
import json
def hello_pubsub(event, context):
    # Get the message data from Pub/Sub
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
   
    try:
        parsed_message = json.loads(pubsub_message)
        print(parsed_message)
        # formatted_message = json.dumps(parsed_message, indent=4, ensure_ascii=False)
        # Initialize the BigQuery client
        bigquery_client = bigquery.Client()

        # Specify the BigQuery dataset and table where you want to write the data
        dataset_id = 'stock_subcribe'
        table_id = 'Stock_pubsub'

        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery_client.get_table(table_ref)

        # Insert the message data into BigQuery
        rows_to_insert = [parsed_message]
        errors=bigquery_client.insert_rows(table, rows_to_insert)
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    except Exception as e:
        print(f"Error inserting JSON message into BigQuery: {str(e)}")