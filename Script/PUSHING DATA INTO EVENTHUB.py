PUSHING DATA INTO EVENTHUB


from azure.eventhub import EventHubProducerClient, EventData
import json

CONNECTION_STR = "<AZURE_EVENT_HUB_CONNECTION_STRING>"
EVENTHUB_NAME = "EVENTHUB_NAME"

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

# Load JSON from DBFS
with open("/dbfs/FileStore/tables/transactions.json", "r") as f:
    transactions = json.load(f)

with producer:
    batch = producer.create_batch()
    for txn in transactions:
        event_data = EventData(json.dumps(txn))
        try:
            batch.add(event_data)
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event_data)
    if len(batch) > 0:
        producer.send_batch(batch)



from azure.eventhub import EventHubProducerClient, EventData
import json

CONNECTION_STR = "<AZURE_EVENT_HUB_CONNECTION_STRING>"
EVENTHUB_NAME = "EVENTHUB_NAME"

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

with open("/dbfs/FileStore/tables/merchants.json", "r") as f:
    customers = json.load(f)

with producer:
    batch = producer.create_batch()
    for cust in customers:
        event_data = EventData(json.dumps(cust))
        try:
            batch.add(event_data)
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event_data)
    if len(batch) > 0:
        producer.send_batch(batch)




from azure.eventhub import EventHubProducerClient, EventData
import json

CONNECTION_STR = "<AZURE_EVENT_HUB_CONNECTION_STRING>"
EVENTHUB_NAME = "EVENTHUB_NAME"

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

with open("/dbfs/FileStore/tables/merchants.json", "r") as f:
    customers = json.load(f)

with producer:
    batch = producer.create_batch()
    for cust in customers:
        event_data = EventData(json.dumps(cust))
        try:
            batch.add(event_data)
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event_data)
    if len(batch) > 0:
        producer.send_batch(batch)




