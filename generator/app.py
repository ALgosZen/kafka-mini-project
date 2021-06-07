from kafka import KafkaProducer
import os
import json
from time import sleep
from transactions import create_random_transaction
#from transactions import transations

#from random import choices, randint
#from string import ascii_letters, digits

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = float(os.environ.get("TRANSACTIONS_PER_SECOND"))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

if __name__ == "__main__":
   producer = KafkaProducer(
       bootstrap_servers=KAFKA_BROKER_URL,
       value_serializer=lambda value: json.dumps(value).encode()
   )

   # infinity loop
   while True:
       #transaction: dict = transactions.create_random_transaction()
       transaction: dict = create_random_transaction()
       message: str = json.dumps(transaction)
       producer.send(TRANSACTIONS_TOPIC, value=transaction)
       #message: str = json.dumps({"a":"b", "c":"d"})
       #print("========= TX TOPIC: ",TRANSACTIONS_TOPIC)
       #producer.send("queueing.transactions", value=message)
       print(transaction)
       sleep(SLEEP_TIME)
