import json
import requests
from kafka import KafkaProducer
from time import sleep
from csv import DictReader

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],
                      value_serializer=lambda x: json.dumps(x).encode('utf-8')
                      )


with open('test1.csv','r') as new_obj:
	csv_dict_reader=DictReader(new_obj)
	
	for row in csv_dict_reader:
		ack=producer.send("HousePrice",value=row)
		print(row)
		metadata=ack.get()
		sleep(5)
		producer.flush()

