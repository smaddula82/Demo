from kafka import KafkaConsumer
import json

if __name__=="__main__":
    consumer=KafkaConsumer('HousePrice',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id='consumer-group-a')
    print('starting the consumer')
    for msg in consumer:
        data=json.loads(msg.value)
        print('HouseData = {}'.format(data))
