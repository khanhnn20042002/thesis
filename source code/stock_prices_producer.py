from crawl import crawl_message, crawl_symbol
import time 
import json 
import random 
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd
import json

# Messages will be serialized as JSON 
def serializer(obj):
    return json.dumps(obj).encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer,
    key_serializer=serializer
)


if __name__ == '__main__':
    # symbols = crawl_symbol()
    # current_datetime = datetime.today()
    # current_date = current_datetime.date()
    # start_date = current_date - timedelta(days=1)
    # end_date = current_date
    # for symbol in symbols:

    #     # Generate a message
    #     message = crawl_message(symbol, start_date, end_date)
        
    #     # Send it to topic
    #     print(f'Producing message @ {datetime.now()} | Message = {str(message)}')
    #     producer.send('stock-prices', message, message["ticker"])
    
    #     # Sleep for a random number of seconds
    #     time_to_sleep = random.randint(1, 11)
    #     time.sleep(time_to_sleep)

    test_data = pd.read_csv("StockPrices.csv")
    for idx, row in test_data.iterrows():
        message = dict(row)
        print(f'Producing message @ {datetime.now()} | Message = {str(message)}')
        producer.send('StockPrices', message, message["ticker"])
        #time_to_sleep = random.randint(1, 3)
        time.sleep(0.01)
    while(True):
        time.sleep(60)