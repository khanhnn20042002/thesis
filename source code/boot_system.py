# Run the system for the very first time
import os
import subprocess
import time
import json
import kafka
import utils
import kafka_api
import druid_api

directory = os.path.dirname(__file__)
process_ids = json.load(open("./tmp/process_ids.json"))
druid_process = subprocess.Popen("~/apache-druid-29.0.1/bin/start-druid", 
                                shell=True, preexec_fn=os.setpgrp)
process_ids['druid'] = druid_process.pid
#CPU usage go to 100% reserve some time to release resource
time.sleep(60)
kafka_process = subprocess.Popen("~/kafka_2.13-2.7.0/bin/kafka-server-start.sh ~/kafka_2.13-2.7.0/config/server.properties",
                                shell = True, preexec_fn=os.setpgrp)
process_ids['kafka'] = kafka_process.pid
bootstrap_servers = ["localhost:9092"]
#reserve some time to start kafka properly
time.sleep(10)

admin_client = kafka.KafkaAdminClient(bootstrap_servers = bootstrap_servers)
kafka_api.create_topic(admin_client, "StockPrices")
druid_api.create_stock_prices_supervisor()

#fixed_indicators =  utils.get_fixed_indicators()
fixed_indicators = ["SMA"]
for indicator in fixed_indicators:
    kafka_api.create_topic(admin_client, indicator)
    druid_api.create_indicator_supervisor(indicator)

stock_prices_producer_process = subprocess.Popen(f"python {os.path.join(directory, 'stock_prices_producer.py')}",
                                                  shell=True, preexec_fn=os.setpgrp)
process_ids['stock_prices_producer'] = stock_prices_producer_process.pid

for indicator in fixed_indicators:
    kafka_api.start_indicator_calculator(indicator, process_ids)

json.dump(process_ids, open("./tmp/process_ids.json", 'w'))
    










