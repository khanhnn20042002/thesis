import json
import os
import time
import signal
import druid_api
import utils

druid_api.suspend_all_supervisors()
process_ids = json.load(open("./tmp/process_ids.json"))

os.killpg(process_ids["stock_prices_producer"], signal.SIGINT)
#fixed_indicators = utils.get_fixed_indicators()
fixed_indicators = ["SMA"]
for indicator in fixed_indicators:
    os.killpg(process_ids[indicator], signal.SIGINT)

time.sleep(5)
os.killpg(process_ids["kafka"], signal.SIGINT)

for filename in os.listdir("./custom-indicators"):
    os.remove(os.path.join("./custom-indicators", filename))

for filename in os.listdir("./tmp"):
    if filename != "process_ids.json":
        os.remove(os.path.join("./tmp", filename))

time.sleep(5)
os.killpg(process_ids["druid"], signal.SIGINT)

json.dump({}, open("./tmp/process_ids.json", "w"))