import requests
import json
import streamlit as st
from pydruid.db import connect
import pandas as pd

router_ip = 'localhost'
router_port = '8888'
api_gateway = f'http://{router_ip}:{router_port}'

def get_cursor():
    conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()
    return curs

def suspend_supervisor(supervisor_id):
    url = f"{api_gateway}/druid/indexer/v1/supervisor/{supervisor_id}/suspend"
    return requests.post(url)

def suspend_all_supervisors():
    url = f"{api_gateway}/druid/indexer/v1/supervisor/suspendAll"
    return requests.post(url)

def resume_supervisor(supervisor_id):
    url = f"{api_gateway}/druid/indexer/v1/supervisor/{supervisor_id}/resume"
    return requests.post(url)

def resume_all_supervisors():
    url = f"{api_gateway}/druid/indexer/v1/supervisor/resumeAll"
    return requests.post(url)

def terminate_supervisor(supervisor_id):
    url = f"{api_gateway}/druid/indexer/v1/supervisor/{supervisor_id}/terminate"
    return requests.post(url)

def terminate_all_supervisors(supervisor_id):
    url = f"{api_gateway}/druid/indexer/v1/supervisor/terminateAll"
    return requests.post(url)

def get_supervisor_ids():
    url = f"{api_gateway}/druid/indexer/v1/supervisor/"
    return requests.get(url).json()

def get_supervisor_spec(supervisor_id):
    url = f"{api_gateway}/druid/indexer/v1/supervisor/{supervisor_id}"
    return requests.get(url).json()

def get_segment_ids(data_source):
    url = f'{api_gateway}/druid/coordinator/v1/datasources/{data_source}/segments'
    headers = {'Content-Type': 'application/json'}
    return requests.get(url, headers=headers).json()

def mark_unused(data_source, segment_ids):
    url = f'{api_gateway}/druid/coordinator/v1/datasources/{data_source}/markUnused'
    headers = {'Content-Type': 'application/json'}
    data = {"segmentIds": segment_ids}
    return requests.post(url, headers=headers, data=json.dumps(data))

def run_kill_task(data_source):
    kill_task_spec = {"type": "kill", "dataSource": data_source, "interval":"1000-01-01/3000-01-01"}
    url = f'{api_gateway}/druid/indexer/v1/task'
    headers = {'Content-Type': 'application/json'}
    return requests.post(url, headers=headers, data=json.dumps(kill_task_spec))

def delete_data_source(data_source):
    segment_ids = get_segment_ids(data_source)
    mark_unused(data_source, segment_ids)
    run_kill_task(data_source)

def get_data_sources():
    url = f'{api_gateway}/druid/coordinator/v1/datasources'
    return requests.get(url).json()

def submit_supervisor_spec(supervisor_spec):
    url = f"{api_gateway}/druid/indexer/v1/supervisor"
    headers = {'Content-Type': 'application/json'}
    return requests.post(url, headers=headers, data=json.dumps(supervisor_spec))

def shutdown_all_tasks_for_data_source(data_source):
    url = f"{api_gateway}/druid/indexer/v1/datasources/{data_source}/shutdownAllTasks"
    return requests.post(url)

def create_indicator_supervisor(indicator):
    data_sources = get_data_sources()
    if indicator in data_sources:
        delete_data_source(indicator)
    supervisor_spec = {
        "type": "kafka",
        "spec": {
            "ioConfig": {
            "type": "kafka",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9092"
            },
            "topic": f"{indicator}",
            "inputFormat": {
                "type": "kafka",
                "keyFormat": {
                "type": "regex",
                "pattern": "([\\s\\S]*)",
                "columns": [
                    "line"
                ]
                },
                "valueFormat": {
                "type": "json"
                }
            },
            "useEarliestOffset": True
            },
            "tuningConfig": {
            "type": "kafka"
            },
            "dataSchema": {
            "dataSource": f"{indicator}",
            "timestampSpec": {
                "column": "time",
                "format": "auto"
            },
            "dimensionsSpec": {
                "dimensions": [
                "ticker",
                {
                    "type": "double",
                    "name": f"{indicator.lower()}"
                }
                ]
            },
            "granularitySpec": {
                "queryGranularity": "none",
                "rollup": False,
                "segmentGranularity": "hour"
            }
            }
        }
    }
    submit_supervisor_spec(supervisor_spec)


def create_stock_prices_supervisor():
    data_sources = get_data_sources()
    if "StockPrices" in data_sources:
        delete_data_source("StockPrices")
    supervisor_spec = {
        "type": "kafka",
        "spec": {
            "ioConfig": {
            "type": "kafka",
            "consumerProperties": {
                "bootstrap.servers": "localhost:9092"
            },
            "topic": "StockPrices",
            "inputFormat": {
                "type": "kafka",
                "keyFormat": {
                "type": "regex",
                "pattern": "([\\s\\S]*)",
                "columns": [
                    "line"
                ]
                },
                "valueFormat": {
                "type": "json"
                }
            },
            "useEarliestOffset": True
            },
            "tuningConfig": {
            "type": "kafka"
            },
            "dataSchema": {
            "dataSource": "StockPrices",
            "timestampSpec": {
                "column": "time",
                "format": "auto"
            },
            "dimensionsSpec": {
                "dimensions": [
                {
                    "type": "long",
                    "name": "volume"
                },
                {
                    "type": "long",
                    "name": "high"
                },
                "ticker",
                {
                    "type": "long",
                    "name": "low"
                },
                {
                    "type": "long",
                    "name": "close"
                },
                {
                    "type": "long",
                    "name": "open"
                }
                ]
            },
            "granularitySpec": {
                "queryGranularity": "none",
                "rollup": False,
                "segmentGranularity": "hour"
            }
            }
        }
    }
    submit_supervisor_spec(supervisor_spec)

def get_query_result(query, curs):
    curs.execute(query)
    result = pd.DataFrame(curs)
    result.rename(columns = {'_0':'time'}, inplace = True)
    return result

    



