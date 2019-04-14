#!/usr/local/bin/python3
import requests
import os
import query

import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger('ksqlQueries')

logger.info("Service starting")


headers = {
    "accept": "application/vnd.ksql.v1+json",
    "content-type": "application/vnd.ksql.v1+json"
}

host = "localhost"
port = "8088"
url = "http://%s:%s/ksql" % (host, port)


def send_query(url, json_data, headers):
    """
    post ksql and streamsProperties to ksql server url
    """
    logger.info(
        "Sending query: %s" % json_data["ksql"]
    )
    resp = requests.post(url=url, json=json_data, headers=headers)
    if resp.status_code == 200:
        logger.info(
            "SUCCESS: status_code %s" %(resp.status_code)
        )
    else:
        logger.info(
            "ERROR: status_code %s\n%s" %(resp.status_code, resp.text)
        )


query_list = query.query_list()
for q in query_list:
    data = {
        "ksql": q,
        "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}
    }
    send_query(url, data, headers)