import azure.functions as func
import json
import logging
import multiweatherapi.multiweatherapi.zentra as zentra
import multiweatherapi.multiweatherapi as multiweatherapi
from requests import Session, Request
import time

import requests

logger = logging.getLogger('zentra')
parms = None

class Timer:    
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('main - Starting...')
    streamer = logging.StreamHandler()
    streamer.setLevel(logging.DEBUG)
    logger.addHandler(streamer)
    logger.info('main - Retrieving parms...')
    logger.info('main - req = ' + str(req.get_body()))
    parms = req.get_json()
    
    with Timer() as timer: 
        zentra_parms = zentra.ZentraParam(token = parms.get('token'), sn = parms.get('serial_number'))   
        readings = zentra.ZentraReadings(zentra_parms)

    logger.info('The call to Zentra took %.03f seconds.' % timer.interval)
    return func.HttpResponse(json.dumps(readings.response))