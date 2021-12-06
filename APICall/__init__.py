import azure.functions as func
import json
import logging
from multiweatherapi.multiweatherapi import multiweatherapi
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
    parms = req.get_json()

    # Change the name of the "api_provider" dictionary key to "vendor" as that's what Junhee's multiweather code is expecting.
    parms['vendor'] = parms['api_provider']
    del parms['api_provider']

    # Change the name of the "identifier" dictionary key to "sn" as that's what Junhee's multiweather code is expecting.
    parms['sn'] = parms['identifier']
    del parms['identifier']    

    logger.info('main - Calling the ' + parms.get('vendor') + ' API...')
    
    with Timer() as timer: 
        readings = multiweatherapi.get_reading(**parms)

    logger.info('The call to ' + parms.get('vendor') + ' took %.03f seconds.' % timer.interval)
    return func.HttpResponse(json.dumps(readings.response))